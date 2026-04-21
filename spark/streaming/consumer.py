import hashlib
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

current_file_path = os.path.abspath(__file__)
streaming_dir = os.path.dirname(current_file_path)
spark_dir = os.path.dirname(streaming_dir)
project_root = os.path.dirname(spark_dir)

sys.path.append(project_root)

from spark.utils.udfs import clean_text, chunk_text

# Max texts per OpenAI embedding API call
EMBED_BATCH_SIZE = 100

# Max doc_ids per ES terms-lookup query
ES_DEDUP_BATCH_SIZE = 1000


def invalidate_chat_cache(redis_url: str):
    """Delete all chat:* keys so the backend re-runs retrieval with fresh ES state.
    Called after each batch that actually wrote new docs."""
    if not redis_url:
        return
    try:
        import redis
        client = redis.from_url(redis_url)
        deleted = 0
        for key in client.scan_iter("chat:*", count=500):
            client.delete(key)
            deleted += 1
        if deleted:
            print(f"[cache] invalidated {deleted} chat:* keys")
    except Exception as e:
        print(f"[cache] invalidate failed (non-fatal): {e}")


def fetch_existing_doc_ids(es_client, index: str, doc_ids: list[str]) -> set[str]:
    """Return the subset of doc_ids that already have at least one chunk in ES."""
    existing: set[str] = set()
    for i in range(0, len(doc_ids), ES_DEDUP_BATCH_SIZE):
        chunk = doc_ids[i:i + ES_DEDUP_BATCH_SIZE]
        try:
            resp = es_client.search(
                index=index,
                size=0,
                query={"terms": {"doc_id": chunk}},
                aggs={"ids": {"terms": {"field": "doc_id", "size": len(chunk)}}},
            )
            for bucket in resp["aggregations"]["ids"]["buckets"]:
                existing.add(bucket["key"])
        except Exception as e:
            print(f"[dedup] ES query failed: {e}")
    return existing


def batch_embed(texts: list[str], client) -> list[list[float]]:
    """Call OpenAI embeddings API with multiple texts in one request."""
    results: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        batch = texts[i:i + EMBED_BATCH_SIZE]
        print(f"[embed] Calling OpenAI for {len(batch)} texts (batch {i // EMBED_BATCH_SIZE + 1})...")
        try:
            response = client.embeddings.create(
                model="text-embedding-3-small",
                input=batch,
            )
            embeddings = [item.embedding for item in response.data]
            results.extend(embeddings)
            print(f"[embed] OK — got {len(embeddings)} embeddings")
        except Exception as e:
            print(f"[embed] ERROR: {e} — filling {len(batch)} with zeros")
            results.extend([[0.0] * 1536 for _ in batch])
    return results


def chunk_id(doc_id: str, chunk_text: str) -> str:
    """Generate a deterministic ID for a chunk based on doc ID and content."""
    h = hashlib.md5(f"{doc_id}:{chunk_text}".encode()).hexdigest()[:12]
    return f"{doc_id}_{h}" if doc_id else ""


def run_streaming_pipeline():
    es_url = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
    es_host = es_url.replace("http://", "").replace("https://", "").split(":")[0]
    es_port = es_url.replace("http://", "").replace("https://", "").split(":")[-1]
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "van-ban-phap-luat")
    es_index = os.environ.get("ES_INDEX", "phapluat")
    api_key = os.environ.get("OPENAI_API_KEY", "")
    max_offsets = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "3")
    redis_url = os.environ.get("REDIS_URL", "")

    # Reusable clients
    openai_client = None
    if api_key:
        from openai import OpenAI
        openai_client = OpenAI(api_key=api_key)

    from elasticsearch import Elasticsearch, helpers
    es_client = Elasticsearch(es_url)

    spark = SparkSession.builder \
        .appName("Legal-Chatbot-Streaming") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("es.nodes", es_host) \
        .config("es.port", es_port) \
        .config("es.nodes.wan.only", "true") \
        .config("es.index.auto.create", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addFile(os.path.join(project_root, "spark"), recursive=True)

    kafka_schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("content", StringType(), True),
        StructField("category", StringType(), True),
        StructField("doc_type", StringType(), True),
        StructField("doc_number", StringType(), True),
        StructField("agency", StringType(), True),
        StructField("published_date", StringType(), True),
        StructField("url", StringType(), True),
        StructField("crawled_at", StringType(), True),
    ])

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", max_offsets) \
        .load()

    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data")
    ).select("data.*")

    # Clean and chunk in Spark, embedding done in foreachBatch
    df_clean = df_parsed.withColumn("clean_text", clean_text(col("content")))
    df_chunks = df_clean.withColumn("chunks", chunk_text(col("clean_text")))
    df_exploded = df_chunks.withColumn("chunk_text", explode("chunks"))

    df_final = df_exploded.select(
        col("id").alias("doc_id"),
        col("chunk_text"),
        col("title"),
        col("url"),
        col("category"),
        col("doc_type"),
    )

    checkpoint_dir = os.path.join(project_root, "checkpoints", "streaming_python")

    print(f"[streaming] Pipeline ready. Kafka={kafka_servers}, Topic={kafka_topic}, ES={es_url}, Index={es_index}")
    print("[streaming] Waiting for new data from Kafka...")

    def embed_and_write_to_es(batch_df, batch_id):
        try:
            rows = batch_df.collect()
            if not rows:
                return

            # Skip chunks whose doc_id already has anything indexed in ES
            unique_doc_ids = list({r["doc_id"] for r in rows if r["doc_id"]})
            if unique_doc_ids:
                existing_ids = fetch_existing_doc_ids(es_client, es_index, unique_doc_ids)
                if existing_ids:
                    before = len(rows)
                    rows = [r for r in rows if r["doc_id"] not in existing_ids]
                    print(f"[batch {batch_id}] skipped {before - len(rows)}/{before} chunks — {len(existing_ids)} doc_ids already in ES")
            if not rows:
                return

            docs = [row.asDict() for row in rows]
            texts = [doc["chunk_text"] or "" for doc in docs]

            print(f"[batch {batch_id}] {len(docs)} chunks to embed and index...")

            # Batch embed all texts in one (or few) API calls
            if openai_client:
                embeddings = batch_embed(texts, openai_client)
            else:
                print(f"[batch {batch_id}] SKIP embedding (no API key)")
                embeddings = [[0.0] * 1536 for _ in texts]

            actions = []
            for doc, emb in zip(docs, embeddings):
                doc["embedding"] = emb
                action = {"_index": es_index, "_source": doc}
                cid = chunk_id(doc.get("doc_id", ""), doc.get("chunk_text", ""))
                if cid:
                    action["_id"] = cid
                actions.append(action)

            success, errors = helpers.bulk(es_client, actions, raise_on_error=False)
            print(f"[batch {batch_id}] Done — {success}/{len(actions)} docs written to ES")
            if errors:
                print(f"[batch {batch_id}] ES errors: {errors}")
            if success:
                invalidate_chat_cache(redis_url)
        except Exception as e:
            print(f"[batch {batch_id}] ERROR: {e}")

    query = df_final.writeStream \
        .foreachBatch(embed_and_write_to_es) \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_pipeline()