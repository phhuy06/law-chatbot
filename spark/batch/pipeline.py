import hashlib
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

current_file_path = os.path.abspath(__file__)
batch_dir = os.path.dirname(current_file_path)
spark_dir = os.path.dirname(batch_dir)
project_root = os.path.dirname(spark_dir)

sys.path.append(project_root)

from spark.utils.udfs import clean_text, chunk_text

# Max texts per OpenAI embedding API call
EMBED_BATCH_SIZE = 100

# Max doc_ids per ES terms-lookup query
ES_DEDUP_BATCH_SIZE = 1000


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
            print(f"[dedup] ES query failed for batch {i // ES_DEDUP_BATCH_SIZE}: {e}")
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


def run_batch_pipeline():
    es_url = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
    es_host = es_url.replace("http://", "").replace("https://", "").split(":")[0]
    es_port = es_url.replace("http://", "").replace("https://", "").split(":")[-1]
    api_key = os.environ.get("OPENAI_API_KEY", "")
    es_index = os.environ.get("ES_INDEX", "phapluat")

    spark = SparkSession.builder \
        .appName("Legal-Chatbot-Batch-Pipeline") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("es.nodes", es_host) \
        .config("es.port", es_port) \
        .config("es.nodes.wan.only", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addFile(os.path.join(project_root, "spark"), recursive=True)

    data_path = os.environ.get(
        "BATCH_DATA_PATH",
        os.path.join(project_root, "crawler", "output", "*.csv"),
    )

    df_raw = spark.read \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .csv(data_path)

    # Skip doc_ids already indexed in ES
    from elasticsearch import Elasticsearch, helpers
    es_client = Elasticsearch(es_url)

    all_ids = [r["id"] for r in df_raw.select("id").distinct().collect() if r["id"]]
    if all_ids:
        existing_ids = fetch_existing_doc_ids(es_client, es_index, all_ids)
        print(f"[batch] {len(existing_ids)}/{len(all_ids)} doc_ids already in ES — skipping")
        if existing_ids:
            df_raw = df_raw.filter(~col("id").isin(list(existing_ids)))

    df_clean = df_raw.withColumn("clean_text", clean_text(col("content")))
    df_chunks = df_clean.withColumn("chunks", chunk_text(col("clean_text")))
    df_exploded = df_chunks.withColumn("chunk_text", explode("chunks"))

    df_final = df_exploded.select(
        col("id").alias("doc_id"),
        col("chunk_text"),
        col("title"),
        col("url"),
        col("category"),
        col("doc_type"),
        col("agency"),
    )

    # Collect and embed in batches on driver, then bulk write to ES
    rows = df_final.collect()
    if not rows:
        print("[batch] No data to process")
        spark.stop()
        return

    docs = [row.asDict() for row in rows]
    texts = [doc["chunk_text"] or "" for doc in docs]
    print(f"[batch] {len(docs)} total chunks to embed...")

    if api_key:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        embeddings = batch_embed(texts, client)
    else:
        print("[batch] SKIP embedding (no API key)")
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
    print(f"[batch] Done — {success}/{len(actions)} docs written to ES")
    if errors:
        print(f"[batch] ES errors: {errors}")

    spark.stop()

if __name__ == "__main__":
    run_batch_pipeline()
