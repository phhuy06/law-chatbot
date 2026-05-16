"""Lambda batch layer.

Reads the master dataset (immutable per-document JSON in MinIO at
``phapluat/raw/{YYYY}/{MM}/{doc_id}.json`` — written by kafka-consumer),
re-runs clean + chunk + embed, and bulk-writes into Elasticsearch.

Env vars:
    BATCH_DATA_PATH   Default empty → read MinIO raw/. If set to a local
                      glob like ``/app/crawler/output/*.csv``, falls back to
                      the legacy CSV path (used by ``./run_batch.sh --local``).
    BATCH_FORCE       "true" → skip the ES dedup query so every doc is
                      re-embedded (cost: full OpenAI embedding bill). Use
                      after model or chunker upgrade. Default: dedup enabled.
    BATCH_RAW_PREFIX  Prefix inside the MinIO bucket. Default ``raw/``.
    MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY / MINIO_BUCKET
                      Standard MinIO connection vars.
"""
import hashlib
import json
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StringType, StructField, StructType

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

# Schema for documents read from MinIO raw/ — matches kafka-consumer output.
RAW_DOC_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("content", StringType()),
    StructField("category", StringType()),
    StructField("doc_type", StringType()),
    StructField("doc_number", StringType()),
    StructField("agency", StringType()),
    StructField("published_date", StringType()),
    StructField("url", StringType()),
    StructField("crawled_at", StringType()),
])


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def invalidate_chat_cache(redis_url: str):
    """Delete all chat:* keys after a successful batch write."""
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


def fetch_raw_from_minio() -> list[dict]:
    """List + download every per-document JSON in MinIO ``phapluat/raw/``.

    Driver-side fan-out: list with paginator, fetch each object, json.loads.
    For our data volume (~10k docs × ~10KB) this is trivial; no s3a:// /
    hadoop-aws complexity needed.
    """
    import boto3

    endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    bucket = os.environ.get("MINIO_BUCKET", "phapluat")
    prefix = os.environ.get("BATCH_RAW_PREFIX", "raw/")

    scheme = "https" if endpoint.startswith("https://") else "http"
    host = endpoint.replace("http://", "").replace("https://", "")

    client = boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{host}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    print(f"[batch] listing s3://{bucket}/{prefix}...")
    docs: list[dict] = []
    skipped = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            try:
                body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
                docs.append(json.loads(body))
            except Exception as e:
                skipped += 1
                print(f"[batch] failed to read {key}: {e}")
    print(f"[batch] loaded {len(docs)} docs from MinIO raw/ (skipped {skipped})")
    return docs


def load_source_dataframe(spark: SparkSession):
    """Decide the source: MinIO raw/ (default) or local CSV (legacy override)."""
    data_path = os.environ.get("BATCH_DATA_PATH", "").strip()

    if data_path:
        # Legacy local-CSV path — used by `./run_batch.sh --local`.
        print(f"[batch] reading local CSV path: {data_path}")
        return spark.read \
            .option("header", "true") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("multiLine", "true") \
            .csv(data_path)

    # Default: read the master dataset (MinIO raw/ zone).
    raw_docs = fetch_raw_from_minio()
    if not raw_docs:
        return None
    # Ensure all expected keys exist so createDataFrame doesn't drop fields.
    for d in raw_docs:
        for f in RAW_DOC_SCHEMA.fieldNames():
            d.setdefault(f, None)
    rows = [{f: d.get(f) for f in RAW_DOC_SCHEMA.fieldNames()} for d in raw_docs]
    return spark.createDataFrame(rows, schema=RAW_DOC_SCHEMA)


def run_batch_pipeline():
    es_url = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
    es_host = es_url.replace("http://", "").replace("https://", "").split(":")[0]
    es_port = es_url.replace("http://", "").replace("https://", "").split(":")[-1]
    api_key = os.environ.get("OPENAI_API_KEY", "")
    es_index = os.environ.get("ES_INDEX", "phapluat")
    redis_url = os.environ.get("REDIS_URL", "")
    force = _env_truthy("BATCH_FORCE")

    spark = SparkSession.builder \
        .appName("Legal-Chatbot-Batch-Pipeline") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("es.nodes", es_host) \
        .config("es.port", es_port) \
        .config("es.nodes.wan.only", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addFile(os.path.join(project_root, "spark"), recursive=True)

    df_raw = load_source_dataframe(spark)
    if df_raw is None:
        print("[batch] No documents in MinIO raw/ — nothing to do")
        spark.stop()
        return

    from elasticsearch import Elasticsearch, helpers
    es_client = Elasticsearch(es_url)

    # Lambda-style dedup: skip docs already indexed unless BATCH_FORCE=true.
    if force:
        print("[batch] BATCH_FORCE=true — skipping ES dedup (full re-embed)")
    else:
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
    )

    # Collect and embed in batches on driver, then bulk write to ES
    rows = df_final.collect()
    if not rows:
        print("[batch] No new chunks to process")
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
    if success:
        invalidate_chat_cache(redis_url)

    spark.stop()


if __name__ == "__main__":
    run_batch_pipeline()
