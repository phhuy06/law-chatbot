"""Watch MinIO for new CSV files, push to Kafka, move to processed folder."""
import csv
import io
import logging
import os
import time
from datetime import datetime, timezone

from minio import Minio
from minio.commonconfig import CopySource
from producer import DocumentProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "phapluat")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "van-ban-phap-luat")

UNPROCESSED_PREFIX = os.environ.get("INGEST_UNPROCESSED_PREFIX", "csv/")
PROCESSED_PREFIX = os.environ.get("INGEST_PROCESSED_PREFIX", "csv/processed/")
# TODO(prod): This is the speed layer (10s) for dev/testing.
# In production, batch layer should run on a nightly cron schedule instead
# of polling. Set INGEST_POLL_INTERVAL=86400 or replace with a scheduler.
POLL_INTERVAL = int(os.environ.get("INGEST_POLL_INTERVAL", "10"))
BATCH_SIZE = int(os.environ.get("INGEST_BATCH_SIZE", "50"))


def parse_row(row: dict) -> dict:
    """Convert a CSV row dict to the document schema expected by the pipeline."""
    return {
        "id": row.get("id", ""),
        "question": row.get("question", ""),
        "answer": row.get("answer", ""),
        "category": row.get("category", ""),
        "author": row.get("author", ""),
        "published_date": row.get("published_date", ""),
        "legal_refs": row.get("legal_refs", ""),
        "tags": row.get("tags", ""),
        "views": int(row.get("views", 0) or 0),
        "url": row.get("url", ""),
        "crawled_at": row.get("crawled_at", datetime.now(timezone.utc).isoformat()),
    }


def ensure_bucket(minio_client: Minio):
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        logger.info("Created bucket: %s", MINIO_BUCKET)


def process_csv(minio_client: Minio, producer: DocumentProducer, object_name: str):
    """Download CSV from MinIO, push rows to Kafka, move to processed."""
    logger.info("Processing: %s", object_name)

    response = minio_client.get_object(MINIO_BUCKET, object_name)
    csv_text = response.read().decode("utf-8")
    response.close()
    response.release_conn()

    reader = csv.DictReader(io.StringIO(csv_text))
    batch = []
    total = 0

    for row in reader:
        doc = parse_row(row)
        if not doc["id"] and not doc["question"]:
            continue
        batch.append(doc)

        if len(batch) >= BATCH_SIZE:
            producer.send_batch(batch, topic=KAFKA_TOPIC)
            total += len(batch)
            batch = []

    if batch:
        producer.send_batch(batch, topic=KAFKA_TOPIC)
        total += len(batch)

    # Move to processed folder
    filename = object_name.replace(UNPROCESSED_PREFIX, "", 1)
    dest_path = f"{PROCESSED_PREFIX}{filename}"

    minio_client.copy_object(
        MINIO_BUCKET, dest_path,
        CopySource(MINIO_BUCKET, object_name),
    )
    minio_client.remove_object(MINIO_BUCKET, object_name)

    logger.info("Done: %s -> %s (%d documents sent to Kafka)", object_name, dest_path, total)
    return total


def poll_loop():
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    ensure_bucket(minio_client)
    producer = DocumentProducer(KAFKA_BOOTSTRAP)

    logger.info(
        "Watching MinIO %s/%s for new CSV files (poll every %ds)",
        MINIO_BUCKET, UNPROCESSED_PREFIX, POLL_INTERVAL,
    )

    while True:
        try:
            objects = minio_client.list_objects(
                MINIO_BUCKET, prefix=UNPROCESSED_PREFIX, recursive=True,
            )
            for obj in objects:
                if obj.object_name.startswith(PROCESSED_PREFIX):
                    continue
                if obj.object_name.endswith(".csv"):
                    try:
                        process_csv(minio_client, producer, obj.object_name)
                    except Exception as e:
                        logger.error("Failed to process %s: %s", obj.object_name, e)
        except Exception as e:
            logger.error("Error listing MinIO objects: %s", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    poll_loop()
