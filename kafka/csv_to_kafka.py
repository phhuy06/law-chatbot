"""Bridge script: read crawled CSV data and push to Kafka topic"""
import argparse
import csv
import logging
import sys
import time

from producer import DocumentProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CSV = "/app/crawler/output/demo-data.csv"
DEFAULT_BOOTSTRAP = "kafka:9092"
DEFAULT_TOPIC = "van-ban-phap-luat"


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
    }


def main():
    parser = argparse.ArgumentParser(description="Push crawled CSV data to Kafka")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Path to CSV file")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--batch-size", type=int, default=50, help="Docs per flush")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between batches (seconds)")
    args = parser.parse_args()

    producer = DocumentProducer(args.bootstrap_servers)

    with open(args.csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        total = 0

        for row in reader:
            doc = parse_row(row)
            if not doc["id"] and not doc["question"]:
                continue
            batch.append(doc)

            if len(batch) >= args.batch_size:
                producer.send_batch(batch, topic=args.topic)
                total += len(batch)
                logger.info("Sent %d documents so far", total)
                batch = []
                time.sleep(args.delay)

        if batch:
            producer.send_batch(batch, topic=args.topic)
            total += len(batch)

    logger.info("Done. Total documents sent to Kafka: %d", total)


if __name__ == "__main__":
    main()
