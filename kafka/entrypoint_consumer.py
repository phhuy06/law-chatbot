"""Entrypoint for running the Kafka consumer as a Docker service."""
import logging
import os

from consumer import DocumentConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    consumer = DocumentConsumer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.environ.get("MINIO_BUCKET", "phapluat"),
    )
    consumer.consume(topic=os.environ.get("KAFKA_TOPIC", "van-ban-phap-luat"))


if __name__ == "__main__":
    main()
