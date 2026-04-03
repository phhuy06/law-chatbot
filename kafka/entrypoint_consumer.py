"""Entrypoint for running the Kafka consumer as a Docker service."""
import logging

from consumer import DocumentConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    consumer = DocumentConsumer(
        bootstrap_servers="kafka:9092",
        minio_endpoint="minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket="phapluat",
    )
    consumer.consume(topic="van-ban-phap-luat")


if __name__ == "__main__":
    main()
