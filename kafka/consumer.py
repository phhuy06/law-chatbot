"""Kafka consumer - saves raw JSON to MinIO and forwards to Spark Streaming"""
import json
import logging
from datetime import datetime
from io import BytesIO

from confluent_kafka import Consumer, KafkaException
from minio import Minio

logger = logging.getLogger(__name__)


class DocumentConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str = "law-chatbot-consumer",
        minio_endpoint: str = "localhost:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin",
        minio_bucket: str = "phapluat",
    ):
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        })
        self._minio = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False,
        )
        self._bucket = minio_bucket
        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self._minio.bucket_exists(self._bucket):
            self._minio.make_bucket(self._bucket)
            logger.info("Created MinIO bucket: %s", self._bucket)

    def _minio_path(self, crawled_at: str, doc_id: str | None = None) -> str:
        dt = datetime.fromisoformat(crawled_at.replace("Z", "+00:00"))
        filename = doc_id if doc_id else crawled_at
        return f"phapluat/raw/{dt.year}/{dt.month:02d}/{filename}.json"

    def _save_to_minio(self, document: dict):
        path = self._minio_path(
            document["crawled_at"],
            doc_id=document.get("id"),
        )
        data = json.dumps(document, ensure_ascii=False).encode("utf-8")
        self._minio.put_object(
            self._bucket,
            path,
            BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        logger.info("Saved to MinIO: %s", path)

    def consume(self, topic: str = "van-ban-phap-luat"):
        self._consumer.subscribe([topic])
        logger.info("Subscribed to topic: %s", topic)

        try:
            while True:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                document = json.loads(msg.value().decode("utf-8"))
                self._save_to_minio(document)
                logger.info("Processed document: %s", document.get("id", "unknown"))
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self._consumer.close()
