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
        # Robust to legacy/corrupt rows where crawled_at is missing or contains
        # garbage (a few old CSVs had columns shifted, putting a URL in this slot).
        # In those cases we fall back to "now" so the doc still lands somewhere.
        try:
            dt = datetime.fromisoformat((crawled_at or "").replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            from datetime import timezone
            dt = datetime.now(timezone.utc)
        filename = doc_id if doc_id else "unknown"
        return f"master/{dt.year}/{dt.month:02d}/{filename}.json"

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
                    logger.warning("Consumer error: %s", msg.error())
                    continue

                try:
                    document = json.loads(msg.value().decode("utf-8"))
                    self._save_to_minio(document)
                    logger.info("Processed document: %s", document.get("id", "unknown"))
                except Exception as e:
                    # Never crash the consumer on a single malformed message —
                    # log and skip. We've seen URLs in the crawled_at field on
                    # legacy CSVs; the per-message try/except keeps the rest of
                    # the batch flowing.
                    logger.error("Skipping bad message: %s  (msg=%s)", e, msg.value()[:200])
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self._consumer.close()
