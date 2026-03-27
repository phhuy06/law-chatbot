"""Kafka producer - pushes crawled documents to topic van-ban-phap-luat"""
import json
import logging

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class DocumentProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def _serialize(self, document: dict) -> bytes:
        return json.dumps(document, ensure_ascii=False).encode("utf-8")

    def _delivery_report(self, err, msg):
        if err:
            logger.error("Delivery failed for %s: %s", msg.key(), err)
        else:
            logger.info("Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())

    def send(self, document: dict, topic: str = "van-ban-phap-luat"):
        self._producer.produce(
            topic=topic,
            value=self._serialize(document),
            key=document.get("id", "").encode("utf-8"),
            callback=self._delivery_report,
        )
        self._producer.flush()

    def send_batch(self, documents: list[dict], topic: str = "van-ban-phap-luat"):
        for doc in documents:
            self._producer.produce(
                topic=topic,
                value=self._serialize(doc),
                key=doc.get("id", "").encode("utf-8"),
                callback=self._delivery_report,
            )
        self._producer.flush()
        logger.info("Flushed batch of %d documents", len(documents))
