"""Application configuration from environment variables"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # OpenAI
    openai_api_key: str = ""

    # Elasticsearch
    elasticsearch_url: str = "http://localhost:9200"
    es_index: str = "phapluat"
    es_audit_index: str = "phapluat-audit"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_cache_ttl: int = 3600

    # Retrieval
    # If the top ES hit's score is below this, skip GPT and return "no info".
    # Hybrid query mixes text-match (boost 5/3) with kNN (boost 0.3), so legal
    # hits usually score > 5 and smalltalk noise scores < 2.
    min_retrieval_score: float = 2.0

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "van-ban-phap-luat"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "phapluat"

    class Config:
        env_file = ".env"


settings = Settings()
