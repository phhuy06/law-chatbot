"""Application configuration from environment variables"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # OpenAI
    openai_api_key: str = ""

    # Elasticsearch
    elasticsearch_url: str = "http://localhost:9200"
    es_index_batch: str = "phapluat-batch"
    es_index_realtime: str = "phapluat-realtime"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_cache_ttl: int = 3600

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
