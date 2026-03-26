"""Application configuration from environment variables"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    openai_api_key: str = ""
    elasticsearch_url: str = "http://elasticsearch:9200"
    es_index_batch: str = "phapluat-batch"
    es_index_realtime: str = "phapluat-realtime"
    redis_url: str = "redis://redis:6379/0"
    redis_cache_ttl: int = 3600

    class Config:
        env_file = ".env"


settings = Settings()
