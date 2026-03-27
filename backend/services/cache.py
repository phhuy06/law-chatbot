"""Redis cache service for question-answer pairs"""
import hashlib
import json
import logging

import redis.asyncio as redis

from backend.config import settings

logger = logging.getLogger(__name__)


class CacheService:
    def __init__(self):
        self._redis = redis.from_url(settings.redis_url, decode_responses=True)
        self._ttl = settings.redis_cache_ttl

    def _make_key(self, question: str) -> str:
        h = hashlib.sha256(question.encode()).hexdigest()
        return f"chat:{h}"

    async def get(self, question: str) -> dict | None:
        key = self._make_key(question)
        data = await self._redis.get(key)
        if data is None:
            return None
        logger.info("Cache hit for key: %s", key[:20])
        return json.loads(data)

    async def set(self, question: str, response: dict):
        key = self._make_key(question)
        await self._redis.setex(key, self._ttl, json.dumps(response, ensure_ascii=False))
        logger.info("Cached response for key: %s (TTL=%ds)", key[:20], self._ttl)
