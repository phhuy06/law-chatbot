"""FastAPI application entry point"""
import logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from backend.config import settings
from backend.routers.chat import router as chat_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not settings.openai_api_key or settings.openai_api_key == "sk-xxx":
        logger.warning("OPENAI_API_KEY is not set — LLM features will fail")
    yield


app = FastAPI(title="Legal Chatbot API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat_router)


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    checks = {}

    es = AsyncElasticsearch(settings.elasticsearch_url)
    try:
        if await es.ping():
            checks["elasticsearch"] = "ok"
        else:
            checks["elasticsearch"] = "unreachable"
    except Exception as e:
        logger.warning("Health check ES ping failed: %s", e)
        checks["elasticsearch"] = "error"
    finally:
        await es.close()

    r = aioredis.from_url(settings.redis_url)
    try:
        await r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        logger.warning("Health check Redis ping failed: %s", e)
        checks["redis"] = "error"
    finally:
        await r.close()

    all_ok = all(v == "ok" for v in checks.values())
    status_code = 200 if all_ok else 503

    return JSONResponse(
        status_code=status_code,
        content={"status": "ok" if all_ok else "degraded", "checks": checks},
    )
