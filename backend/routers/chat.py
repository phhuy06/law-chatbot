"""POST /api/chat - RAG pipeline endpoint"""
import logging
import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.services.audit import AuditService
from backend.services.cache import CacheService
from backend.services.llm import LLMService
from backend.services.search import SearchService

logger = logging.getLogger(__name__)

router = APIRouter()

cache_service = CacheService()
search_service = SearchService()
llm_service = LLMService()
audit_service = AuditService()


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1)


class Source(BaseModel):
    title: str
    url: str
    doc_id: str


class ChatResponse(BaseModel):
    answer: str
    sources: list[Source]


@router.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    question = req.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    started = time.monotonic()

    # 1. Check cache
    cached = await cache_service.get(question)
    if cached:
        logger.info("Cache hit for question (hash)")
        latency_ms = int((time.monotonic() - started) * 1000)
        await audit_service.log(
            question=question,
            answer_length=len(cached.get("answer", "")),
            source_count=len(cached.get("sources", [])),
            latency_ms=latency_ms,
            cache_hit=True,
        )
        return cached

    # 2. Embed question
    try:
        vector = await llm_service.embed(question)
    except Exception as exc:
        logger.error("Embedding failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Failed to generate embedding for the question. Please try again later.",
        )

    # 3. Hybrid search (kNN + full-text)
    try:
        chunks = await search_service.hybrid_search(question, vector, top_k=10)
    except Exception as exc:
        logger.error("Elasticsearch search failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Search service is currently unavailable. Please try again later.",
        )

    # 4. Generate answer — GPT returns answer + which chunks it used
    try:
        answer, used_indices = await llm_service.generate(question, chunks)
    except Exception as exc:
        logger.error("LLM generation failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Language model service is currently unavailable. Please try again later.",
        )

    # 5. Build sources — only from chunks GPT actually used (deduplicate by URL)
    no_info = "không tìm thấy thông tin" in answer.lower()

    seen_urls: set[str] = set()
    sources = []
    if no_info:
        used_chunks = []
    elif used_indices:
        used_chunks = [chunks[i] for i in used_indices if i < len(chunks)]
    else:
        # GPT answered but forgot to list sources — use top 3
        used_chunks = chunks[:3]

    for chunk in used_chunks:
        url = chunk.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            sources.append({
                "title": chunk.get("title", ""),
                "url": url,
                "doc_id": chunk.get("doc_id", ""),
            })

    response = {"answer": answer, "sources": sources}

    # 6. Cache response
    await cache_service.set(question, response)

    latency_ms = int((time.monotonic() - started) * 1000)
    await audit_service.log(
        question=question,
        answer_length=len(answer),
        source_count=len(sources),
        latency_ms=latency_ms,
        cache_hit=False,
    )

    return response
