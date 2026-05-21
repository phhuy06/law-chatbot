"""POST /api/chat - RAG pipeline endpoint"""
import logging
import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.config import settings
from backend.services.audit import AuditService
from backend.services.cache import CacheService
from backend.services.llm import LLMService
from backend.services.metrics import (
    chat_latency_seconds,
    chat_requests_total,
    rag_sources_returned,
)
from backend.services.search import SearchService
from backend.services.smalltalk import match_smalltalk

logger = logging.getLogger(__name__)

router = APIRouter()

cache_service = CacheService()
search_service = SearchService()
llm_service = LLMService()
audit_service = AuditService()

NO_INFO_REPLY = "Xin lỗi, tôi không tìm thấy thông tin liên quan trong cơ sở dữ liệu."


class HistoryTurn(BaseModel):
    role: str  # "user" | "assistant"
    content: str


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1)
    history: list[HistoryTurn] = Field(default_factory=list)


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

    # 1. Smalltalk prefilter — greetings, identity, capability, thanks, goodbye
    smalltalk_reply = match_smalltalk(question)
    if smalltalk_reply:
        elapsed = time.monotonic() - started
        chat_requests_total.labels(path="smalltalk").inc()
        chat_latency_seconds.labels(path="smalltalk").observe(elapsed)
        await audit_service.log(
            question=question,
            answer_length=len(smalltalk_reply),
            source_count=0,
            latency_ms=int(elapsed * 1000),
            cache_hit=False,
        )
        return {"answer": smalltalk_reply, "sources": []}

    # 2. Check cache
    cached = await cache_service.get(question)
    if cached:
        logger.info("Cache hit for question (hash)")
        elapsed = time.monotonic() - started
        chat_requests_total.labels(path="cache").inc()
        chat_latency_seconds.labels(path="cache").observe(elapsed)
        await audit_service.log(
            question=question,
            answer_length=len(cached.get("answer", "")),
            source_count=len(cached.get("sources", [])),
            latency_ms=int(elapsed * 1000),
            cache_hit=True,
        )
        return cached

    # 3. Query rewriting — turn follow-up into standalone query using history
    history_for_rewrite = [t.model_dump() for t in req.history] if req.history else []
    search_query = await llm_service.rewrite_query(history_for_rewrite, question)
    if search_query != question:
        logger.info("Rewrote query: %r -> %r", question, search_query)

    # 4. Embed (rewritten) query
    try:
        vector = await llm_service.embed(search_query)
    except Exception as exc:
        logger.error("Embedding failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Failed to generate embedding for the question. Please try again later.",
        )

    # 5. Hybrid search (kNN + full-text)
    try:
        chunks = await search_service.hybrid_search(search_query, vector, top_k=10)
    except Exception as exc:
        logger.error("Elasticsearch search failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Search service is currently unavailable. Please try again later.",
        )

    # 6. Score threshold — if best hit is too weak, skip GPT and refuse cleanly
    top_score = chunks[0].get("score", 0) if chunks else 0
    if top_score < settings.min_retrieval_score:
        logger.info(
            "Retrieval below threshold (top=%.2f < %.2f) for %r",
            top_score, settings.min_retrieval_score, question,
        )
        elapsed = time.monotonic() - started
        chat_requests_total.labels(path="threshold_refused").inc()
        chat_latency_seconds.labels(path="threshold_refused").observe(elapsed)
        await audit_service.log(
            question=question,
            answer_length=len(NO_INFO_REPLY),
            source_count=0,
            latency_ms=int(elapsed * 1000),
            cache_hit=False,
        )
        return {"answer": NO_INFO_REPLY, "sources": []}

    # 7. Generate answer — GPT returns answer + which chunks it used
    try:
        answer, used_indices = await llm_service.generate(question, chunks)
    except Exception as exc:
        logger.error("LLM generation failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Language model service is currently unavailable. Please try again later.",
        )

    # 8. Build sources — only from chunks GPT actually used (deduplicate by URL)
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

    # 9. Cache response
    await cache_service.set(question, response)

    elapsed = time.monotonic() - started
    path_label = "no_info" if no_info else "rag"
    chat_requests_total.labels(path=path_label).inc()
    chat_latency_seconds.labels(path=path_label).observe(elapsed)
    rag_sources_returned.observe(len(sources))
    await audit_service.log(
        question=question,
        answer_length=len(answer),
        source_count=len(sources),
        latency_ms=int(elapsed * 1000),
        cache_hit=False,
    )

    return response
