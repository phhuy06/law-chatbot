"""POST /api/chat - RAG pipeline endpoint"""
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.services.cache import CacheService
from backend.services.llm import LLMService
from backend.services.search import SearchService

logger = logging.getLogger(__name__)

router = APIRouter()

cache_service = CacheService()
search_service = SearchService()
llm_service = LLMService()


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1)


class Source(BaseModel):
    title: str
    url: str
    doc_number: str


class ChatResponse(BaseModel):
    answer: str
    sources: list[Source]


@router.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    question = req.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    # 1. Check cache
    cached = await cache_service.get(question)
    if cached:
        logger.info("Cache hit for question: %s", question[:50])
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

    # 3. Search ES (kNN + full-text, both indices)
    try:
        chunks = await search_service.hybrid_search(question, vector, top_k=5)
    except Exception as exc:
        logger.error("Elasticsearch search failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Search service is currently unavailable. Please try again later.",
        )

    # 4. Generate answer with GPT
    try:
        answer = await llm_service.generate(question, chunks)
    except Exception as exc:
        logger.error("LLM generation failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Language model service is currently unavailable. Please try again later.",
        )

    # 5. Build sources (deduplicate by URL)
    seen_urls = set()
    sources = []
    for chunk in chunks:
        url = chunk.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            sources.append({
                "title": chunk.get("title", ""),
                "url": url,
                "doc_number": chunk.get("doc_number", ""),
            })

    response = {"answer": answer, "sources": sources}

    # 6. Cache response
    await cache_service.set(question, response)

    return response
