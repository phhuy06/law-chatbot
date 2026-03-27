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
    vector = await llm_service.embed(question)

    # 3. Search ES (kNN + full-text, both indices)
    chunks = await search_service.hybrid_search(question, vector, top_k=5)

    # 4. Generate answer with GPT
    answer = await llm_service.generate(question, chunks)

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
