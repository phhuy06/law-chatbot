"""GPT-4o mini integration for answer generation"""
import logging

from openai import AsyncOpenAI

from backend.config import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Ban la tro ly phap luat Viet Nam. Tra loi cau hoi dua tren cac doan van ban "
    "phap luat duoc cung cap ben duoi. Neu khong tim thay thong tin phu hop, "
    "hay noi rang ban khong co du thong tin de tra loi. "
    "Luon trich dan so hieu van ban khi tra loi."
)


class LLMService:
    def __init__(self):
        self._client = AsyncOpenAI(api_key=settings.openai_api_key)

    def _build_messages(self, question: str, chunks: list[dict]) -> list[dict]:
        context_parts = []
        for i, chunk in enumerate(chunks, 1):
            title = chunk.get("title", "")
            text = chunk.get("chunk_text", "")
            context_parts.append(f"[{i}] {title}\n{text}")

        context = "\n\n".join(context_parts) if context_parts else "(Khong co tai lieu lien quan)"

        user_content = f"""Tai lieu tham khao:
{context}

Cau hoi: {question}"""

        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ]

    async def embed(self, text: str) -> list[float]:
        response = await self._client.embeddings.create(
            model="text-embedding-3-small",
            input=text,
        )
        return response.data[0].embedding

    async def generate(self, question: str, chunks: list[dict]) -> str:
        messages = self._build_messages(question, chunks)
        response = await self._client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.3,
            max_tokens=1024,
        )
        return response.choices[0].message.content
