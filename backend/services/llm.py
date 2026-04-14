"""GPT-4o mini integration for answer generation"""
import json
import logging

from openai import AsyncOpenAI

from backend.config import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Bạn là trợ lý tư vấn pháp luật Việt Nam. "
    "Quy tắc: "
    "- Trả lời dựa trên các đoạn văn bản được cung cấp. Tổng hợp thông tin từ tất cả các đoạn để trả lời đầy đủ nhất. "
    "- Nếu các đoạn văn bản có chứa thông tin liên quan đến câu hỏi, hãy sử dụng nó để trả lời dù chủ đề không phải pháp luật thuần túy. "
    "- Nếu KHÔNG có đoạn nào chứa thông tin liên quan, trả lời: 'Xin lỗi, tôi không tìm thấy thông tin liên quan trong cơ sở dữ liệu.' "
    "- KHÔNG viết trích dẫn nguồn trong nội dung câu trả lời. "
    "\n"
    "Trả lời theo đúng format JSON sau:\n"
    '{"answer": "nội dung trả lời", "used_sources": [0, 2]}\n'
    "Trong đó used_sources là danh sách số thứ tự các đoạn văn bản mà bạn ĐÃ SỬ DỤNG để trả lời. "
    "Nếu không dùng đoạn nào, trả về []."
)


class LLMService:
    def __init__(self):
        self._client = AsyncOpenAI(api_key=settings.openai_api_key)

    def _build_messages(self, question: str, chunks: list[dict]) -> list[dict]:
        context_parts = []
        for i, chunk in enumerate(chunks):
            title = chunk.get("title", "")
            text = chunk.get("chunk_text", "")
            context_parts.append(f"[{i}] {title}\n{text}")

        context = "\n\n".join(context_parts) if context_parts else "(Không có tài liệu liên quan)"

        user_content = f"""Tài liệu tham khảo:
{context}

Câu hỏi: {question}"""

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

    async def generate(self, question: str, chunks: list[dict]) -> tuple[str, list[int]]:
        """Returns (answer_text, list_of_used_chunk_indices)."""
        messages = self._build_messages(question, chunks)
        response = await self._client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.3,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content

        # Parse JSON response
        try:
            # Handle cases where GPT wraps in ```json ... ```
            cleaned = raw.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            data = json.loads(cleaned)
            answer = data.get("answer", raw)
            used = data.get("used_sources", [])
            return answer, [int(i) for i in used if isinstance(i, (int, float))]
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning("Failed to parse structured response, using raw: %s", raw[:100])
            return raw, list(range(len(chunks)))
