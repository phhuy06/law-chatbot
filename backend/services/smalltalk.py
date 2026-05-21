"""Lightweight intent prefilter for non-legal queries.

Returns a canned answer for greetings, identity, capability, thanks, and goodbye
so the RAG pipeline doesn't waste embeddings + ES + GPT calls on them.
"""
from __future__ import annotations

import re

_CAPABILITIES_REPLY = (
    "Tôi có thể giúp bạn:\n"
    "1. Tra cứu văn bản pháp luật Việt Nam (luật, nghị định, thông tư, bộ luật).\n"
    "2. Trả lời câu hỏi pháp lý dựa trên nội dung văn bản thực tế, kèm trích dẫn nguồn.\n"
    "3. Tổng hợp các quy định liên quan đến vấn đề bạn quan tâm.\n\n"
    "Bạn có thể thử các câu hỏi sau:\n"
    "• Bao nhiêu tuổi thì được kết hôn?\n"
    "• Làm giấy khai sinh cho con cần giấy tờ gì?\n"
    "• Trẻ sơ sinh có quyền thừa kế không?\n"
    "• Hút shisha có bị phạt không?"
)

_GREETING_REPLY = (
    "Xin chào! Tôi là trợ lý tư vấn pháp luật Việt Nam. "
    "Bạn có thể hỏi tôi về luật dân sự, hình sự, đất đai, hôn nhân – gia đình, "
    "thừa kế, lao động, doanh nghiệp… và tôi sẽ tra cứu văn bản pháp luật để trả lời."
)

_IDENTITY_REPLY = (
    "Tôi là chatbot tư vấn pháp luật Việt Nam, hoạt động theo kiến trúc RAG "
    "(Retrieval-Augmented Generation): tra cứu văn bản pháp luật từ cơ sở dữ liệu "
    "rồi dùng mô hình ngôn ngữ để tổng hợp câu trả lời kèm trích dẫn nguồn."
)

_THANKS_REPLY = "Rất vui được giúp bạn! Nếu còn câu hỏi pháp lý nào khác, hãy cứ hỏi nhé."

_GOODBYE_REPLY = "Tạm biệt! Chúc bạn một ngày tốt lành."

# Regex patterns chosen to match the *whole* short query, not embedded keywords.
# We don't want "bạn có thể làm gì để thừa kế tài sản" to match "bạn làm gì".
_INTENTS: list[tuple[str, re.Pattern[str]]] = [
    (
        _GREETING_REPLY,
        re.compile(
            r"^(xin\s*chào|chào(\s+(bạn|anh|chị|em))?|hello|hi|hey|halo)\W*$",
            re.IGNORECASE,
        ),
    ),
    (
        _CAPABILITIES_REPLY,
        re.compile(
            r"^(bạn\s+(có\s+thể\s+)?(làm|giúp)\s+(được\s+)?(gì|j)(\s+nhiều)?"
            r"|chức\s+năng\s+của\s+bạn"
            r"|what\s+can\s+you\s+do)\W*$",
            re.IGNORECASE,
        ),
    ),
    (
        _IDENTITY_REPLY,
        re.compile(
            r"^(bạn\s+là\s+(ai|gì)|bạn\s+tên\s+(là\s+)?gì|who\s+are\s+you|giới\s+thiệu\s+về\s+bạn)\W*$",
            re.IGNORECASE,
        ),
    ),
    (
        _THANKS_REPLY,
        re.compile(
            r"^(cảm\s*ơn|cám\s*ơn|thanks?(\s+you)?|thank\s+u|tks|ty)\W*$",
            re.IGNORECASE,
        ),
    ),
    (
        _GOODBYE_REPLY,
        re.compile(
            r"^(tạm\s+biệt|chào\s+(tạm\s+biệt|bạn\s+nhé)|bye(\s+bye)?|goodbye)\W*$",
            re.IGNORECASE,
        ),
    ),
]


def match_smalltalk(question: str) -> str | None:
    q = question.strip()
    if not q:
        return None
    for reply, pattern in _INTENTS:
        if pattern.match(q):
            return reply
    return None
