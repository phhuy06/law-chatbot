"""Audit log for chat queries.

Writes a sanitized record per query to a separate Elasticsearch index
(``phapluat-audit``). PII (phone numbers, emails, Vietnamese CMND/CCCD,
tax IDs) is redacted from the stored question before persistence.

Best-effort: all failures are swallowed so audit logging never breaks the
user-facing request path.
"""
import hashlib
import logging
import re
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Vietnamese-relevant PII patterns. Conservative on purpose — we redact
# rather than try to be clever about formatting.
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
_PHONE_RE = re.compile(r"\b(?:\+?84|0)(?:\d[\s.-]?){8,10}\b")
_CMND_RE = re.compile(r"\b\d{9}\b|\b\d{12}\b")
_TAX_RE = re.compile(r"\b\d{10}(?:-\d{3})?\b")


def sanitize(text: str) -> tuple[str, int]:
    """Redact PII from text. Returns (sanitized_text, redaction_count)."""
    redactions = 0
    for pattern, label in (
        (_EMAIL_RE, "[EMAIL]"),
        (_PHONE_RE, "[PHONE]"),
        (_TAX_RE, "[TAX_ID]"),
        (_CMND_RE, "[ID]"),
    ):
        text, n = pattern.subn(label, text)
        redactions += n
    return text, redactions


class AuditService:
    def __init__(self):
        # Lazy imports so unit tests can exercise ``sanitize`` without the
        # full FastAPI/elasticsearch dependency tree installed.
        from elasticsearch import AsyncElasticsearch

        from backend.config import settings

        self._es = AsyncElasticsearch(settings.elasticsearch_url)
        self._index = getattr(settings, "es_audit_index", "phapluat-audit")

    async def log(
        self,
        *,
        question: str,
        answer_length: int,
        source_count: int,
        latency_ms: int,
        cache_hit: bool,
    ) -> None:
        try:
            sanitized, redactions = sanitize(question)
            doc = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "question_hash": hashlib.sha256(question.encode()).hexdigest(),
                "question_sanitized": sanitized,
                "answer_length": answer_length,
                "source_count": source_count,
                "latency_ms": latency_ms,
                "cache_hit": cache_hit,
                "pii_redactions": redactions,
            }
            await self._es.index(index=self._index, document=doc)
        except Exception as exc:
            logger.warning("Audit log write failed (non-fatal): %s", exc)

    async def close(self):
        await self._es.close()
