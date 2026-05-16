"""Unit tests for PII sanitization in the audit service."""
import pytest

from backend.services.audit import sanitize


class TestSanitize:
    def test_redacts_email(self):
        text = "Liên hệ huy.phamquang@example.com để biết thêm"
        sanitized, n = sanitize(text)
        assert "huy.phamquang@example.com" not in sanitized
        assert "[EMAIL]" in sanitized
        assert n == 1

    def test_redacts_phone(self):
        for raw in ("0912345678", "+84912345678", "0912 345 678", "0912.345.678"):
            sanitized, n = sanitize(f"Gọi {raw} ngay")
            assert raw not in sanitized
            assert "[PHONE]" in sanitized
            assert n >= 1

    def test_redacts_cmnd_9_digits(self):
        sanitized, n = sanitize("CMND của tôi là 123456789")
        assert "123456789" not in sanitized
        assert "[ID]" in sanitized or "[TAX_ID]" in sanitized

    def test_redacts_cccd_12_digits(self):
        sanitized, n = sanitize("CCCD: 012345678901")
        assert "012345678901" not in sanitized

    def test_keeps_clean_text_unchanged(self):
        text = "Luật doanh nghiệp năm 2020 quy định những gì?"
        sanitized, n = sanitize(text)
        assert sanitized == text
        assert n == 0

    def test_redaction_count_aggregates(self):
        text = "Email: a@b.com, phone: 0912345678"
        sanitized, n = sanitize(text)
        assert n >= 2
        assert "[EMAIL]" in sanitized
        assert "[PHONE]" in sanitized


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
