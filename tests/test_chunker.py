"""Unit tests for Spark UDF logic (clean_text, chunk_text).

These call the underlying Python functions directly (not the Spark UDF
wrappers), so they need no SparkSession.
"""
import pytest

from spark.utils.text import chunk_text as chunk_text_udf
from spark.utils.text import clean_text as clean_text_udf


class TestCleanText:
    def test_strips_html(self):
        html = "<p>Điều 1. Nhà nước <b>Cộng hoà</b> XHCN Việt Nam.</p>"
        result = clean_text_udf(html)
        assert "<p>" not in result
        assert "<b>" not in result
        assert "Điều 1" in result
        assert "Cộng hoà" in result

    def test_collapses_whitespace(self):
        html = "<p>A    B\n\n\nC</p>"
        assert clean_text_udf(html) == "A B C"

    def test_handles_empty_input(self):
        assert clean_text_udf("") == ""
        assert clean_text_udf(None) == ""

    def test_handles_non_string(self):
        assert clean_text_udf(123) == ""

    def test_unicode_normalization(self):
        # Composed vs decomposed Vietnamese diacritics should normalize.
        composed = "Việt"
        decomposed = "Việt"
        assert clean_text_udf(composed) == clean_text_udf(decomposed)


class TestChunkText:
    def test_short_text_returns_single_chunk(self):
        text = "Câu ngắn."
        chunks = chunk_text_udf(text)
        assert chunks == [text]

    def test_empty_text(self):
        assert chunk_text_udf("") == []
        assert chunk_text_udf(None) == []

    def test_respects_max_length(self):
        # Make sure no chunk exceeds max_length by an unreasonable margin.
        text = ". ".join([f"Câu số {i} này là một câu dài đủ để phân đoạn" for i in range(200)])
        chunks = chunk_text_udf(text, max_length=500, overlap=50)
        assert len(chunks) > 1
        for c in chunks:
            assert len(c) <= 500 + 50, f"chunk too large: {len(c)}"

    def test_splits_long_single_sentence(self):
        # A single sentence longer than max_length must still be split.
        long_sentence = "a" * 3000
        chunks = chunk_text_udf(long_sentence, max_length=1000, overlap=100)
        assert len(chunks) >= 3
        for c in chunks:
            assert len(c) <= 1000

    def test_preserves_sentence_boundaries(self):
        text = "Điều 1. Quy định A. Điều 2. Quy định B. Điều 3. Quy định C."
        chunks = chunk_text_udf(text, max_length=200)
        joined = " ".join(chunks)
        # Every "Điều N" marker should survive chunking.
        for marker in ("Điều 1", "Điều 2", "Điều 3"):
            assert marker in joined


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
