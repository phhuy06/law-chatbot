"""Pure Python text helpers (no PySpark dependency).

These are unit-testable without a SparkSession. ``spark/utils/udfs.py``
wraps them as Spark UDFs.
"""
import re
import unicodedata

from bs4 import BeautifulSoup


def clean_text(html_content: str) -> str:
    if not html_content or not isinstance(html_content, str):
        return ""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=" ")
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def chunk_text(text: str, max_length: int = 1000, overlap: int = 100) -> list[str]:
    if not text or len(text) < 50:
        return [text] if text else []
    sentences = re.split(r"(?<=[.!?])\s+|\n+", text)
    sentences = [s.strip() for s in sentences if s.strip()]

    chunks: list[str] = []
    current_chunk: list[str] = []
    current_length = 0

    for sentence in sentences:
        sentence_len = len(sentence)
        if sentence_len > max_length:
            if current_chunk:
                chunks.append(" ".join(current_chunk))
                current_chunk = []
                current_length = 0
            for i in range(0, sentence_len, max_length - overlap):
                chunk = sentence[i:i + max_length]
                chunks.append(chunk.strip())
            continue
        if current_chunk:
            test_length = current_length + 1 + sentence_len
        else:
            test_length = sentence_len

        if test_length <= max_length:
            current_chunk.append(sentence)
            current_length = test_length
        else:
            if current_chunk:
                chunks.append(" ".join(current_chunk))
            if overlap > 0 and current_chunk:
                overlap_text = " ".join(current_chunk)[-overlap:]
                current_chunk = [overlap_text] if overlap_text else []
                current_length = len(overlap_text)
            else:
                current_chunk = []
                current_length = 0
            current_chunk.append(sentence)
            current_length += sentence_len + 1 if current_chunk else sentence_len

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks
