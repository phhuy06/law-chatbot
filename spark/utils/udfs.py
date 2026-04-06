import re
import unicodedata
from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, FloatType
import os
from openai import OpenAI

def clean_text_udf(html_content: str) -> str:
    if not html_content or not isinstance(html_content, str):
        return ""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=" ")
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def chunk_text_udf(text: str, max_length: int = 500, overlap: int = 50) -> list[str]:
    if not text or len(text) < 50:
        return [text] if text else []
    sentences = re.split(r'(?<=[.!?])\s+|\n+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    
    chunks = []
    current_chunk = []
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

clean_text = udf(clean_text_udf, StringType())
chunk_text = udf(chunk_text_udf, ArrayType(StringType()))

def get_embedding_udf(text: str) -> list[float]:
    api_key = os.environ.get("OPENAI_API_KEY", "api_key_chua_co")
    if api_key == "api_key_chua_co":
        return [0.01] * 1536
        
    try:
        client = OpenAI(api_key=api_key)
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"Lỗi khi nhúng text: {e}")
        return [0.01] * 1536

embed_text = udf(get_embedding_udf, ArrayType(FloatType()))