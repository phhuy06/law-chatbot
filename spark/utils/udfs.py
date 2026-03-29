import re
import unicodedata
from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType

def clean_text_udf(html_content: str) -> str:
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=" ")
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text
def chunk_text_udf(text: str) -> list[str]:
    if not text or len(text) < 50:
        return [text] if text else []
    max_length = 500
    overlap_size = 50
    sentences = re.split(r'(?<=[.;])\s+|\n+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    
    chunks = []
    current_chunk = ""
    
    for sentence in sentences:
        if len(sentence) > max_length:
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            for i in range(0, len(sentence), max_length - overlap_size):
                chunks.append(sentence[i:i + max_length])
            continue

        test_chunk = (current_chunk + " " + sentence).strip() if current_chunk else sentence
        
        if len(test_chunk) <= max_length:
            current_chunk = test_chunk
        else:
            if current_chunk:
                chunks.append(current_chunk)
                overlap_text = current_chunk[-overlap_size:]
                last_space = overlap_text.find(' ') 
                if last_space != -1:
                    overlap_text = overlap_text[last_space:].strip()
                
                current_chunk = (overlap_text + " " + sentence).strip()
            else:
                current_chunk = sentence
    if current_chunk:
        chunks.append(current_chunk)
    return chunks

clean_text = udf(clean_text_udf, StringType())
chunk_text = udf(chunk_text_udf, ArrayType(StringType()))