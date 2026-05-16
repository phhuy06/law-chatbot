import os

from openai import OpenAI
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType, StringType

from spark.utils.text import chunk_text as chunk_text_fn
from spark.utils.text import clean_text as clean_text_fn

# Re-export pure Python functions for ergonomic imports (and tests).
clean_text_udf = clean_text_fn
chunk_text_udf = chunk_text_fn

clean_text = udf(clean_text_fn, StringType())
chunk_text = udf(chunk_text_fn, ArrayType(StringType()))

_embed_counter = 0


def get_embedding_udf(text: str) -> list[float]:
    global _embed_counter
    _embed_counter += 1
    preview = text[:80].replace("\n", " ") if text else "(empty)"

    api_key = os.environ.get("OPENAI_API_KEY", "api_key_chua_co")
    if api_key == "api_key_chua_co":
        print(f"[embed #{_embed_counter}] SKIP (no API key): {preview}...")
        return [0.01] * 1536

    try:
        print(f"[embed #{_embed_counter}] Calling OpenAI for: {preview}...")
        client = OpenAI(api_key=api_key)
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        print(f"[embed #{_embed_counter}] OK (1536 dims)")
        return response.data[0].embedding
    except Exception as e:
        print(f"[embed #{_embed_counter}] ERROR: {e}")
        return [0.01] * 1536


embed_text = udf(get_embedding_udf, ArrayType(FloatType()))
