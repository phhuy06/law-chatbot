"""Initialize Elasticsearch indices with proper mappings for kNN search."""
import os
import sys
import time

from elasticsearch import Elasticsearch

ES_URL = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")

INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "vietnamese": {
                    "type": "custom",
                    "tokenizer": "icu_tokenizer",
                    "filter": ["icu_normalizer", "icu_folding", "lowercase"],
                },
            },
        },
    },
    "mappings": {
        "properties": {
            "doc_id": {"type": "keyword"},
            "chunk_text": {"type": "text", "analyzer": "vietnamese"},
            "title": {"type": "text", "analyzer": "vietnamese"},
            "url": {"type": "keyword"},
            "category": {"type": "keyword"},
            "doc_type": {"type": "keyword"},
            "embedding": {
                "type": "dense_vector",
                "dims": 1536,
                "index": True,
                "similarity": "cosine",
            },
        }
    },
}

AUDIT_INDEX_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "question_hash": {"type": "keyword"},
            "question_sanitized": {"type": "text", "analyzer": "standard"},
            "answer_length": {"type": "integer"},
            "source_count": {"type": "integer"},
            "latency_ms": {"type": "integer"},
            "cache_hit": {"type": "boolean"},
            "pii_redactions": {"type": "integer"},
        }
    },
}

INDICES = [
    (os.environ.get("ES_INDEX", "phapluat"), INDEX_MAPPING),
    (os.environ.get("ES_AUDIT_INDEX", "phapluat-audit"), AUDIT_INDEX_MAPPING),
]


def wait_for_es(es: Elasticsearch, retries: int = 30, delay: int = 2):
    for i in range(retries):
        try:
            if es.ping():
                print(f"Elasticsearch is ready at {ES_URL}")
                return
        except Exception:
            pass
        print(f"Waiting for Elasticsearch... ({i + 1}/{retries})")
        time.sleep(delay)
    print("ERROR: Elasticsearch not available", file=sys.stderr)
    sys.exit(1)


def create_indices(es: Elasticsearch):
    for index, mapping in INDICES:
        if es.indices.exists(index=index):
            print(f"Index '{index}' already exists, skipping.")
            continue
        es.indices.create(
            index=index,
            settings=mapping["settings"],
            mappings=mapping["mappings"],
        )
        print(f"Created index '{index}'.")


def main():
    es = Elasticsearch(ES_URL)
    wait_for_es(es)
    create_indices(es)
    print("Elasticsearch initialization complete.")


if __name__ == "__main__":
    main()
