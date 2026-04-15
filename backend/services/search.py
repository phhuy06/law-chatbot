"""Elasticsearch hybrid search service (kNN + full-text)"""
import logging

from elasticsearch import AsyncElasticsearch

from backend.config import settings

logger = logging.getLogger(__name__)


class SearchService:
    def __init__(self):
        self._es = AsyncElasticsearch(settings.elasticsearch_url)
        self._index = settings.es_index

    def _format_results(self, hits: list[dict]) -> list[dict]:
        results = []
        for hit in hits:
            src = hit["_source"]
            results.append({
                "chunk_text": src.get("chunk_text", ""),
                "title": src.get("title", ""),
                "url": src.get("url", ""),
                "doc_type": src.get("doc_type", ""),
                "doc_id": src.get("doc_id", ""),
                "agency": src.get("agency", ""),
                "score": hit.get("_score", 0),
            })
        return results

    async def hybrid_search(self, question: str, vector: list[float], top_k: int = 10) -> list[dict]:
        """Hybrid search: kNN (low boost) + text match (high boost)."""
        try:
            resp = await self._es.search(
                index=self._index,
                knn={
                    "field": "embedding",
                    "query_vector": vector,
                    "k": top_k,
                    "num_candidates": top_k * 10,
                    "boost": 0.3,
                },
                query={
                    "bool": {
                        "should": [
                            {"match": {"chunk_text": {"query": question, "boost": 5.0}}},
                            {"match": {"title": {"query": question, "boost": 3.0}}},
                        ],
                    },
                },
                size=top_k,
            )
            hits = resp["hits"]["hits"]
        except Exception as e:
            logger.warning("Search failed on index %s: %s", self._index, e)
            hits = []
        return self._format_results(hits)

    async def close(self):
        await self._es.close()
