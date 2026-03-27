"""Elasticsearch kNN + full-text search service"""
import logging

from elasticsearch import AsyncElasticsearch

from backend.config import settings

logger = logging.getLogger(__name__)


class SearchService:
    def __init__(self):
        self._es = AsyncElasticsearch(settings.elasticsearch_url)
        self._indices = [settings.es_index_batch, settings.es_index_realtime]

    def _build_knn_query(self, vector: list[float], k: int = 5) -> dict:
        return {
            "field": "embedding",
            "query_vector": vector,
            "k": k,
            "num_candidates": k * 10,
        }

    def _build_text_query(self, question: str) -> dict:
        return {
            "bool": {
                "should": [
                    {"match": {"chunk_text": {"query": question, "boost": 2.0}}},
                    {"match": {"title": {"query": question, "boost": 1.0}}},
                ],
            },
        }

    def _format_results(self, hits: list[dict]) -> list[dict]:
        results = []
        for hit in hits:
            src = hit["_source"]
            results.append({
                "chunk_text": src.get("chunk_text", ""),
                "title": src.get("title", ""),
                "url": src.get("url", ""),
                "doc_type": src.get("doc_type", ""),
                "doc_number": src.get("doc_number", ""),
                "agency": src.get("agency", ""),
                "score": hit.get("_score", 0),
            })
        return results

    async def hybrid_search(self, question: str, vector: list[float], top_k: int = 5) -> list[dict]:
        all_hits = []
        for index in self._indices:
            try:
                resp = await self._es.search(
                    index=index,
                    knn=self._build_knn_query(vector, k=top_k),
                    query=self._build_text_query(question),
                    size=top_k,
                )
                all_hits.extend(resp["hits"]["hits"])
            except Exception as e:
                logger.warning("Search failed on index %s: %s", index, e)

        all_hits.sort(key=lambda h: h.get("_score", 0), reverse=True)
        return self._format_results(all_hits[:top_k])

    async def close(self):
        await self._es.close()
