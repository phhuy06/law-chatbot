# RAG Evaluation

Offline retrieval-quality check for the legal chatbot.

## Files

- `gold_set.json` — Vietnamese legal queries with expected keywords + categories.
- `run_eval.py` — runs each query against the backend (or ES directly) and reports precision@k, recall@k, category hit rate.

## Usage

```bash
# Against the live FastAPI backend (requires LLM key)
python eval/run_eval.py --backend http://localhost:8000

# Against Elasticsearch directly (no LLM cost, retrieval-only)
python eval/run_eval.py --direct --es-url http://localhost:9200
```

## Metrics

- **precision@k**: fraction of returned sources whose title/text matches an expected keyword.
- **recall@k**: 1.0 if any returned source matches, else 0.0 (per-query, then averaged).
- **category hit rate**: fraction of queries where a returned source has the expected category.
