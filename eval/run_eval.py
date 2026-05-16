"""Offline evaluation of the RAG retrieval layer.

For each query in ``gold_set.json`` we query the deployed backend's
``/api/chat`` endpoint (or, with ``--direct``, the Elasticsearch index
directly) and check how many returned sources match expected keywords
or categories. Computes:

* precision@k — fraction of returned sources that match
* recall@k    — whether at least one expected source appears
* category hit rate

Usage:
    python eval/run_eval.py --backend http://localhost:8000
    python eval/run_eval.py --direct --es-url http://localhost:9200
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
GOLD_PATH = HERE / "gold_set.json"


def keyword_match(text: str, keywords: list[str]) -> bool:
    text_low = text.lower()
    return any(kw.lower() in text_low for kw in keywords)


def category_match(category: str, expected: list[str]) -> bool:
    return category in expected if category else False


def score_hits(hits: list[dict], expected_keywords: list[str], expected_categories: list[str]) -> dict:
    if not hits:
        return {"precision": 0.0, "recall": 0.0, "category_hit": False, "n": 0}
    matched = 0
    cat_hit = False
    for h in hits:
        text = " ".join([h.get("title", ""), h.get("chunk_text", ""), h.get("url", "")])
        if keyword_match(text, expected_keywords):
            matched += 1
        if category_match(h.get("category", "") or h.get("doc_type", ""), expected_categories):
            cat_hit = True
    return {
        "precision": matched / len(hits),
        "recall": 1.0 if matched > 0 else 0.0,
        "category_hit": cat_hit,
        "n": len(hits),
    }


def query_backend(backend_url: str, question: str, timeout: float = 30.0) -> list[dict]:
    import httpx
    r = httpx.post(f"{backend_url}/api/chat", json={"question": question}, timeout=timeout)
    r.raise_for_status()
    body = r.json()
    return [{"title": s.get("title", ""), "url": s.get("url", ""), "doc_id": s.get("doc_id", "")} for s in body.get("sources", [])]


def query_es_direct(es_url: str, index: str, question: str, top_k: int = 5) -> list[dict]:
    from elasticsearch import Elasticsearch
    es = Elasticsearch(es_url)
    resp = es.search(
        index=index,
        size=top_k,
        query={
            "bool": {
                "should": [
                    {"match": {"chunk_text": {"query": question, "boost": 5.0}}},
                    {"match": {"title": {"query": question, "boost": 3.0}}},
                ],
            }
        },
    )
    return [h["_source"] for h in resp["hits"]["hits"]]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", default=os.environ.get("BACKEND_URL", "http://localhost:8000"))
    parser.add_argument("--direct", action="store_true", help="Query Elasticsearch directly instead of the backend")
    parser.add_argument("--es-url", default=os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200"))
    parser.add_argument("--es-index", default=os.environ.get("ES_INDEX", "phapluat"))
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--gold", default=str(GOLD_PATH))
    args = parser.parse_args()

    gold = json.loads(Path(args.gold).read_text(encoding="utf-8"))
    queries = gold["queries"]

    results = []
    for q in queries:
        started = time.monotonic()
        try:
            if args.direct:
                hits = query_es_direct(args.es_url, args.es_index, q["question"], top_k=args.top_k)
            else:
                hits = query_backend(args.backend, q["question"])
        except Exception as exc:
            print(f"[{q['id']}] FAIL: {exc}", file=sys.stderr)
            results.append({"id": q["id"], "precision": 0, "recall": 0, "category_hit": False, "n": 0, "latency_ms": 0, "error": str(exc)})
            continue
        latency_ms = int((time.monotonic() - started) * 1000)
        s = score_hits(hits, q.get("expected_keywords", []), q.get("expected_categories", []))
        s.update({"id": q["id"], "latency_ms": latency_ms})
        results.append(s)
        print(f"[{q['id']}] P={s['precision']:.2f} R={s['recall']:.2f} cat={s['category_hit']} n={s['n']} t={latency_ms}ms")

    ok = [r for r in results if "error" not in r and r["n"] > 0]
    if ok:
        mean_p = statistics.mean(r["precision"] for r in ok)
        mean_r = statistics.mean(r["recall"] for r in ok)
        cat_hit_rate = sum(1 for r in ok if r["category_hit"]) / len(ok)
        mean_lat = statistics.mean(r["latency_ms"] for r in ok)
        print("\n=== Summary ===")
        print(f"queries          : {len(results)}")
        print(f"successful       : {len(ok)}")
        print(f"mean precision@k : {mean_p:.3f}")
        print(f"mean recall@k    : {mean_r:.3f}")
        print(f"category hit rate: {cat_hit_rate:.3f}")
        print(f"mean latency     : {mean_lat:.0f} ms")
    else:
        print("\nNo successful queries — backend or ES not reachable?", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
