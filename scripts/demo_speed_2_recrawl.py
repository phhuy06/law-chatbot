#!/usr/bin/env python3
"""Demo speed layer — Step 2/2: re-crawl the article + verify the pipeline.

Reads /tmp/law-chatbot-demo-state.json (written by step 1), invokes the real
crawler in ``--realtime --url <URL>`` mode, then watches Elasticsearch, MinIO,
and the chatbot to confirm the article round-tripped through the speed layer.

Prereqs (run k8s/start.sh first):
    - ES port-forwarded at localhost:9200
    - Kafka external listener at localhost:29092
    - MinIO at localhost:9000
    - Backend at localhost:8001

Usage:
    .venv/bin/python scripts/demo_speed_2_recrawl.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from urllib.request import Request, urlopen

from minio import Minio

STATE_FILE = "/tmp/law-chatbot-demo-state.json"
ES = "http://localhost:9200"
MINIO_ENDPOINT = "localhost:9000"
BUCKET = "phapluat"
BACKEND = "http://localhost:8001"


def banner(text: str) -> None:
    print("=" * 64)
    print(text)
    print("=" * 64)


def es_post(path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else b""
    headers = {"Content-Type": "application/json"} if body is not None else {}
    req = Request(f"{ES}{path}", data=data, headers=headers, method="POST")
    return json.loads(urlopen(req, timeout=30).read())


def main() -> int:
    banner("Speed-layer demo — Step 2: re-crawl + verify")

    if not os.path.exists(STATE_FILE):
        print(f"\n[!] {STATE_FILE} not found. Run step 1 first.")
        return 1

    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)
    doc_id = state["id"]
    url = state["url"]
    title = state["title"]
    print(f"\n[1] Target (from {STATE_FILE}):")
    print(f"    id:         {doc_id}")
    print(f"    title:      {title[:80]}")
    print(f"    url:        {url}")
    print(f"    deleted at: {state.get('deleted_at', '?')}")

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    venv_python = os.path.join(repo_root, ".venv", "bin", "python")
    if not os.path.exists(venv_python):
        venv_python = sys.executable
    crawler = os.path.join(repo_root, "crawler", "playwright_scrape.py")

    print(f"\n[2] Running crawler in --realtime --url mode...")
    t0 = time.time()
    proc = subprocess.run(
        [
            venv_python, crawler,
            "--url", url,
            "--realtime",
            "--category", state.get("category") or "misc",
            "--es-url", ES,
            "--kafka-servers", "localhost:29092",
            "--minio-endpoint", MINIO_ENDPOINT,
        ],
        capture_output=True, text=True, timeout=180, cwd=repo_root,
    )
    print(f"    --- crawler output ---")
    for line in proc.stdout.strip().splitlines():
        print(f"    | {line}")
    if proc.returncode != 0:
        print(f"    !!! crawler exited {proc.returncode}")
        for line in proc.stderr.strip().splitlines()[-10:]:
            print(f"    err | {line}")
        return proc.returncode
    print(f"    elapsed: {time.time() - t0:.1f}s")

    print(f"\n[3] Waiting for spark-streaming microbatch...")
    for i in range(6):
        time.sleep(5)
        print(f"    ...{(i + 1) * 5}s", flush=True)

    urlopen(Request(f"{ES}/phapluat/_refresh", method="POST"), timeout=30).read()

    print(f"\n[4] Elasticsearch check:")
    count = es_post("/phapluat/_count", {"query": {"term": {"doc_id": doc_id}}}).get("count", 0)
    status = "OK" if count > 0 else "STILL EMPTY"
    print(f"    chunks for doc_id={doc_id}: {count}  ({status})")

    print(f"\n[5] MinIO check:")
    client = Minio(MINIO_ENDPOINT, access_key="minioadmin", secret_key="minioadmin", secure=False)
    found = False
    for obj in client.list_objects(BUCKET, prefix="master/", recursive=True):
        if obj.object_name.endswith(f"/{doc_id}.json"):
            print(f"    {obj.object_name}: {obj.size} bytes  ({'OK' if obj.size > 100 else 'TOO SMALL'})")
            found = True
    if not found:
        print(f"    master/.../{doc_id}.json not found yet")

    print(f"\n[6] Chatbot test:")
    req = Request(
        f"{BACKEND}/api/chat",
        data=json.dumps({"question": title}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        resp = json.loads(urlopen(req, timeout=60).read())
        print(f"    Q: {title}")
        print(f"    A: {resp.get('answer', '')[:140]}...")
        cited = False
        for s in resp.get("sources", []):
            marker = "  ← recrawled doc" if s.get("doc_id") == doc_id else ""
            if marker:
                cited = True
            print(f"      - doc_id={s.get('doc_id', ''):<12} {s.get('title', '')[:60]}{marker}")
        print(f"    citation of recrawled doc: {'YES' if cited else 'no'}")
    except Exception as e:
        print(f"    backend request failed: {e}")

    elapsed = time.time() - t0
    banner(f"Done. Total round-trip: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
