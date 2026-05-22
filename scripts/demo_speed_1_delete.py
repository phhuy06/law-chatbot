#!/usr/bin/env python3
"""Demo speed layer — Step 1/2: pick the first article on the live hub.

Goes to https://thuvienphapluat.vn/hoi-dap-phap-luat, takes the first
``hoi-dap-phap-luat/<slug>-<id>.html`` link in DOM order (= newest by the
site's own ranking), then:

- if that article IS in our Elasticsearch → delete it from ES + MinIO + Redis
  (classic round-trip demo: delete then recrawl)
- if it's NOT in our ES (likely — hub shows brand-new articles we haven't
  crawled yet) → no delete needed; step 2 will just crawl it fresh and the
  speed layer will index it

Either way, the URL is written to /tmp/law-chatbot-demo-state.json so step 2
knows what to crawl.

Prereqs (run k8s/start.sh first):
    - ES port-forwarded at localhost:9200
    - MinIO at localhost:9000
    - kubectl exec rights for the redis pod

Usage:
    .venv/bin/python scripts/demo_speed_1_delete.py
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from urllib.request import Request, urlopen

from minio import Minio
from playwright.sync_api import sync_playwright

STATE_FILE = "/tmp/law-chatbot-demo-state.json"
ES = "http://localhost:9200"
MINIO_ENDPOINT = "localhost:9000"
BUCKET = "phapluat"
HUB_URL = "https://thuvienphapluat.vn/hoi-dap-phap-luat"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
ARTICLE_RE = re.compile(r"/hoi-dap-phap-luat/[^/]+-(\d+)\.html$")


def banner(text: str) -> None:
    print("=" * 64)
    print(text)
    print("=" * 64)


def es_post(path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else b""
    headers = {"Content-Type": "application/json"} if body is not None else {}
    req = Request(f"{ES}{path}", data=data, headers=headers, method="POST")
    return json.loads(urlopen(req, timeout=30).read())


def find_first_hub_article() -> tuple[str, str, str]:
    """Return (doc_id, title, url) for the first article on the live hub.

    We fetch the article page itself so we get the real <h1> title rather than
    the truncated anchor text on the hub.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA, locale="vi-VN")
        page = ctx.new_page()
        page.goto(HUB_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        target = None
        for a in page.query_selector_all("a[href]"):
            href = a.get_attribute("href") or ""
            m = ARTICLE_RE.search(href)
            if not m:
                continue
            if href.startswith("/"):
                href = "https://thuvienphapluat.vn" + href
            target = (m.group(1), href)
            break
        if not target:
            browser.close()
            raise RuntimeError("No article links found on hub")

        doc_id, url = target
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector("h1", timeout=10000)
        h1 = page.query_selector("h1")
        title = (h1.inner_text().strip() if h1 else "") or ""
        browser.close()
    return doc_id, title, url


def es_chunk_count(doc_id: str) -> int:
    return es_post("/phapluat/_count", {"query": {"term": {"doc_id": doc_id}}}).get("count", 0)


def find_minio_path(client: Minio, doc_id: str) -> str | None:
    for obj in client.list_objects(BUCKET, prefix="master/", recursive=True):
        if obj.object_name.endswith(f"/{doc_id}.json"):
            return obj.object_name
    return None


def main() -> int:
    banner("Speed-layer demo — Step 1: pick first hub article")

    print(f"\n[1] Fetching the live hub at {HUB_URL} ...")
    doc_id, title, url = find_first_hub_article()
    print(f"    First article on hub:")
    print(f"      id:    {doc_id}")
    print(f"      title: {title[:90]}")
    print(f"      url:   {url}")

    print(f"\n[2] Checking if we already have this in Elasticsearch...")
    chunks = es_chunk_count(doc_id)
    print(f"    chunks for doc_id={doc_id}: {chunks}")

    client = Minio(MINIO_ENDPOINT, access_key="minioadmin", secret_key="minioadmin", secure=False)
    minio_path = find_minio_path(client, doc_id)
    if minio_path:
        print(f"    MinIO has master/{minio_path.split('master/',1)[1]}")
    else:
        print(f"    MinIO has no master/.../{doc_id}.json")

    deleted_from_es = 0
    if chunks > 0:
        print(f"\n[3] Article IS in our system — deleting for round-trip test...")
        res = es_post("/phapluat/_delete_by_query", {"query": {"term": {"doc_id": doc_id}}})
        deleted_from_es = res.get("deleted", 0)
        print(f"    ES: deleted {deleted_from_es} chunks")
        if minio_path:
            client.remove_object(BUCKET, minio_path)
            print(f"    MinIO: removed {minio_path}")
        subprocess.run(
            ["kubectl", "-n", "law-chatbot", "exec", "deployment/redis", "--",
             "redis-cli", "FLUSHDB"],
            check=True, capture_output=True,
        )
        print(f"    Redis: flushed")
        urlopen(Request(f"{ES}/phapluat/_refresh", method="POST"), timeout=30).read()
        time.sleep(1)
        after = es_chunk_count(doc_id)
        print(f"    Verify: ES chunks now = {after}  ({'OK' if after == 0 else 'STILL THERE'})")
    else:
        print(f"\n[3] Article is NOT in our system yet (brand-new on the site).")
        print(f"    No delete needed — step 2 will crawl it fresh and the")
        print(f"    speed layer will demonstrate ingest of a never-seen article.")

    state = {
        "id": doc_id,
        "title": title,
        "url": url,
        "minio_path": minio_path or "",
        "was_in_es": chunks > 0,
        "deleted_chunks": deleted_from_es,
        "step1_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    print(f"\n[4] State written to {STATE_FILE}")
    print(f"\nNext: run  .venv/bin/python scripts/demo_speed_2_recrawl.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
