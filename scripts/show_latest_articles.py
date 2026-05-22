#!/usr/bin/env python3
"""Show the N latest articles in the master dataset.

Reads every JSON in MinIO ``master/`` and sorts by ``published_date`` (the
source's own publication timestamp) so you can answer "what's the newest
article in our system?" without per-category guesswork.

Falls back to ``crawled_at`` for docs whose ``published_date`` is empty
(some legacy rows from the May-11 batch).

Usage:
    .venv/bin/python scripts/show_latest_articles.py
    .venv/bin/python scripts/show_latest_articles.py --top 20
    .venv/bin/python scripts/show_latest_articles.py --category thua-ke
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from minio import Minio

MINIO_ENDPOINT = "localhost:9000"
BUCKET = "phapluat"
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--top", type=int, default=10, help="how many to show")
    ap.add_argument("--category", default=None, help="filter by category slug")
    args = ap.parse_args()

    client = Minio(MINIO_ENDPOINT, access_key="minioadmin", secret_key="minioadmin", secure=False)

    rows = []
    skipped = 0
    for obj in client.list_objects(BUCKET, prefix="master/", recursive=True):
        if not obj.object_name.endswith(".json"):
            continue
        try:
            d = json.loads(client.get_object(BUCKET, obj.object_name).read())
        except Exception:
            continue
        if args.category and d.get("category") != args.category:
            continue
        # Require a valid ISO date — many legacy May-11 rows have empty
        # published_date and a URL stuffed into crawled_at; we don't want
        # those polluting "latest" results.
        pub = d.get("published_date") or ""
        if not ISO_DATE.match(pub):
            skipped += 1
            continue
        rows.append((pub, d, obj.object_name))

    rows.sort(reverse=True, key=lambda r: r[0])
    total = len(rows)
    print(f"=== Latest {min(args.top, total)} of {total} articles with valid published_date ===")
    if args.category:
        print(f"    filtered to category = {args.category}")
    if skipped:
        print(f"    skipped {skipped} docs with missing/invalid published_date")
    print()
    for i, (key, d, path) in enumerate(rows[:args.top], 1):
        pub = d.get("published_date") or "(no pub date)"
        crawled = d.get("crawled_at", "")[:10] if d.get("crawled_at") else ""
        cat = d.get("category", "")
        title = (d.get("title") or "")[:78]
        print(f"  {i:>2}. {pub:<22}  [{cat}]")
        print(f"       id={d.get('id')}  crawled={crawled}")
        print(f"       {title}")
        print(f"       {d.get('url', '')[:100]}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
