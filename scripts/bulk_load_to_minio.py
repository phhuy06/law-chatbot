#!/usr/bin/env python3
"""Wipe the phapluat MinIO bucket and upload every CSV from crawler/output/
into ``raw/<category>.csv``.

Pipeline that takes over from here:
  raw/<category>.csv
   └→ data-ingest pod (polls raw/, sends each row to Kafka, moves CSV to raw/processed/)
       └→ kafka-consumer pod (writes 1 JSON per article to master/YYYY/MM/<id>.json)
           └→ spark-streaming (writes chunks + embeddings to Elasticsearch)
           └→ spark-batch CronJob (re-reads master/ on schedule)

Prereqs:
    - k8s/start.sh has been run; MinIO port-forwarded at localhost:9000
    - crawler/output/*.csv contains the data you want loaded

Usage:
    .venv/bin/python scripts/bulk_load_to_minio.py
    .venv/bin/python scripts/bulk_load_to_minio.py --dry-run
    .venv/bin/python scripts/bulk_load_to_minio.py --skip-wipe
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

from minio import Minio

MINIO_ENDPOINT = "localhost:9000"
BUCKET = "phapluat"
ES = "http://localhost:9200"
ES_INDEX = "phapluat"


def banner(text: str) -> None:
    print("=" * 64)
    print(text)
    print("=" * 64)


def list_csvs(output_dir: Path) -> list[Path]:
    return sorted(p for p in output_dir.glob("*.csv") if p.is_file())


def wipe_bucket(client: Minio, dry_run: bool) -> int:
    """Remove every object under the bucket. Returns count removed."""
    n = 0
    for obj in client.list_objects(BUCKET, recursive=True):
        if dry_run:
            print(f"  would remove: {obj.object_name}")
        else:
            client.remove_object(BUCKET, obj.object_name)
        n += 1
    return n


def wipe_es_index(dry_run: bool) -> int:
    """Delete all docs from the phapluat index. Returns count deleted."""
    if dry_run:
        return -1
    req = Request(
        f"{ES}/{ES_INDEX}/_delete_by_query?refresh=true",
        data=json.dumps({"query": {"match_all": {}}}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    res = json.loads(urlopen(req, timeout=120).read())
    return res.get("deleted", 0)


def flush_redis(dry_run: bool) -> None:
    if dry_run:
        return
    subprocess.run(
        ["kubectl", "-n", "law-chatbot", "exec", "deployment/redis", "--",
         "redis-cli", "FLUSHDB"],
        check=True, capture_output=True,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="list operations without doing them")
    ap.add_argument("--skip-wipe", action="store_true", help="don't wipe, just upload")
    ap.add_argument("--keep-es", action="store_true",
                    help="don't wipe Elasticsearch (default: also wipes ES + Redis cache)")
    args = ap.parse_args()

    banner("Bulk load: crawler/output/*.csv → MinIO phapluat/raw/")

    repo_root = Path(__file__).resolve().parent.parent
    output_dir = repo_root / "crawler" / "output"
    csvs = list_csvs(output_dir)
    print(f"\n[1] Found {len(csvs)} CSV files in {output_dir}")
    if not csvs:
        print("    nothing to upload, aborting")
        return 1
    total_bytes = sum(p.stat().st_size for p in csvs)
    print(f"    total size: {total_bytes / 1_000_000:.1f} MB")

    client = Minio(MINIO_ENDPOINT, access_key="minioadmin", secret_key="minioadmin", secure=False)

    if not args.skip_wipe:
        print(f"\n[2] Wiping MinIO bucket '{BUCKET}'...")
        removed = wipe_bucket(client, args.dry_run)
        print(f"    {'would remove' if args.dry_run else 'removed'} {removed} objects")

        if not args.keep_es:
            print(f"\n[2b] Wiping Elasticsearch index '{ES_INDEX}' + Redis cache...")
            deleted = wipe_es_index(args.dry_run)
            print(f"    ES: {'would delete all' if args.dry_run else f'deleted {deleted} chunks'}")
            flush_redis(args.dry_run)
            print(f"    Redis: {'would flush' if args.dry_run else 'flushed'}")
    else:
        print(f"\n[2] --skip-wipe set, keeping existing data")

    print(f"\n[3] Uploading CSVs to raw/...")
    t0 = time.time()
    for i, p in enumerate(csvs, 1):
        key = f"raw/{p.name}"
        if args.dry_run:
            print(f"  [{i:>2}/{len(csvs)}] would upload {p.name:<35} → {key}  ({p.stat().st_size} B)")
            continue
        client.fput_object(BUCKET, key, str(p), content_type="text/csv")
        print(f"  [{i:>2}/{len(csvs)}] {p.name:<35} → {key}  ({p.stat().st_size} B)")
    print(f"    elapsed: {time.time() - t0:.1f}s")

    if args.dry_run:
        print("\n(dry run) no changes made.")
        return 0

    print(f"\n[4] Bucket state:")
    counts: dict[str, int] = {}
    for obj in client.list_objects(BUCKET, recursive=True):
        prefix = obj.object_name.split("/", 1)[0]
        counts[prefix] = counts.get(prefix, 0) + 1
    for k, v in sorted(counts.items()):
        print(f"    {k}/ : {v} objects")

    print(f"\nNext steps:")
    print(f"  - data-ingest pod will poll raw/ every 10s, push rows to Kafka,")
    print(f"    and move processed CSVs to raw/processed/.")
    print(f"  - spark-streaming pod will embed + index chunks into Elasticsearch")
    print(f"    as they arrive.")
    print(f"  - Watch with:")
    print(f"      kubectl -n law-chatbot logs -f deployment/data-ingest")
    print(f"      kubectl -n law-chatbot logs -f deployment/spark-job")
    return 0


if __name__ == "__main__":
    sys.exit(main())
