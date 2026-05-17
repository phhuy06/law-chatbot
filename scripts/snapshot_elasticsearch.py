"""Register an Elasticsearch snapshot repository on MinIO (S3) and take a snapshot.

Idempotent: re-registering the repo or re-running with the same snapshot name is safe.
"""
import os
import sys
import time
from datetime import datetime, timezone

import httpx
from elasticsearch import Elasticsearch

ES_URL = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
REPO_NAME = os.environ.get("ES_SNAPSHOT_REPO", "phapluat-snapshots")
BUCKET = os.environ.get("ES_SNAPSHOT_BUCKET", "es-snapshots")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")


def wait_for_es(es: Elasticsearch, retries: int = 30, delay: int = 2):
    for i in range(retries):
        try:
            if es.ping():
                return
        except Exception:
            pass
        print(f"Waiting for Elasticsearch... ({i + 1}/{retries})")
        time.sleep(delay)
    print("ERROR: Elasticsearch not available", file=sys.stderr)
    sys.exit(1)


def ensure_bucket():
    # MinIO accepts a PUT on a non-existent bucket to create it (S3 semantics).
    from minio import Minio
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        print(f"Created MinIO bucket '{BUCKET}'")
    else:
        print(f"MinIO bucket '{BUCKET}' already exists")


def register_repo(es: Elasticsearch):
    # ES 8.x reads S3 credentials from the keystore (s3.client.default.*).
    # The keystore is built at ES pod startup by the `load-keystore`
    # initContainer in k8s/infrastructure/elasticsearch.yaml, so we don't
    # pass access_key / secret_key here.
    body = {
        "type": "s3",
        "settings": {
            "bucket": BUCKET,
            "endpoint": MINIO_ENDPOINT,
            "protocol": "http",
            "path_style_access": "true",
        },
    }
    # Use raw HTTP to avoid client-version-specific kwargs.
    url = f"{ES_URL}/_snapshot/{REPO_NAME}"
    r = httpx.put(url, json=body, timeout=30.0)
    if r.status_code >= 300:
        print(f"ERROR registering repo: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    print(f"Snapshot repository '{REPO_NAME}' registered → s3://{BUCKET}")


def take_snapshot(es: Elasticsearch):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    snap_name = f"snap-{ts}"
    url = f"{ES_URL}/_snapshot/{REPO_NAME}/{snap_name}?wait_for_completion=true"
    body = {"indices": "phapluat,phapluat-audit", "ignore_unavailable": True, "include_global_state": False}
    print(f"Taking snapshot '{snap_name}'...")
    r = httpx.put(url, json=body, timeout=600.0)
    if r.status_code >= 300:
        print(f"ERROR taking snapshot: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    print(f"Snapshot '{snap_name}' complete")


def main():
    action = sys.argv[1] if len(sys.argv) > 1 else "all"
    es = Elasticsearch(ES_URL)
    wait_for_es(es)
    if action in ("init", "all"):
        ensure_bucket()
        register_repo(es)
    if action in ("snapshot", "all"):
        take_snapshot(es)


if __name__ == "__main__":
    main()
