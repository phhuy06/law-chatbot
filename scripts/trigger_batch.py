#!/usr/bin/env python3
"""Trigger the Lambda batch layer (spark-batch) on-demand and watch progress.

The batch CronJob runs every 6 hours on its own; this script kicks one off
NOW for demos. It creates a Job from the spark-batch CronJob template, then
streams logs until the Job completes.

The batch reads MinIO master/ and bulk-indexes to Elasticsearch. With ES
dedup enabled (default), chunks already in ES are skipped — so if streaming
already populated ES, the batch run will be a fast no-op. Pass --force to
re-embed every doc (warning: full OpenAI bill).

Usage:
    .venv/bin/python scripts/trigger_batch.py
    .venv/bin/python scripts/trigger_batch.py --force
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time


def banner(text: str) -> None:
    print("=" * 64); print(text); print("=" * 64)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--force", action="store_true",
                    help="Set BATCH_FORCE=true so docs already in ES are re-embedded")
    args = ap.parse_args()

    banner("Trigger spark-batch (Lambda batch layer)")

    job_name = f"spark-batch-manual-{int(time.time())}"
    print(f"\n[1] Creating Job '{job_name}' from CronJob/spark-batch...")
    subprocess.run(
        ["kubectl", "-n", "law-chatbot", "create", "job",
         "--from=cronjob/spark-batch", job_name],
        check=True,
    )

    if args.force:
        print(f"\n[1b] Patching Job to add BATCH_FORCE=true...")
        subprocess.run([
            "kubectl", "-n", "law-chatbot", "patch", "job", job_name,
            "--type=json",
            "-p", '[{"op":"add","path":"/spec/template/spec/containers/0/env/-","value":{"name":"BATCH_FORCE","value":"true"}}]',
        ], check=True)

    print(f"\n[2] Waiting for pod to start...")
    for _ in range(30):
        result = subprocess.run(
            ["kubectl", "-n", "law-chatbot", "get", "pods",
             "-l", f"job-name={job_name}", "-o", "jsonpath={.items[0].status.phase}"],
            capture_output=True, text=True,
        )
        phase = result.stdout.strip()
        if phase in ("Running", "Succeeded"):
            print(f"    pod is {phase}")
            break
        time.sleep(2)
    else:
        print(f"    pod never reached Running state — check `kubectl get pods`")
        return 1

    print(f"\n[3] Streaming pod logs (Ctrl-C to detach; Job keeps running):")
    print("-" * 64)
    proc = subprocess.run(
        ["kubectl", "-n", "law-chatbot", "logs", "-f", f"job/{job_name}"],
        check=False,
    )
    print("-" * 64)

    print(f"\n[4] Final Job status:")
    subprocess.run(
        ["kubectl", "-n", "law-chatbot", "get", "job", job_name],
        check=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
