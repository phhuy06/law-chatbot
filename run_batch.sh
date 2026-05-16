#!/usr/bin/env bash
# Manual trigger for the Lambda batch layer.
#
# Default (no args):
#   Re-process the master dataset in MinIO phapluat/raw/{YYYY}/{MM}/*.json,
#   skipping any doc_ids already present in Elasticsearch (cheap mode).
#
# Options:
#   --force           Skip the ES dedup. Re-embed every doc in MinIO raw/.
#                     Use after an embedding model or chunker upgrade.
#                     Costs ~the full OpenAI embedding bill again.
#   --local <glob>    Read local CSV files instead of MinIO. Glob is
#                     relative to crawler/output/, e.g. 'doanh-nghiep.csv'
#                     or '*.csv'.
#
# Examples:
#   ./run_batch.sh                          # MinIO raw/, dedup on (default)
#   ./run_batch.sh --force                  # MinIO raw/, full re-embed
#   ./run_batch.sh --local '*.csv'          # local crawler/output, dedup on
#   ./run_batch.sh --local 'thue.csv' --force
#
# Requires: Docker running, .env populated with OPENAI_API_KEY, and the
# spark-job image built (docker-compose build spark-job) at least once.

set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

FORCE=""
LOCAL_GLOB=""

while [ $# -gt 0 ]; do
  case "$1" in
    --force)
      FORCE="true"
      shift
      ;;
    --local)
      LOCAL_GLOB="${2:?--local requires a glob argument}"
      shift 2
      ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      echo "Run with -h for usage." >&2
      exit 1
      ;;
  esac
done

EXTRA_ENV=()
EXTRA_VOL=()
if [ -n "$FORCE" ]; then
  EXTRA_ENV+=("-e" "BATCH_FORCE=true")
fi
if [ -n "$LOCAL_GLOB" ]; then
  EXTRA_ENV+=("-e" "BATCH_DATA_PATH=/app/crawler/output/$LOCAL_GLOB")
  EXTRA_VOL+=("-v" "$PROJECT_ROOT/crawler/output:/app/crawler/output:ro")
fi

echo "=== $(date -Iseconds) batch run (force=${FORCE:-false}, local=${LOCAL_GLOB:-MinIO raw/}) ==="
docker-compose run --rm --no-deps \
  -v "$PROJECT_ROOT/spark:/app/spark:ro" \
  "${EXTRA_VOL[@]}" \
  "${EXTRA_ENV[@]}" \
  spark-job python3 -m spark.batch.pipeline
echo "=== $(date -Iseconds) done ==="
