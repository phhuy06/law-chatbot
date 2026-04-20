#!/usr/bin/env bash
# Manual batch pipeline run.
#
# Reads CSVs under crawler/output/, cleans + chunks + embeds via OpenAI,
# and bulk-writes into Elasticsearch. Safe to re-run — chunk IDs are
# deterministic (doc_id + md5 of chunk text), so repeat runs upsert.
#
# Usage:
#   ./run_batch.sh                       # process crawler/output/*.csv
#   ./run_batch.sh doanh-nghiep.csv      # process a single file
#   ./run_batch.sh 'tai-*.csv'           # glob (quote it so the host shell does not expand)
#
# Requires: Docker running, .env populated with OPENAI_API_KEY, and the
# spark-job image built (docker-compose build spark-job) at least once.

set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ $# -eq 0 ]; then
  DATA_PATH="/app/crawler/output/*.csv"
else
  DATA_PATH="/app/crawler/output/$1"
fi

cd "$PROJECT_ROOT"

echo "=== $(date -Iseconds) batch run: $DATA_PATH ==="
docker-compose run --rm --no-deps \
  -v "$PROJECT_ROOT/crawler/output:/app/crawler/output:ro" \
  -v "$PROJECT_ROOT/spark:/app/spark:ro" \
  -e BATCH_DATA_PATH="$DATA_PATH" \
  spark-job python3 -m spark.batch.pipeline
echo "=== $(date -Iseconds) done ==="
