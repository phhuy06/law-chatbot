#!/usr/bin/env bash
# Hourly realtime crawl: discover every category from the hub page, then
# sweep pages 1-3 of each with --stop-on-seen so already-indexed articles
# break the inner loop. MinIO uploads only happen when new rows are added.
#
# Usage:
#   ./run_hourly.sh
#
# Cron:
#   0 * * * * /path/to/law-chatbot/crawler/run_hourly.sh >> /path/to/law-chatbot/crawler/output/cron.log 2>&1

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
cd "$SCRIPT_DIR"

echo "=== $(date -Iseconds) discovering categories ==="
CATEGORIES=()
while IFS= read -r line; do
  [ -n "$line" ] && CATEGORIES+=("$line")
done < <(python playwright_scrape.py --list-categories)

if [ ${#CATEGORIES[@]} -eq 0 ]; then
  echo "ERROR: no categories discovered — aborting hourly run" >&2
  exit 1
fi

echo "=== $(date -Iseconds) found ${#CATEGORIES[@]} categories: ${CATEGORIES[*]} ==="

for cat in "${CATEGORIES[@]}"; do
  echo "=== $(date -Iseconds) crawling $cat ==="
  python playwright_scrape.py \
      --category "$cat" \
      --start 1 --end 3 \
      --realtime \
      --stop-on-seen \
    || echo "FAILED: $cat (continuing)"
done

echo "=== $(date -Iseconds) hourly run done ==="
