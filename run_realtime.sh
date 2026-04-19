#!/usr/bin/env bash
# Manual realtime crawl (same infrastructure as the hourly cron, but for
# ad-hoc invocation).
#
# Usage:
#   ./run_realtime.sh                       # sweep all categories like the cron does
#   ./run_realtime.sh <category>            # one category, pages 1-5, stop-on-seen
#   ./run_realtime.sh <category> <start> <end>
#
# --stop-on-seen is always on: as soon as the crawler hits an article already
# indexed in ES, the category loop exits. Listings are newest-first, so
# everything after the first seen article is already done. For a deep
# backfill that scans past already-indexed articles, invoke the python
# script directly without the flag:
#   .venv/bin/python crawler/playwright_scrape.py --category X --start 1 --end 20 --realtime
#
# Examples:
#   ./run_realtime.sh                       # full hourly-style run, right now
#   ./run_realtime.sh doanh-nghiep          # incremental catch-up of doanh-nghiep
#   ./run_realtime.sh bat-dong-san 1 20     # same as above, just wider page range

set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# No args: reuse the hourly sweep (discovery + all categories + stop-on-seen).
if [ $# -eq 0 ]; then
  exec "$PROJECT_ROOT/crawler/run_hourly.sh"
fi

CAT="$1"
START="${2:-1}"
END="${3:-5}"

cd "$PROJECT_ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
cd "$PROJECT_ROOT/crawler"

echo "=== $(date -Iseconds) manual realtime: $CAT pages $START-$END ==="
python playwright_scrape.py \
    --category "$CAT" \
    --start "$START" --end "$END" \
    --realtime \
    --stop-on-seen
echo "=== $(date -Iseconds) done ==="
