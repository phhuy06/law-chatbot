#!/usr/bin/env bash
# Manual realtime crawl (same infrastructure as the hourly cron, but for
# ad-hoc invocation).
#
# Usage:
#   ./run_realtime.sh                       # sweep all categories like the cron does
#   ./run_realtime.sh <category>            # one category, pages 1-5, no stop-on-seen
#   ./run_realtime.sh <category> <start> <end>
#
# Examples:
#   ./run_realtime.sh                       # full hourly-style run, right now
#   ./run_realtime.sh doanh-nghiep          # backfill pages 1-5 of doanh-nghiep
#   ./run_realtime.sh bat-dong-san 1 20     # deep backfill pages 1-20 of bat-dong-san

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
    --realtime
echo "=== $(date -Iseconds) done ==="
