"""Bulk crawl every category on thuvienphapluat.vn into crawler/output/{slug}.csv.

Chế độ batch (không --realtime) — chỉ ghi CSV cho teammate commit git, không đụng
Kafka/ES/MinIO. CSV được upsert theo `id` (overwrite row cùng id) và cuối run sẽ
sắp xếp lại theo `published_date` giảm dần (bài mới nhất ở đầu file).

Usage:
    python crawler/run_crawl_all.py                       # pages 1-5, mọi category
    python crawler/run_crawl_all.py --start 1 --end 20    # rộng hơn
    python crawler/run_crawl_all.py --categories chung-khoan doanh-nghiep
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

BASEDIR = Path(__file__).parent
OUTPUT_DIR = BASEDIR / "output"
SCRAPE = BASEDIR / "playwright_scrape.py"
PY = sys.executable


def discover_categories() -> list[str]:
    """Call playwright_scrape.py --list-categories and return the slug list."""
    result = subprocess.run(
        [PY, str(SCRAPE), "--list-categories"],
        capture_output=True, text=True, check=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def crawl_one(category: str, start: int, end: int, limit: int) -> int:
    stamp = datetime.now().isoformat(timespec="seconds")
    print(f"=== {stamp}  crawling {category}  pages {start}-{end} ===")
    cmd = [
        PY, str(SCRAPE),
        "--category", category,
        "--start", str(start),
        "--end", str(end),
    ]
    if limit:
        cmd += ["--limit", str(limit)]
    return subprocess.call(cmd)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=int, default=1, help="start page (default 1)")
    ap.add_argument("--end", type=int, default=5, help="end page (default 5)")
    ap.add_argument("--limit", type=int, default=0,
                    help="max articles per category (0 = no limit)")
    ap.add_argument("--categories", nargs="+", default=None,
                    help="explicit slugs; default = auto-discover from hub page")
    args = ap.parse_args()

    cats = args.categories
    if not cats:
        print("Discovering categories from hub page…")
        cats = discover_categories()

    if not cats:
        print("ERROR: no categories found", file=sys.stderr)
        return 1

    print(f"Will crawl {len(cats)} category/ies: {', '.join(cats)}")
    print(f"Pages {args.start}-{args.end}" + (f"  limit={args.limit}" if args.limit else "") + "\n")

    fails: list[str] = []
    for c in cats:
        rc = crawl_one(c, args.start, args.end, limit=args.limit)
        if rc != 0:
            fails.append(c)
            print(f"  FAILED: {c} (rc={rc})")

    total = len(cats)
    ok = total - len(fails)
    print(f"\nDone. {ok}/{total} succeeded.")
    if fails:
        print(f"Failed: {', '.join(fails)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
