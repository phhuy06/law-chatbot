from playwright.sync_api import sync_playwright
import re
import csv
from datetime import datetime
import time
import random
import argparse

import os

BASEDIR = os.path.dirname(__file__)
OUTPUT = os.path.join(BASEDIR, "output", "quyen-dan-su.csv")
START_URL = "https://thuvienphapluat.vn/hoi-dap-phap-luat/quyen-dan-su"
START_PAGE = 1
END_PAGE = 10


def normalize_text(s: str) -> str:
    if not s:
        return ""
    return " ".join([line.strip() for line in s.splitlines() if line.strip()])


def node_text(node):
    if not node:
        return ""
    try:
        # ElementHandle.inner_text() usually takes no args
        return node.inner_text()
    except TypeError:
        try:
            # Page.inner_text requires a selector
            return node.inner_text('body')
        except Exception:
            return ""
    except Exception:
        return ""


def extract_from_detail(page, url):
    # wait for main detail container
    try:
        page.wait_for_selector("div.wap-page-detail", timeout=5000)
    except Exception:
        pass

    # select the detail container
    detail = page.query_selector("div.wap-page-detail") or page.query_selector("div.tvpl-main.container.pt-3.pb-3.wap-page-detail") or page

    # id
    m = re.search(r"-(\d+)\.html$", url)
    doc_id = m.group(1) if m else ""

    # title
    title_el = detail.query_selector("h1")
    title = normalize_text(node_text(title_el)) if title_el else ""

    # answer/content: try common selectors
    answer_selectors = [
        "div.answer",
        "div.post-content",
        "div.content",
        "div.article-content",
        "div.news-detail",
        "div.entry-content",
        "div#toanvancontent",
        "div.content1",
    ]
    answer = ""
    for sel in answer_selectors:
        el = detail.query_selector(sel)
        if el:
            answer = normalize_text(el.inner_text())
            if answer:
                break
    if not answer:
        answer = normalize_text(node_text(detail))

    # category from breadcrumb
    cat = ""
    bc = page.query_selector_all("nav.breadcrumb a")
    if bc:
        try:
            cat = normalize_text(bc[-1].inner_text())
        except Exception:
            cat = ""

    # author
    author = ""
    for sel in [".author", ".post-author", ".author-name", ".post-meta .author"]:
        el = detail.query_selector(sel)
        if el:
            author = normalize_text(el.inner_text())
            break

    # published date
    pub = ""
    for sel in [".date", ".post-date", ".publish-date", ".meta time"]:
        el = detail.query_selector(sel)
        if el:
            try:
                pub = normalize_text(el.get_attribute('datetime') or el.inner_text())
            except Exception:
                pub = normalize_text(el.inner_text())
            break
    # try to parse to YYYY-MM-DD
    published_date = ""
    if pub:
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(pub.strip(), fmt)
                published_date = dt.strftime("%Y-%m-%d")
                break
            except Exception:
                continue

    # legal refs and tags
    legal_refs = []
    for sel in [".legal-ref", ".ref-list", ".refs", ".post-meta .refs"]:
        els = detail.query_selector_all(sel)
        for e in els:
            text = normalize_text(e.inner_text())
            if text:
                legal_refs.append(text)
    # tags
    tags = []
    for sel in [".tags a", ".tag-list a", ".post-tags a"]:
        els = detail.query_selector_all(sel)
        for e in els:
            t = normalize_text(e.inner_text())
            if t:
                tags.append(t)

    # views
    views = ""
    for sel in [".views", ".post-views"]:
        el = detail.query_selector(sel)
        if el:
            v = re.search(r"(\d[\d,]*)", el.inner_text())
            if v:
                views = v.group(1).replace(',', '')
                break

    return {
        "id": doc_id,
        "question": title,
        "answer": answer,
        "category": cat,
        "author": author,
        "published_date": published_date,
        "legal_refs": ";".join(legal_refs),
        "tags": ";".join(tags),
        "views": int(views) if views.isdigit() else 0,
        "url": url,
    }


def safe_goto(page, url, retries=3, timeout=20000):
    """Navigate with retries and exponential backoff. Returns True on success, False on final failure."""
    attempt = 0
    while attempt <= retries:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return True
        except Exception as e:
            attempt += 1
            if attempt > retries:
                print(f"Failed to goto {url} after {retries} retries: {e}")
                return False
            backoff = (2 ** (attempt - 1)) + random.uniform(0.5, 1.5)
            print(f"Goto failed ({attempt}/{retries}) for {url}: {e}; retrying after {backoff:.1f}s")
            time.sleep(backoff)


def main(start_page=START_PAGE, end_page=END_PAGE, per_page_limit=0, delay_min=1.0, delay_max=2.0, retries=3):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/114.0.0.0 Safari/537.36"
        ))
        page = context.new_page()

        all_links = []
        for p in range(start_page, end_page + 1):
            list_url = f"{START_URL}?page={p}"
            print(f"Fetching list page: {list_url}")
            try:
                ok = safe_goto(page, list_url, retries=retries, timeout=20000)
                if not ok:
                    continue
            except Exception as e:
                print(f"Failed to load list page {list_url}: {e}")
                continue

            # wait for list container
            try:
                page.wait_for_selector("div.tvpl-main.container.pt-3.pb-3", timeout=5000)
            except Exception:
                pass

            # collect article links from articles inside container
            links = []
            articles = page.query_selector_all("div.tvpl-main.container.pt-3.pb-3 article")
            if not articles:
                articles = page.query_selector_all("article")
            for a in articles:
                el = a.query_selector("a")
                if el:
                    href = el.get_attribute("href")
                    if href and href.startswith("/"):
                        href = "https://thuvienphapluat.vn" + href
                    if href and href.startswith("http"):
                        links.append(href)
            # fallback: look for article links on page
            if not links:
                for a in page.query_selector_all("a"):
                    href = a.get_attribute("href")
                    if href and "/hoi-dap-phap-luat/" in href and href.endswith('.html'):
                        if href.startswith('/'):
                            href = 'https://thuvienphapluat.vn' + href
                        links.append(href)

            # dedupe per page and add
            seen_local = set()
            for l in links:
                if l not in seen_local:
                    seen_local.add(l)
                    all_links.append(l)

            # polite delay between list pages
            time.sleep(random.uniform(1.0, 2.0))

        # dedupe overall
        seen = set()
        ordered = []
        for l in all_links:
            if l not in seen:
                seen.add(l)
                ordered.append(l)

        if per_page_limit and per_page_limit > 0:
            links_to_process = ordered[:per_page_limit]
        else:
            links_to_process = ordered

        # prepare output CSV: append mode, write header only if file missing or empty
        os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
        existing_urls = set()
        if os.path.exists(OUTPUT):
            try:
                with open(OUTPUT, "r", encoding="utf-8", newline='') as rf:
                    rdr = csv.reader(rf, delimiter=",", quoting=csv.QUOTE_ALL)
                    # skip header
                    first = True
                    for row in rdr:
                        if first:
                            first = False
                            continue
                        if len(row) >= 10:
                            existing_urls.add(row[9])
            except Exception:
                existing_urls = set()

        write_header = not os.path.exists(OUTPUT) or os.path.getsize(OUTPUT) == 0
        with open(OUTPUT, "a", encoding="utf-8", newline='') as f:
            writer = csv.writer(f, delimiter=",", quoting=csv.QUOTE_ALL)
            if write_header:
                writer.writerow(["id","question","answer","category","author","published_date","legal_refs","tags","views","url"])

            to_process = [l for l in links_to_process if l not in existing_urls]
            print(f"New links to process (excluding already-present): {len(to_process)}")

            for idx, link in enumerate(to_process, 1):
                print(f"Processing {idx}/{len(to_process)}: {link}")
                try:
                    ok = safe_goto(page, link, retries=retries, timeout=20000)
                    if not ok:
                        print(f"Skipping {link} due to repeated navigation failures")
                        continue
                    item = extract_from_detail(page, link)
                    writer.writerow([
                        item.get('id', ''), item.get('question', ''), item.get('answer', ''),
                        item.get('category', ''), item.get('author', ''), item.get('published_date', ''),
                        item.get('legal_refs', ''), item.get('tags', ''), item.get('views', 0), item.get('url', '')
                    ])
                except Exception as e:
                    print(f"Error fetching {link}: {e}")

                # polite delay between articles
                time.sleep(random.uniform(delay_min, delay_max))

        context.close()
        browser.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Playwright scraper for thuvienphapluat')
    parser.add_argument('--start', type=int, default=START_PAGE, help='start page')
    parser.add_argument('--end', type=int, default=END_PAGE, help='end page')
    parser.add_argument('--limit', type=int, default=0, help='limit total articles (0 = all)')
    parser.add_argument('--delay-min', type=float, default=1.0, help='minimum per-request delay')
    parser.add_argument('--delay-max', type=float, default=2.0, help='maximum per-request delay')
    parser.add_argument('--retries', type=int, default=3, help='navigation retry attempts')
    args = parser.parse_args()
    main(start_page=args.start, end_page=args.end, per_page_limit=args.limit, delay_min=args.delay_min, delay_max=args.delay_max, retries=args.retries)
