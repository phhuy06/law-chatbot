# patchright = playwright drop-in with stealth patches deep enough to clear
# Cloudflare's JS challenge on thuvienphapluat. Needs real Chrome installed and
# headed mode (classic/new headless both get flagged).
from patchright.sync_api import sync_playwright
import re
import csv
from datetime import datetime, timezone
import time
import random
import argparse
import json
import sys

import os
from bs4 import BeautifulSoup
from typing import Dict, List, Optional

# Persistent browser profile — keeps the cf_clearance cookie across runs so
# subsequent invocations avoid re-solving the challenge.
PROFILE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "law-chatbot-crawler-profile")


def _clear_cf_challenge(page, max_wait_s: int = 30) -> bool:
    """Wait for Cloudflare 'Chờ một chút...' / 'Just a moment...' to resolve."""
    for _ in range(max_wait_s):
        t = page.title() or ""
        if "Chờ" not in t and "Just a moment" not in t:
            return True
        time.sleep(1)
    return False

try:
    from elasticsearch import Elasticsearch
    from confluent_kafka import Producer
    from minio import Minio
    HAS_REALTIME = True
except ImportError:
    HAS_REALTIME = False

BASEDIR = os.path.dirname(__file__)
DEFAULT_CATEGORY = "tai-nguyen-moi-truong"
BASE_URL = "https://thuvienphapluat.vn/hoi-dap-phap-luat"
START_URL = f"{BASE_URL}/{DEFAULT_CATEGORY}"  # referenced only by the standalone collect_links() helper
START_PAGE = 1
END_PAGE = 5

KAFKA_TOPIC = "van-ban-phap-luat"
MINIO_BUCKET = "phapluat"
ES_INDEX = "phapluat"


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


def _has_classes(tag, *classes):
    if not tag:
        return False
    cls = tag.get('class') or []
    return all(c in cls for c in classes)


def parse_article(html: str) -> Dict[str, object]:
    """Parse an article HTML string and return {'question': str, 'answer': List[str]}.

    This follows the path described: tvpl-main -> first div.row -> div.col-md-9 ps-md-0 ->
    article -> div.row -> div.col-md-9 ct-main pe-md-0. Then extracts header>h1 as question
    and all <p> / <blockquote> inside section.news-content#news-content as answer paragraphs.
    Nested tags inside p/blockquote are flattened to text.
    """
    soup = BeautifulSoup(html, "html.parser")

    # root container
    root = soup.find(lambda t: t.name == 'div' and (t.get('class') and 'tvpl-main' in t.get('class')))
    if not root:
        root = soup.find('div', class_='tvpl-main')
    if not root:
        return {"question": "", "answer": []}

    # first row inside root
    row1 = root.find('div', class_='row')
    if not row1:
        return {"question": "", "answer": []}

    # target column: prefer div with both classes
    col_main = row1.find(lambda t: t.name == 'div' and _has_classes(t, 'col-md-9', 'ps-md-0'))
    if not col_main:
        col_main = row1.find(lambda t: t.name == 'div' and ('col-md-9' in (t.get('class') or [])))
    if not col_main:
        return {"question": "", "answer": []}

    article = col_main.find('article') or col_main
    inner_row = article.find('div', class_='row')
    if not inner_row:
        return {"question": "", "answer": []}

    target_col = inner_row.find(lambda t: t.name == 'div' and _has_classes(t, 'col-md-9', 'ct-main', 'pe-md-0'))
    if not target_col:
        target_col = inner_row.find(lambda t: t.name == 'div' and ('col-md-9' in (t.get('class') or [])))

    # question from header > h1
    question = ""
    if target_col:
        header = target_col.find('header')
        if header:
            h1 = header.find('h1')
            if h1:
                question = " ".join([ln.strip() for ln in h1.get_text(separator=" ").splitlines() if ln.strip()])

    # answer: collect p and blockquote inside section.news-content#news-content
    answer_list: List[str] = []
    if target_col:
        section = target_col.find('section', id='news-content') or target_col.find('section', class_='news-content')
        if section:
            elems = section.find_all(['p', 'blockquote'])
            for el in elems:
                # skip empty
                txt = " ".join([ln.strip() for ln in el.get_text(separator=" ").splitlines() if ln.strip()])
                if txt:
                    answer_list.append(txt)

    return {"question": question, "answer": answer_list}


def extract_from_detail(page, url, category_slug: str = ""):
    # Wait for the main detail container
    try:
        page.wait_for_selector("div.tvpl-main.container.pt-3.pb-3.wap-page-detail", timeout=5000)
    except Exception:
        pass

    # Locate detail container and prefer article inside it
    detail = page.query_selector("div.tvpl-main.container.pt-3.pb-3.wap-page-detail") or page.query_selector("div.wap-page-detail") or page

    # Remove non-article top-level divs inside the detail container to avoid breadcrumbs/ads
    try:
        page.eval_on_selector("div.tvpl-main.container.pt-3.pb-3.wap-page-detail", "(container) => { Array.from(container.querySelectorAll(':scope > div')).forEach(d=>{ if(!d.querySelector('article')) d.remove(); }); }")
    except Exception:
        # ignore if selector not present
        pass

    # prefer article element for the answer/content
    article = detail.query_selector("article") or detail

    # id from URL
    m = re.search(r"-(\d+)\.html$", url)
    doc_id = m.group(1) if m else ""

    # title: first h1.h3.fw-bold.title inside header (within article if present)
    title = ""
    try:
        header = article.query_selector("header") or article
        t = header.query_selector("h1.h3.fw-bold.title")
        if t:
            title = normalize_text(t.inner_text())
    except Exception:
        title = ""

    # Try parsing the raw HTML with BeautifulSoup (more robust); do this before any
    # destructive JS evaluation that removes inner divs. We will use the parsed
    # result as a fallback if Playwright-based extraction yields no answer.
    parsed = {"question": "", "answer": []}
    try:
        html = page.content()
        parsed = parse_article(html)
    except Exception:
        parsed = {"question": "", "answer": []}

    # Remove inner divs inside the big detail/article to avoid sidebar/menu text
    try:
        article.evaluate("node => { Array.from(node.querySelectorAll('div')).forEach(d=>d.remove()); Array.from(node.querySelectorAll('script, style, .ads, .sidebar, .related-questions, .social-share')).forEach(e=>e.remove()); }")
    except Exception:
        pass

    # answer: ONLY gather <p> and <blockquote> inside section.news-content#news-content (skip <p> that contain <img>)
    answer = ""
    try:
        section = article.query_selector('section.news-content#news-content') or article.query_selector('section#news-content')
        parts = []
        if section:
            # 1) collect h2 headings (if any)
            try:
                h2s = section.query_selector_all('h2')
                for h in h2s:
                    t = normalize_text(h.inner_text())
                    if t:
                        parts.append(t)
            except Exception:
                pass
            # 2) collect p tags (skip those that contain images)
            try:
                ps = section.query_selector_all('p')
                for p in ps:
                    try:
                        has_img = p.eval_on_selector('img', 'el => el !== null')
                    except Exception:
                        has_img = False
                    if has_img:
                        continue
                    t = normalize_text(p.inner_text())
                    if t:
                        parts.append(t)
            except Exception:
                pass
            # 3) collect blockquotes
            try:
                bq = section.query_selector_all('blockquote')
                for b in bq:
                    t = normalize_text(b.inner_text())
                    if t:
                        parts.append(t)
            except Exception:
                pass
            if parts:
                answer = "\n\n".join(parts)
            else:
                answer = ""
        else:
            # No section found: do not take fallback from other parts — keep answer empty
            answer = ""
    except Exception:
        answer = ""

    # If Playwright extraction failed to find answer, fall back to BeautifulSoup parse
    try:
        if (not answer or answer.strip() == "") and parsed and parsed.get('answer'):
            answer = "\n\n".join(parsed.get('answer'))
        # also prefer parsed question/title when Playwright didn't find a title
        if (not title or title.strip() == "") and parsed and parsed.get('question'):
            title = parsed.get('question')
    except Exception:
        pass

    # published date: from breadcrumb container span.news-time. The site renders
    # it as "HH:MM | DD/MM/YYYY", so preserve the time — it is the only way to
    # order two articles published on the same day.
    published_date = ""
    try:
        pub_el = page.query_selector("div.d-flex.justify-content-between.align-items-baseline.tvpl-breadcrumb-container span.news-time") or page.query_selector("span.news-time")
        pub = normalize_text(pub_el.inner_text()) if pub_el else ""
        if pub:
            for fmt in ("%H:%M | %d/%m/%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
                try:
                    dt = datetime.strptime(pub.strip(), fmt)
                    # Always emit ISO 8601 so lexicographic sort == chronological
                    # (date-only rows get a 00:00:00 suffix, sort before same-day timed ones)
                    published_date = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    break
                except Exception:
                    continue
    except Exception:
        published_date = ""

    # Category = slug passed from caller (breadcrumb selector missed > 99%).
    cat = category_slug

    # author (fallback)
    author = ""
    try:
        for sel in [".author", ".post-author", ".author-name", ".post-meta .author"]:
            el = article.query_selector(sel) or detail.query_selector(sel)
            if el:
                author = normalize_text(el.inner_text())
                break
    except Exception:
        author = ""

    # legal refs and tags (fallback)
    legal_refs = []
    try:
        for sel in [".legal-ref", ".ref-list", ".refs", ".post-meta .refs"]:
            els = article.query_selector_all(sel) or detail.query_selector_all(sel)
            for e in els:
                text = normalize_text(e.inner_text())
                if text:
                    legal_refs.append(text)
    except Exception:
        legal_refs = []

    tags = []
    try:
        for sel in [".tags a", ".tag-list a", ".post-tags a"]:
            els = article.query_selector_all(sel) or detail.query_selector_all(sel)
            for e in els:
                t = normalize_text(e.inner_text())
                if t:
                    tags.append(t)
    except Exception:
        tags = []

    # views (fallback)
    views = ""
    try:
        el = article.query_selector('.views') or detail.query_selector('.views')
        if el:
            v = re.search(r"(\d[\d,]*)", el.inner_text())
            if v:
                views = v.group(1).replace(',', '')
    except Exception:
        views = ""

    return {
        "id": doc_id,
        "question": title,
        "answer": answer,
        "category": cat,
        "author": author,
        "published_date": published_date,
        "legal_refs": ";".join(legal_refs),
        "tags": ";".join(tags),
        "views": int(views) if isinstance(views, str) and views.isdigit() else 0,
        "url": url,
    }


def is_article_exists(es: 'Elasticsearch', article_id: str) -> bool:
    """Check if article already exists in Elasticsearch."""
    if not article_id:
        return False
    try:
        resp = es.search(
            index=ES_INDEX,
            query={"term": {"doc_id": article_id}},
            size=1,
            _source=False,
        )
        return resp["hits"]["total"]["value"] > 0
    except Exception:
        return False


def create_kafka_producer(servers: str) -> Optional['Producer']:
    """Create Kafka producer."""
    try:
        return Producer({"bootstrap.servers": servers})
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        return None


def to_kafka_doc(item: dict) -> dict:
    """Convert crawler item to Kafka document schema."""
    return {
        "id": item.get("id", ""),
        "title": item.get("question", ""),
        "content": item.get("answer", ""),
        "category": item.get("category", ""),
        "doc_type": "",
        "doc_number": "",
        "agency": item.get("author", ""),
        "published_date": item.get("published_date", ""),
        "url": item.get("url", ""),
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }


def publish_to_kafka(producer: 'Producer', item: dict, topic: str = KAFKA_TOPIC):
    """Publish document to Kafka."""
    try:
        doc = to_kafka_doc(item)
        producer.produce(
            topic=topic,
            value=json.dumps(doc, ensure_ascii=False).encode("utf-8"),
            key=doc["id"].encode("utf-8"),
        )
        producer.flush()
        return True
    except Exception as e:
        print(f"Failed to publish to Kafka: {e}")
        return False


def upload_bytes_to_minio(data: bytes, category: str, endpoint: str, access_key: str, secret_key: str):
    """Upload in-memory CSV bytes to MinIO at csv/backup/{category}/{timestamp}.csv."""
    try:
        from io import BytesIO
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        remote_path = f"csv/backup/{category}/{ts}.csv"
        client.put_object(
            MINIO_BUCKET,
            remote_path,
            BytesIO(data),
            length=len(data),
            content_type="text/csv",
        )
        print(f"Uploaded to MinIO: {remote_path}")
        return True
    except Exception as e:
        print(f"Failed to upload to MinIO: {e}")
        return False


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


def discover_categories(hub_url=BASE_URL, retries=3, timeout=20000) -> List[str]:
    """Fetch the hub page and return category slugs found in its navigation.

    A category link is any anchor whose href points to a single path segment under
    /hoi-dap-phap-luat/ (i.e. not an article page, which ends in .html)."""
    slugs: List[str] = []
    seen: set = set()
    os.makedirs(PROFILE_DIR, exist_ok=True)
    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            PROFILE_DIR, channel="chrome", headless=False, no_viewport=True,
        )
        page = context.pages[0] if context.pages else context.new_page()
        if not safe_goto(page, hub_url, retries=retries, timeout=timeout):
            print(f"Failed to load hub page {hub_url}")
            context.close()
            return []
        _clear_cf_challenge(page)
        try:
            page.wait_for_selector("a[href*='/hoi-dap-phap-luat/']", timeout=5000)
        except Exception:
            pass

        for a in page.query_selector_all("a[href*='/hoi-dap-phap-luat/']"):
            href = a.get_attribute("href") or ""
            if href.startswith("/"):
                href = "https://thuvienphapluat.vn" + href
            clean = href.split("?")[0].split("#")[0].rstrip("/")
            m = re.match(r"^https?://[^/]+/hoi-dap-phap-luat/([^/]+)$", clean)
            if not m:
                continue
            slug = m.group(1)
            if not slug or slug.endswith(".html") or slug in seen:
                continue
            seen.add(slug)
            slugs.append(slug)
        context.close()
    return sorted(slugs)


def collect_links(start_page=START_PAGE, end_page=END_PAGE):
    """Collect article links from START_URL?page=start_page..end_page and print counts."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        page = browser.new_page()
        all_links = []
        for p in range(start_page, end_page + 1):
            list_url = f"{START_URL}?page={p}"
            try:
                page.goto(list_url, wait_until="domcontentloaded", timeout=20000)
            except Exception as e:
                print(f"Failed to open {list_url}: {e}")
                continue

            try:
                page.wait_for_selector("div.tvpl-main.container.pt-3.pb-3", timeout=5000)
            except Exception:
                pass

            links = []
            try:
                container = page.query_selector("div.tvpl-main.container.pt-3.pb-3") or page
                row = container.query_selector("div.row") or container
                col = row.query_selector("div.col-md-9") or row
                section = col.query_selector("section") or col
                articles = section.query_selector_all("article.news-card") if section else []
                if not articles:
                    articles = page.query_selector_all("article.news-card")
                for a in articles:
                    anchor = a.query_selector("a")
                    if anchor:
                        href = anchor.get_attribute("href")
                        if href and href.startswith("/"):
                            href = "https://thuvienphapluat.vn" + href
                        if href and href.startswith("http"):
                            links.append(href)
            except Exception as e:
                print(f"Error parsing list page {list_url}: {e}")
                for a in page.query_selector_all("a"):
                    href = a.get_attribute("href")
                    if href and "/hoi-dap-phap-luat/" in href and href.endswith('.html'):
                        if href.startswith('/'):
                            href = 'https://thuvienphapluat.vn' + href
                        links.append(href)

            # dedupe per page
            seen = set()
            deduped = []
            for l in links:
                if l not in seen:
                    seen.add(l)
                    deduped.append(l)

            print(f"Page {p}: found {len(deduped)} links")
            all_links.extend(deduped)
            time.sleep(random.uniform(0.5, 1.0))

        unique = list(dict.fromkeys(all_links))
        print(f"Total unique links collected: {len(unique)}")
        for i, u in enumerate(unique, 1):
            print(f"{i}: {u}")

        browser.close()


def main(start_url, output_path,
         start_page=START_PAGE, end_page=END_PAGE, per_page_limit=0, delay_min=1.0, delay_max=2.0, retries=3,
         realtime=False, es_url="http://localhost:9200", kafka_servers="localhost:29092",
         minio_endpoint="localhost:9000", minio_access="minioadmin", minio_secret="minioadmin",
         stop_on_seen=False):

    # Initialize realtime components
    es = None
    producer = None
    if realtime:
        if not HAS_REALTIME:
            print("ERROR: Realtime mode requires: pip install elasticsearch confluent-kafka minio")
            return 0
        try:
            es = Elasticsearch(es_url)
            print(f"Connected to Elasticsearch: {es_url}")
        except Exception as e:
            print(f"Failed to connect to Elasticsearch: {e}")
            return 0

        producer = create_kafka_producer(kafka_servers)
        if not producer:
            print("Failed to create Kafka producer")
            return 0
        print(f"Connected to Kafka: {kafka_servers}")

    new_rows_count = 0
    os.makedirs(PROFILE_DIR, exist_ok=True)
    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            PROFILE_DIR, channel="chrome", headless=False, no_viewport=True,
        )
        page = context.pages[0] if context.pages else context.new_page()
        # warm up: hit the homepage, let CF clearance cookie land before we
        # navigate to the protected listing pages
        try:
            page.goto("https://thuvienphapluat.vn/", wait_until="domcontentloaded", timeout=20000)
            _clear_cf_challenge(page)
            time.sleep(random.uniform(1.5, 3.0))
        except Exception:
            pass

        all_links = []
        for p in range(start_page, end_page + 1):
            list_url = f"{start_url}?page={p}"
            print(f"Fetching list page: {list_url}")
            try:
                ok = safe_goto(page, list_url, retries=retries, timeout=20000)
                if not ok:
                    continue
            except Exception as e:
                print(f"Failed to load list page {list_url}: {e}")
                continue

            # clear any Cloudflare challenge, then wait for list container
            _clear_cf_challenge(page)
            try:
                page.wait_for_selector("div.tvpl-main.container.pt-3.pb-3", timeout=5000)
            except Exception:
                pass

            # traverse path: body -> div.tvpl-main.container.pt-3.pb-3 -> div.row -> div.col-md-9 -> section -> article.news-card
            links = []
            try:
                container = page.query_selector("div.tvpl-main.container.pt-3.pb-3") or page
                row = container.query_selector("div.row") or container
                col = row.query_selector("div.col-md-9") or row
                section = col.query_selector("section") or col
                articles = section.query_selector_all("article.news-card") if section else []
                if not articles:
                    articles = page.query_selector_all("article.news-card")
                for a in articles:
                    anchor = a.query_selector("a")
                    if anchor:
                        href = anchor.get_attribute("href")
                        if href and href.startswith("/"):
                            href = "https://thuvienphapluat.vn" + href
                        if href and href.startswith("http"):
                            links.append(href)
            except Exception:
                # fallback: scan page for links matching pattern
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

        # ES is the ONLY source of truth for dedup (realtime mode). Local CSV
        # is just an artifact for teammates to commit; we don't read it back.
        HEADER = ["id","title","content","category","doc_type","doc_number","agency","published_date","url","crawled_at"]

        import io
        buf = None
        local_fh = None
        if realtime:
            # Realtime: buffer rows in memory; upload to MinIO at end.
            # Skip local file entirely — crawler/output/ is only for batch/teammate mode.
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=",", quoting=csv.QUOTE_ALL)
            writer.writerow(HEADER)
        else:
            # Batch/teammate mode: append to local CSV so teammates can commit to git.
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            write_header = not os.path.exists(output_path) or os.path.getsize(output_path) == 0
            local_fh = open(output_path, "a", encoding="utf-8", newline='')
            writer = csv.writer(local_fh, delimiter=",", quoting=csv.QUOTE_ALL)
            if write_header:
                writer.writerow(HEADER)

        try:
            to_process = links_to_process
            print(f"Links to process: {len(to_process)}")

            for idx, link in enumerate(to_process, 1):
                print(f"Processing {idx}/{len(to_process)}: {link}")
                
                # Extract article ID for ES dedup check
                article_id = ""
                if realtime:
                    m = re.search(r"-(\d+)\.html$", link)
                    article_id = m.group(1) if m else ""
                    
                    # Check if exists in ES
                    if article_id and is_article_exists(es, article_id):
                        if stop_on_seen:
                            print(f"  [stop] {article_id} already exists in ES — listings are newest-first, stopping this category")
                            break
                        print(f"  [skip] {article_id} already exists in ES")
                        continue
                
                try:
                    ok = safe_goto(page, link, retries=retries, timeout=20000)
                    if not ok:
                        print(f"Skipping {link} due to repeated navigation failures")
                        continue
                    _clear_cf_challenge(page)
                    
                    item = extract_from_detail(page, link, category_slug=os.path.splitext(os.path.basename(output_path))[0])
                    
                    if not item.get("id"):
                        print(f"  [skip] No ID extracted from {link}")
                        continue
                    
                    # Write to CSV (10-col Kafka-style schema: matches to_kafka_doc() and
                    # what spark.batch.pipeline / spark.streaming.consumer expect)
                    writer.writerow([
                        item.get('id', ''),
                        item.get('question', ''),      # → title
                        item.get('answer', ''),        # → content
                        item.get('category', ''),
                        '',                             # doc_type (not extracted by this crawler)
                        '',                             # doc_number (not extracted)
                        item.get('author', ''),        # → agency
                        item.get('published_date', ''),
                        item.get('url', ''),
                        datetime.now(timezone.utc).isoformat(),
                    ])
                    new_rows_count += 1

                    # Publish to Kafka (realtime mode)
                    if realtime and producer:
                        if publish_to_kafka(producer, item):
                            print(f"  [kafka] {item['id']}")
                        else:
                            print(f"  [kafka-fail] {item['id']}")
                    
                except Exception as e:
                    print(f"Error fetching {link}: {e}")

                # polite delay between articles
                time.sleep(random.uniform(delay_min, delay_max))
        finally:
            if local_fh is not None:
                local_fh.close()

        # Realtime: upload buffered CSV to MinIO, grouped by category
        if realtime and new_rows_count > 0 and buf is not None:
            category = os.path.splitext(os.path.basename(output_path))[0]
            upload_bytes_to_minio(
                buf.getvalue().encode("utf-8"),
                category,
                minio_endpoint,
                minio_access,
                minio_secret,
            )
        elif realtime:
            print("No new rows — skipping MinIO upload")

        context.close()
    return new_rows_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Playwright scraper for thuvienphapluat')
    parser.add_argument('--category', default=DEFAULT_CATEGORY,
                        help='category slug; crawls {BASE_URL}/{slug} into output/{slug}.csv')
    parser.add_argument('--list-categories', action='store_true',
                        help='Discover category slugs from the hub page, print one per line, and exit')
    parser.add_argument('--start', type=int, default=START_PAGE, help='start page')
    parser.add_argument('--end', type=int, default=END_PAGE, help='end page')
    parser.add_argument('--limit', type=int, default=0, help='limit total articles per run (0 = all)')
    parser.add_argument('--delay-min', type=float, default=1.0, help='minimum per-request delay')
    parser.add_argument('--delay-max', type=float, default=2.0, help='maximum per-request delay')
    parser.add_argument('--retries', type=int, default=3, help='navigation retry attempts')
    parser.add_argument('--realtime', action='store_true', help='Check ES dedup + publish Kafka + upload MinIO')
    parser.add_argument('--stop-on-seen', action='store_true',
                        help='Stop this category as soon as an article is found already in ES (listings are newest-first, so everything after is also already seen)')
    parser.add_argument('--es-url', default='http://localhost:9200', help='Elasticsearch URL')
    parser.add_argument('--kafka-servers', default='localhost:29092', help='Kafka bootstrap servers')
    parser.add_argument('--minio-endpoint', default='localhost:9000', help='MinIO endpoint')
    parser.add_argument('--minio-access', default='minioadmin', help='MinIO access key')
    parser.add_argument('--minio-secret', default='minioadmin', help='MinIO secret key')
    args = parser.parse_args()

    if args.list_categories:
        cats = discover_categories()
        if not cats:
            sys.exit(1)
        for slug in cats:
            print(slug)
        sys.exit(0)

    start_url = f"{BASE_URL}/{args.category}"
    output_path = os.path.join(BASEDIR, "output", f"{args.category}.csv")

    main(
        start_page=args.start,
        end_page=args.end,
        per_page_limit=args.limit,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        retries=args.retries,
        realtime=args.realtime,
        es_url=args.es_url,
        kafka_servers=args.kafka_servers,
        minio_endpoint=args.minio_endpoint,
        minio_access=args.minio_access,
        minio_secret=args.minio_secret,
        start_url=start_url,
        output_path=output_path,
        stop_on_seen=args.stop_on_seen,
    )
