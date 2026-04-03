from playwright.sync_api import sync_playwright
import re
import csv
from datetime import datetime

import os

BASEDIR = os.path.dirname(__file__)
OUTPUT = os.path.join(BASEDIR, "output", "test-data.csv")
START_URL = "https://thuvienphapluat.vn/hoi-dap-phap-luat/tien-te-ngan-hang"


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


def main(limit=20):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/114.0.0.0 Safari/537.36"
        ))
        page = context.new_page()
        page.goto(START_URL, wait_until="domcontentloaded")

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

        # dedupe and limit
        seen = set()
        ordered = []
        for l in links:
            if l not in seen:
                seen.add(l)
                ordered.append(l)
        links = ordered[:limit]

        # open CSV
        with open(OUTPUT, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f, delimiter=",", quoting=csv.QUOTE_ALL)
            writer.writerow(["id","question","answer","category","author","published_date","legal_refs","tags","views","url"])
            for idx, link in enumerate(links, 1):
                try:
                    page.goto(link, wait_until="domcontentloaded", timeout=15000)
                    item = extract_from_detail(page, link)
                    writer.writerow([
                        item['id'], item['question'], item['answer'], item['category'], item['author'],
                        item['published_date'], item['legal_refs'], item['tags'], item['views'], item['url']
                    ])
                except Exception as e:
                    print(f"Error fetching {link}: {e}")

        context.close()
        browser.close()


if __name__ == '__main__':
    main(limit=20)
