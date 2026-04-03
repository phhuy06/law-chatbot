import re
from datetime import datetime
import scrapy


class HoiDapSpider(scrapy.Spider):
    name = "thuvien_hoi_dap"
    allowed_domains = ["thuvienphapluat.vn"]

    start_urls = [
        "https://thuvienphapluat.vn/hoi-dap-phap-luat/tien-te-ngan-hang"
    ]

    custom_settings = {
        # ensure Playwright can be used if enabled in project
        # set DOWNLOAD_DELAY if needed
    }

    def parse(self, response):
        # container with list of articles
        container = response.css("div.tvpl-main.container.pt-3.pb-3")
        if not container:
            container = response

        # iterate article cards
        for article in container.css("article"):
            href = article.css("a::attr(href)").get()
            if href:
                yield response.follow(href, callback=self.parse_detail, meta={"playwright": True})

        # follow pagination (try common patterns)
        next_link = response.css("a[rel=next]::attr(href)").get()
        if not next_link:
            # fallback: look for link with 'page=' in href and text 'Next' or '»'
            next_link = response.xpath("//a[contains(@href,'page=') and (contains(text(),'Tiếp') or contains(text(),'›') or contains(text(),'»'))]/@href").get()

        if next_link:
            yield response.follow(next_link, callback=self.parse, meta={"playwright": True})

    def start_requests(self):
        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/114.0.0.0 Safari/537.36"
        )
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers={"User-Agent": ua},
                meta={"playwright": True},
            )

    def parse_detail(self, response):
        url = response.url

        # find the main detail container described by the user
        detail = response.css("div.tvpl-main.container.pt-3.pb-3.wap-page-detail")
        if not detail:
            # try a more permissive selector
            detail = response.css("div.wap-page-detail, div.wap-page-detail.container")
        if not detail:
            detail = response

        # Extract id from URL: last number before .html
        m = re.search(r"-(\d+)\.html$", url)
        doc_id = m.group(1) if m else None

        # Question/title
        question = detail.css("h1::text").get()
        if question:
            question = question.strip()

        # Answer: try to extract a content block and get its visible text
        # Use XPath string() to collapse inner text and preserve spacing
        answer = detail.xpath('string(.//div[contains(@class,"answer") or contains(@class,"post-content") or contains(@class,"content") or contains(@class,"article-content") or contains(@class,"news-detail") or contains(@class,"entry-content")])').get()
        if not answer:
            # fallback: take most text under detail
            answer = detail.xpath('string(.)').get()
        if answer:
            answer = " ".join([s.strip() for s in answer.splitlines() if s.strip()])

        # Category: try breadcrumb or page heading
        category = response.css("nav.breadcrumb a::text").getall()
        category = category[-1].strip() if category else None

        # Author and published date heuristics
        author = detail.css(".author::text, .post-author::text, .author-name::text").get()
        if author:
            author = author.strip()

        pub_raw = detail.css(".date::text, .post-date::text, .publish-date::text, .meta time::attr(datetime)").get()
        published_date = None
        if pub_raw:
            pub_raw = pub_raw.strip()
            # try common date formats
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                try:
                    dt = datetime.strptime(pub_raw, fmt)
                    published_date = dt.strftime("%Y-%m-%d")
                    break
                except Exception:
                    continue

        # legal refs and tags: try to collect comma/semicolon separated lists
        legal_refs = detail.css(".legal-ref::text, .ref-list::text, .tags a::text").getall()
        legal_refs = ";".join([s.strip() for s in legal_refs if s and s.strip()]) if legal_refs else ""

        tags = detail.css(".tags a::text, .tag-list a::text, .post-tags a::text").getall()
        tags = ";".join([s.strip() for s in tags if s and s.strip()]) if tags else ""

        # views
        views_raw = detail.css(".views::text, .post-views::text").re_first(r"\d[\,\d]*")
        views = int(views_raw.replace(',', '')) if views_raw else None

        yield {
            "id": doc_id or "",
            "question": question or "",
            "answer": answer or "",
            "category": category or "",
            "author": author or "",
            "published_date": published_date or "",
            "legal_refs": legal_refs or "",
            "tags": tags or "",
            "views": views or 0,
            "url": url,
        }
