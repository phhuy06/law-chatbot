# Huong dan cai thien Crawler de du lieu tot hon

## Van de hien tai

Du lieu CSV hien tai cua crawler co van de **nghiem trong**: truong `answer` chua **toan bo noi dung trang web** thay vi chi chua phan tra loi phap luat.

### Vi du du lieu hien tai (SAI)

```
question: "Mua vang nhan co phai chuyen khoan khong?"
answer: "/ Hoi dap Phap luat / Hoi dap phap luat ve Tien te - Ngan hang 15:50 | 29/03/2026 
        Mua vang nhan co phai chuyen khoan khong? Mua vang nhan co phai chuyen khoan khong? 
        Mang theo vang khi xuat canh, nhap canh vuot muc quy dinh la hanh vi vi pham dung khong? 
        Noi dung chinh Mua vang nhan co phai chuyen khoan khong?..."
```

Truong `answer` chua: breadcrumb, menu, cau hoi lien quan, sidebar, footer... (8000-12000 ky tu nhung chi co ~500 ky tu la noi dung thuc).

### Du lieu mong muon (DUNG)

```
question: "Mua vang nhan co phai chuyen khoan khong?"
answer: "Theo Nghi dinh 24/2012/ND-CP ve quan ly hoat dong kinh doanh vang, viec mua ban vang 
        mieng phai thuc hien qua tai khoan ngan hang khi giao dich co gia tri tu 300 trieu dong 
        tro len. Tuy nhien, doi voi vang trang suc, my nghe (bao gom vang nhan), phap luat hien 
        hanh chua bat buoc phai thanh toan qua chuyen khoan."
```

## Nguyen nhan

Trong file `crawler/playwright_scrape.py`, ham `extract_from_detail()` thu lay noi dung tu nhieu selector:

```python
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
```

Nhung khong selector nao match duoc phan tra loi chinh xac tren trang `thuvienphapluat.vn`. Nen fallback chay:

```python
if not answer:
    answer = normalize_text(node_text(detail))  # <- Lay TOAN BO trang
```

## Huong dan sua

### Buoc 1: Xac dinh dung CSS selector cho phan tra loi

Mo mot trang hoi dap bat ky tren `thuvienphapluat.vn`, vi du:
```
https://thuvienphapluat.vn/hoi-dap-phap-luat/mua-vang-nhan-co-phai-chuyen-khoan-khong-138085238.html
```

Dung Chrome DevTools (F12 > Inspect) de tim phan tu HTML chua **chi phan tra loi phap luat**.

Tim cac element co the la:
- `div.question-detail-content`
- `div.cms-body`  
- `div.detail-content`
- `article .entry-content`
- Hoac bat ky class/id nao chua dung phan tra loi

**Luu y**: Moi category tren thuvienphapluat co the co layout khac nhau. Can kiem tra it nhat 5-10 trang khac nhau.

### Buoc 2: Cap nhat `playwright_scrape.py`

Thay doi danh sach `answer_selectors` trong ham `extract_from_detail()`:

```python
# Thay the danh sach nay bang selector dung
answer_selectors = [
    "div.question-detail-content",  # <-- Thay bang selector thuc te
    "div.cms-body",                  # <-- Kiem tra tren trang
    # ... them cac selector khac neu can
]
```

### Buoc 3: Loai bo noi dung khong can thiet

Truoc khi lay text, nen xoa cac phan tu khong lien quan ben trong container:

```python
# Sau khi tim duoc element chua answer
el = detail.query_selector("div.question-detail-content")  # selector thuc te
if el:
    # Xoa cac phan tu khong can thiet truoc khi lay text
    page.evaluate("""
        (container) => {
            // Xoa sidebar
            container.querySelectorAll('.sidebar, .related-questions, .ads, .banner, nav, .breadcrumb, .social-share, .comment-section')
                .forEach(el => el.remove());
            // Xoa cac link "Xem them"
            container.querySelectorAll('a[href*="xem-them"], .see-more')
                .forEach(el => el.remove());
        }
    """, el)
    answer = normalize_text(el.inner_text())
```

### Buoc 4: Cap nhat `hoi_dap_spider.py` (Scrapy)

Tuong tu, cap nhat XPath trong `parse_detail()`:

```python
# Thay dong nay:
answer = detail.xpath('string(.//div[contains(@class,"answer") or contains(@class,"post-content")...])').get()

# Bang selector chinh xac:
answer = detail.css("div.question-detail-content::text").getall()  # selector thuc te
answer = " ".join([s.strip() for s in answer if s.strip()])
```

### Buoc 5: Them truong `crawled_at`

CSV hien tai **khong co** cot `crawled_at`. Can them de pipeline hoat dong tot hon:

```python
from datetime import datetime, timezone

yield {
    "id": doc_id,
    "question": title,
    "answer": answer,
    # ... cac truong khac ...
    "crawled_at": datetime.now(timezone.utc).isoformat(),  # <-- THEM DONG NAY
}
```

## Kiem tra du lieu sau khi sua

### Cach 1: Kiem tra CSV truc tiep

```bash
python3 -c "
import csv
with open('crawler/output/test-data.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    row = next(reader)
    print('Question:', row['question'])
    print('Answer length:', len(row['answer']), 'chars')
    print('Answer preview:', row['answer'][:300])
"
```

**Tieu chi PASS**:
- `answer` chi chua noi dung tra loi phap luat (khong co breadcrumb, sidebar, menu)
- Do dai `answer` thuong 200-2000 ky tu (khong phai 8000-12000)
- Bat dau bang noi dung phap luat, khong phai "/ Hoi dap Phap luat / ..."

### Cach 2: Upload CSV len MinIO va test end-to-end

```bash
# 1. Copy CSV vao MinIO
docker cp crawler/output/ten-file.csv law-chatbot-minio-1:/tmp/
docker compose exec minio mc cp /tmp/ten-file.csv local/phapluat/csv/

# 2. Doi 30 giay cho pipeline xu ly

# 3. Test API
curl -s -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "Cau hoi tu CSV"}' | python3 -m json.tool
```

**Tieu chi PASS**: GPT tra loi duoc cau hoi voi noi dung phap luat cu the, co trich dan so hieu van ban.

## Cau truc CSV chuan

| Cot | Mo ta | Bat buoc | Vi du |
|-----|-------|----------|-------|
| `id` | ID bai viet (lay tu URL) | Co | `138085238` |
| `question` | Cau hoi phap luat | Co | `Mua vang nhan co phai chuyen khoan khong?` |
| `answer` | **Chi phan tra loi phap luat** (KHONG co sidebar/menu) | Co | `Theo Nghi dinh 24/2012/ND-CP...` |
| `category` | Danh muc | Khong | `Tien te - Ngan hang` |
| `author` | Tac gia | Khong | `Luat su Nguyen Van A` |
| `published_date` | Ngay dang | Khong | `29/03/2026` |
| `legal_refs` | Van ban phap luat lien quan | Khong | `Nghi dinh 24/2012/ND-CP` |
| `tags` | Tags | Khong | `vang,chuyen khoan` |
| `views` | Luot xem | Khong | `1250` |
| `url` | Link goc | Co | `https://thuvienphapluat.vn/...` |

## Luu y quan trong

1. **Chi lay noi dung tra loi** - Khong lay sidebar, breadcrumb, "Bai viet lien quan", footer, quang cao
2. **Kiem tra nhieu trang** - Moi category co the co layout khac nhau  
3. **Test voi 5 dong truoc** - Truoc khi crawl hang nghin trang, tao 5 dong CSV va test qua pipeline
4. **File test mau**: Xem `crawler/output/test-clean-5rows.csv` de biet du lieu tot tro nhu the nao
