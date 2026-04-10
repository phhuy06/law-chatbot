# Huong dan: Crawler

## Tuan 1 - Muc tieu

- Cai dat moi truong Scrapy + Playwright
- Vao duoc trang thuvienphapluat.vn, parse duoc 1 trang chi tiet van ban
- Xuat ra dung 10 field JSON theo schema quy dinh

---

## 1. Cai dat moi truong

```bash
# Python
source .venv/bin/activate
pip install -r requirements.txt

# Cai Playwright browser (bat buoc de render trang JS)
playwright install chromium

# Kiem tra Scrapy chay duoc
cd crawler
scrapy version
```

---

## 2. Hieu cau truc trang web

### 2.1 Trang danh sach van ban

URL mau:
```
https://thuvienphapluat.vn/page/tim-van-ban.aspx?keyword=&area=0&match=False&type=0&status=0&signer=0&sort=1&lan=1&scan=0&org=0&fields=0&page=1
```

- Tham so `page=1`, `page=2`, ... de phan trang
- Moi trang co ~20 link den trang chi tiet van ban

Hoac tim theo loai van ban:
- Luat: `type=1`
- Nghi dinh: `type=2`
- Thong tu: `type=4`

### 2.2 Trang chi tiet van ban

URL mau:
```
https://thuvienphapluat.vn/van-ban/Doanh-nghiep/Luat-59-2020-QH14-Doanh-nghiep-123456.aspx
```

Cac thong tin can lay:

| Thong tin | Vi tri tren trang | Goi y CSS Selector |
|-----------|--------------------|--------------------|
| Tieu de | Heading chinh | `.doc-title` hoac `h1` |
| Loai van ban | Bang thuoc tinh | Dong "Loai van ban" |
| So hieu | Bang thuoc tinh | Dong "So hieu" |
| Ngay ban hanh | Bang thuoc tinh | Dong "Ngay ban hanh" — can chuyen sang `YYYY-MM-DD` |
| Co quan ban hanh | Bang thuoc tinh | Dong "Co quan ban hanh" |
| Noi dung HTML | Than van ban | `.content1` hoac `#toanvancontent` |
| Tinh trang | Bang thuoc tinh | Dong "Tinh trang" |

**LUU Y:** CSS selector o tren chi la goi y. Ban PHAI tu mo trang web, nhan F12 (DevTools), inspect de xac dinh chinh xac. Trang web co the dung class name khac.

---

## 3. Viet spider

File: `crawler/spiders/thuvienphapluat.py`

### 3.1 Cau truc co ban

```python
import scrapy
import uuid
from datetime import datetime


class ThuvienPhapLuatSpider(scrapy.Spider):
    name = "thuvienphapluat"
    allowed_domains = ["thuvienphapluat.vn"]

    def start_requests(self):
        # Tuan 1: chi can test voi 1 URL trang chi tiet
        # Chua can crawl trang danh sach
        test_url = "https://thuvienphapluat.vn/van-ban/Doanh-nghiep/Luat-59-2020-QH14-Doanh-nghiep-450686.aspx"
        yield scrapy.Request(
            test_url,
            callback=self.parse_document,
            meta={"playwright": True},  # Bat buoc de render JS
        )

    def parse_document(self, response):
        # TODO: Thay bang CSS selector thuc te sau khi inspect trang web
        title = response.css("h1::text").get("").strip()
        # ... lay cac field khac ...

        yield {
            "id": str(uuid.uuid4()),
            "title": title,
            "doc_type": doc_type,
            "doc_number": doc_number,
            "issued_date": issued_date,       # PHAI la "YYYY-MM-DD"
            "agency": agency,
            "content_html": content_html,      # Giu nguyen HTML
            "url": response.url,
            "crawled_at": datetime.utcnow().isoformat() + "Z",
            "status": status,                  # "con-hieu-luc" hoac "het-hieu-luc"
        }
```

### 3.2 Cach chay thu

```bash
cd crawler

# Chay spider, xuat ra file JSON
scrapy crawl thuvienphapluat -O output/test.json
```

Kiem tra file `output/test.json` co du 10 field khong.

---

## 4. Schema bat buoc (KHONG DUOC THAY DOI)

Moi document xuat ra PHAI co dung 10 field nay:

```json
{
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "title": "Luat Doanh nghiep 2020",
    "doc_type": "Luat",
    "doc_number": "59/2020/QH14",
    "issued_date": "2020-06-17",
    "agency": "Quoc hoi",
    "content_html": "<p>Dieu 1. Pham vi dieu chinh...</p>",
    "url": "https://thuvienphapluat.vn/van-ban/...",
    "crawled_at": "2026-03-26T10:30:00Z",
    "status": "con-hieu-luc"
}
```

| Field | Kieu | Bat buoc | Luu y |
|-------|------|----------|-------|
| `id` | string | CO | UUID4, tu generate: `str(uuid.uuid4())` |
| `title` | string | CO | Tieu de day du tu trang web |
| `doc_type` | string | CO | Mot trong: `Luat`, `Nghi dinh`, `Thong tu`, `Nghi quyet`, `Quyet dinh`, `Bo luat` |
| `doc_number` | string | CO | VD: "59/2020/QH14" |
| `issued_date` | string | CO | **Bat buoc format `YYYY-MM-DD`**. Trang web co the hien thi "17/06/2020" — phai chuyen thanh "2020-06-17" |
| `agency` | string | CO | VD: "Quoc hoi", "Chinh phu", "Bo Tai chinh" |
| `content_html` | string | CO | Giu nguyen HTML goc, KHONG strip tags |
| `url` | string | CO | `response.url` |
| `crawled_at` | string | CO | ISO 8601: `datetime.utcnow().isoformat() + "Z"` |
| `status` | string | CO | Chi co 2 gia tri: `con-hieu-luc` hoac `het-hieu-luc` |

**KHONG doi ten field. KHONG them field moi. KHONG doi kieu du lieu.** Kafka, Spark, va Backend deu phu thuoc vao schema nay.

---

## 5. Loi thuong gap

| Loi | Nguyen nhan | Cach sua |
|-----|-------------|----------|
| Trang load xong nhung khong co noi dung | Chua bat Playwright | Them `meta={"playwright": True}` vao Request |
| `issued_date` sai format | Trang web hien thi "17/06/2020" | Parse va chuyen: `datetime.strptime(raw, "%d/%m/%Y").strftime("%Y-%m-%d")` |
| `content_html` rong | Sai CSS selector | Mo DevTools (F12), inspect phan noi dung, tim dung selector |
| Bi block / 403 | Crawl qua nhanh | Da co `DOWNLOAD_DELAY = 1.5` trong settings.py, khong can sua |

---

## 6. Ket qua can dat cuoi tuan 1

- [ ] Scrapy + Playwright cai dat thanh cong
- [ ] Chay spider voi 1 URL trang chi tiet, khong loi
- [ ] Xuat ra file JSON co du 10 field
- [ ] `issued_date` dung format `YYYY-MM-DD`
- [ ] `content_html` co noi dung (khong rong)
- [ ] `status` la `con-hieu-luc` hoac `het-hieu-luc`
