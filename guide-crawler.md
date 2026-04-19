# Huong dan: Crawler

Crawler hien tai la mot script Playwright dung thang (`crawler/playwright_scrape.py`),
khong dung Scrapy. No cao `thuvienphapluat.vn/hoi-dap-phap-luat/<category>`, luu CSV
local, va (trong che do realtime) day moi bai viet len Kafka + backup CSV len MinIO.

## 1. Cai dat

```bash
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## 2. Cau truc

```
crawler/
  playwright_scrape.py   # script chinh
  test_realtime.py       # kiem tra ES / Kafka / MinIO truoc khi chay realtime
  run_hourly.sh          # wrapper cron (page 1-3 cua moi category, stop-on-seen)
  output/                # CSV theo category: bat-dong-san.csv, doanh-nghiep.csv, ...
```

## 3. CLI

```
python playwright_scrape.py \
    --category <slug> \          # vd: doanh-nghiep, bat-dong-san, ...
    --start 1 --end 3 \          # khoang trang listing can quet
    --limit 0 \                  # gioi han tong bai (0 = khong gioi han)
    --realtime \                 # bat dedup ES + Kafka + MinIO
    --stop-on-seen               # gap bai da co trong ES thi dung category
```

`--category` tu dong suy ra:
- URL listing = `https://thuvienphapluat.vn/hoi-dap-phap-luat/<slug>`
- File CSV   = `crawler/output/<slug>.csv`

Liet ke moi category hien co tren trang (dung cho cron / audit):

```bash
python playwright_scrape.py --list-categories
```

Lenh nay fetch `https://thuvienphapluat.vn/hoi-dap-phap-luat`, parse toan bo anchor
tro toi `/hoi-dap-phap-luat/<slug>` mot cap, in moi slug tren 1 dong, roi thoat.
`run_hourly.sh` goi no dau moi run de khong phai hardcode danh sach.

Ket noi mac dinh (host → docker-compose):

| Service       | Default                    |
| ------------- | -------------------------- |
| Elasticsearch | `http://localhost:9200`    |
| Kafka         | `localhost:29092`          |
| MinIO         | `localhost:9000`           |

## 4. Luong du lieu

```
playwright_scrape.py (host)
    │  1. ES term query tren doc_id → neu co → skip/break
    │  2. Extract question/answer/... tu trang chi tiet
    │  3. Append 1 row vao crawler/output/<category>.csv
    │  4. producer.produce("van-ban-phap-luat", key=id, value=json)
    ▼
Kafka  ──►  spark-job (consumer.py)  ──►  OpenAI embedding  ──►  Elasticsearch (index: phapluat)

Cuoi run, neu co >=1 row moi: upload CSV → MinIO bucket "phapluat/csv/backup/..."
```

Schema Kafka payload (10 truong):

```json
{
  "id": "450686",
  "title": "...",
  "content": "...",
  "category": "",
  "doc_type": "",
  "doc_number": "",
  "agency": "",
  "published_date": "2024-03-17",
  "url": "https://thuvienphapluat.vn/hoi-dap-phap-luat/.../...-450686.html",
  "crawled_at": "2026-04-18T12:00:00+00:00"
}
```

`doc_type`, `doc_number`, `agency`, `category` hien chua duoc extractor dien —
cac cot CSV `category`/`author` duoc map vao `category`/`agency` nhung thuong rong.

## 5. Chay

### Kiem tra dich vu

```bash
docker-compose up -d zookeeper kafka kafka-init elasticsearch es-init minio spark-job
cd crawler
python test_realtime.py
```

### Test thu nho

```bash
python playwright_scrape.py --category doanh-nghiep --start 1 --end 1 --limit 3 --realtime
```

Ky vong log: `[kafka] <id>` xuat hien cho moi bai moi. Neu `[kafka-fail]` → kiem tra
lai listener Kafka (`docker-compose.yml`) va bien `--kafka-servers`.

### Backfill 1 category

```bash
python playwright_scrape.py --category bat-dong-san --start 1 --end 20 --realtime
# KHONG --stop-on-seen: quet het 20 trang, skip rieng tung bai da co
```

### Chay hang gio (production)

`crawler/run_hourly.sh` duyet toan bo 16 category, moi category chay trang 1-3 voi
`--stop-on-seen` nen lan chay thu 2 tro di rat nhanh khi khong co bai moi.

Cai cron:

```bash
crontab -e
# Them dong:
0 * * * * /Users/huy/Workspaces/Hust/Big\ Data/law-chatbot/crawler/run_hourly.sh >> /Users/huy/Workspaces/Hust/Big\ Data/law-chatbot/crawler/output/cron.log 2>&1
```

macOS: co the phai cap **Full Disk Access** cho `/usr/sbin/cron`
(System Settings → Privacy & Security → Full Disk Access).

## 6. Loi thuong gap

| Loi                               | Nguyen nhan                                   | Cach sua                                      |
| --------------------------------- | --------------------------------------------- | --------------------------------------------- |
| Kafka producer hang / timeout     | Kafka listener khong expose host              | Dung `localhost:29092`, kiem tra compose file |
| Tat ca bai bi `[skip]`            | ES da co het (binh thuong), hoac CSV da co    | Tang `--end`, hoac xoa CSV neu can rebuild    |
| `[skip]` roi break ngay lap tuc   | `--stop-on-seen` + bai dau tien da co trong ES | Dung, nghia la khong co gi moi                |
| MinIO upload bi bo qua            | `new_rows_count == 0` — khong co gi de upload | Dung, day la behavior co chu y                |
| `scrapy crawl ...` bao khong tim thay spider | Du an KHONG con dung Scrapy              | Chay `python playwright_scrape.py ...`        |
