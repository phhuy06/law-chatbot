# PROJECT REQUIREMENTS DOCUMENT
# ChatBot Hoi Dap Phap Luat Viet Nam

**He thong xu ly du lieu lon - Kien truc Lambda - RAG + GPT-4o mini**

| | |
|---|---|
| **Mon hoc** | Xu ly Du lieu Lon |
| **Phien ban** | 2.1 |
| **Ngay tao** | 26/3/2026 |
| **Cap nhat** | 19/4/2026 |
| **So thanh vien** | 5 nguoi |
| **Thoi gian** | 8 tuan (1 ngay/tuan) |
| **Moi truong** | Mac Mini M4 — Local K8s (Minikube) |

---

## 1. Tong Quan Du An

### 1.1 Mo ta bai toan

He thong xay dung mot chatbot hoi dap ve phap luat Viet Nam, cho phep nguoi dung dat cau hoi bang ngon ngu tu nhien va nhan cau tra loi chinh xac dua tren van ban phap luat thuc te duoc thu thap tu thuvienphapluat.vn.

Bai toan phu hop voi Big Data vi:
- Corpus van ban phap luat Viet Nam co hang tram nghin van ban, lien tuc cap nhat
- Can pipeline xu ly NLP quy mo lon: lam sach, chia doan, tao embedding vector
- Yeu cau tim kiem semantic real-time tren tap du lieu lon
- Kien truc phai scale duoc khi data tang len production

### 1.2 Kien truc tong the

Du an trien khai Lambda Architecture voi hai layer chinh:

**Speed Layer (Spark Structured Streaming):** Xu ly van ban moi real-time ngay khi crawler thu thap duoc. Van ban moi xuat hien trong chatbot sau ~30 giay.

**Batch Layer (Spark Batch — chay thu cong):** Re-process toan bo corpus tu MinIO khi can — nang cap model embedding, sua bug xu ly, hoac phuc hoi Elasticsearch sau su co.

**Serving Layer:** Elasticsearch (vector search + full-text) + Redis (cache) + FastAPI (RAG pipeline) + GPT-4o mini (sinh cau tra loi).

**Luu y:** He thong su dung **mot ES index duy nhat** (`phapluat`) cho ca speed va batch layer. Khong tach rieng index.

### 1.3 Pham vi du an

- Thu thap van ban phap luat tu thuvienphapluat.vn (muc tieu: toi da so luong co the trong thoi gian cho phep)
- Xay dung pipeline Spark xu ly va tao embedding vector
- Chatbot tra loi cau hoi phap luat bang tieng Viet, co trich dan nguon
- Toan bo he thong chay tren Mac Mini M4 voi Minikube (local Kubernetes)

Ngoai pham vi:
- Khong xu ly cau hoi tu van phap ly ca nhan (chi tra cuu van ban)
- Khong deploy len cloud public
- Khong co tinh nang user authentication

### 1.4 Kien truc chi tiet

```
                          DATA INGESTION
  +------------------------------------------------------------------+
  |                                                                    |
  |  Crawler (Patchright + BeautifulSoup)                              |
  |  - Crawl thuvienphapluat.vn (bypass Cloudflare)                   |
  |  - Persistent Chrome profile giu cf_clearance cookie              |
  |  - Cron hourly (run_hourly.sh) hoac manual (run_realtime.sh)      |
  |  - Pre-check: query ES doc_id → --stop-on-seen                    |
  |       |                    |                                       |
  |   (realtime)           (backup)                                    |
  |       |                    |                                       |
  |       v                    v                                       |
  |  +----------+      +-----------+                                   |
  |  |  Kafka   |      |   MinIO   |                                   |
  |  | (message)|      | (CSV file)|                                   |
  |  |          |      |           |                                   |
  |  | topic:   |      | bucket:   |                                   |
  |  | van-ban- |      | phapluat/ |                                   |
  |  | phap-luat|      | csv/      |                                   |
  |  +----+-----+      | csv/backup|                                   |
  |       |            +-----------+                                   |
  |       |                  ^                                         |
  |       +---> kafka-consumer ---> phapluat/raw/{YYYY}/{MM}/{id}.json |
  |             (luu JSON per-doc de replay / audit)                   |
  +-------+-----------------------------------------------------------+
          |
  +-------+-----------------------------------------------------------+
  |       v             PROCESSING (Spark)                             |
  |                                                                    |
  |  Spark Streaming (spark/streaming/consumer.py)                     |
  |  1. readStream Kafka (maxOffsetsPerTrigger)                        |
  |  2. clean_text UDF (remove HTML, NFC normalize)                    |
  |  3. chunk_text UDF (1000 chars, 100 overlap, sentence-aware)       |
  |  4. foreachBatch:                                                  |
  |     4a. Batch-level ES dedup: skip chunks co doc_id da ton tai    |
  |     4b. Batch embed driver-side (100 texts/call OpenAI)            |
  |     4c. Bulk write ES (_id = doc_id + md5(doc_id:chunk)[0:12])    |
  |     4d. Invalidate Redis chat:* keys neu co doc moi                |
  |       |                                                            |
  |  Spark Master + Spark Worker (cluster)                             |
  +-------+-----------------------------------------------------------+
          |
  +-------+-----------------------------------------------------------+
  |       v                  SERVING                                   |
  |                                                                    |
  |  +----------------+  +-----------+  +-----------+                  |
  |  | Elasticsearch  |  |   Redis   |  |  Kibana   |                  |
  |  | (index:phapluat)|  |  (cache)  |  |(dashboard)|                  |
  |  |                |  |           |  |           |                  |
  |  | - Full-text    |  | - Response|  | - Total   |                  |
  |  | - kNN (cosine) |  |   caching |  |   chunks  |                  |
  |  | - 1536-dim     |  | - TTL 1h  |  | - Unique  |                  |
  |  |   vectors      |  |           |  |   docs    |                  |
  |  +-------+--------+  +-----+-----+  +-----------+                 |
  |          |                  |                                       |
  |          +--------+---------+                                      |
  |                   |                                                |
  |                   v                                                |
  |  +--------------------------------------+                          |
  |  | Backend (FastAPI)          :8000     |                          |
  |  | 1. Check Redis cache                |                          |
  |  | 2. Embed question (OpenAI)          |                          |
  |  | 3. Hybrid search ES (kNN + text)    |                          |
  |  | 4. Generate answer (GPT-4o-mini)    |                          |
  |  | 5. Cache response in Redis          |                          |
  |  +------------------+------------------+                          |
  |                      |                                             |
  |                      v                                             |
  |  +--------------------------------------+                          |
  |  | Frontend (React + Vite)     :3000    |                          |
  |  | Nginx reverse proxy -> Backend       |                          |
  |  +--------------------------------------+                          |
  +--------------------------------------------------------------------+
```

### 1.5 Deduplication (Chong trung lap)

| Tang | Co che | Mo ta |
|---|---|---|
| Crawler | Query ES `doc_id` + `--stop-on-seen` | Bo qua bai da co; dung ca category khi gap bai da thay (listings newest-first) |
| Kafka | Message key = article ID | Tracing + consumer dedup |
| Spark batch/streaming | `fetch_existing_doc_ids()` truoc khi embed | Query ES aggregation `doc_id`, skip chunks cua doc_id da ton tai — tranh ton token OpenAI |
| ES | `_id = doc_id + md5(doc_id:chunk)[0:12]` | Chunk trung bi ghi de (upsert) |

---

## 2. Stack Cong Nghe

| Thanh phan | Cong nghe | Vai tro cu the trong du an | Ly do chon |
|---|---|---|---|
| Thu thap du lieu | Patchright + BeautifulSoup | Crawl thuvienphapluat.vn, xu ly JS rendering, bypass Cloudflare challenge bang persistent Chrome profile | Patchright la Playwright drop-in co stealth patches du sau de qua Cloudflare; classic/new headless bi flag nen phai chay headed + real Chrome channel |
| Message Queue | Apache Kafka | Truyen van ban tu crawler sang Spark Streaming | Exactly-once delivery, replay duoc trong 7 ngay |
| Object Storage | MinIO | Luu CSV backup tu crawler, checkpoint Spark | S3-compatible, chay local, co UI web de demo |
| Stream Processing | Spark Structured Streaming | Xu ly van ban moi real-time tu Kafka 24/7 | Cung API voi Spark Batch, exactly-once, watermark |
| Batch Processing | Apache Spark (PySpark) | Re-process toan bo corpus khi can, chay thu cong | Xu ly song song, scale duoc, yeu cau de bai |
| Search & Vector DB | Elasticsearch 8.x | Full-text search + kNN vector search (HNSW) | Tich hop ca 2 loai search trong 1 service |
| Truc quan hoa | Kibana 8.x | Dashboard thong ke corpus, monitoring query | Di kem Elasticsearch, khong can setup rieng |
| Cache | Redis | Cache ket qua cau hoi thuong gap (TTL 1h) | In-memory, nhanh, tiet kiem API GPT |
| LLM | GPT-4o mini (OpenAI API) | Sinh cau tra loi tu context phap luat | Tot nhat cho tieng Viet, gia re |
| Embedding | text-embedding-3-small | Tao vector 1536 chieu cho chunks va cau hoi | Nhanh, re, chat luong tot |
| Backend API | FastAPI (Python) | RAG pipeline endpoint, orchestrate toan bo serving | Async, fast, tu sinh OpenAPI docs |
| Frontend | React + TypeScript | Giao dien chat, hien thi cau tra loi + nguon trich dan | Component-based, de phat trien |
| Orchestration | Docker Compose (dev) / K8s (demo) | Chay tat ca service | Production-like |

### 2.1 Cau truc luu tru MinIO

| Path trong MinIO | Noi dung | Ghi boi | Doc boi |
|---|---|---|---|
| phapluat/csv/*.csv | CSV upload thu cong de chay batch ingest | Nguoi dung (hoac crawler neu chay batch-seed) | data-ingest |
| phapluat/csv/backup/*.csv | CSV backup tu crawler realtime (ten file co timestamp) | Crawler (sau moi run co row moi) | Khong ai (chi luu tru) |
| phapluat/csv/processed/*.csv | CSV da duoc batch ingest xu ly | data-ingest | Khong ai (luu tru) |
| phapluat/raw/{YYYY}/{MM}/{doc_id}.json | JSON per-document dump tu kafka-consumer, dung de replay / audit | kafka-consumer | Khong ai (chi luu tru) |
| checkpoints/streaming_python/ | Spark Structured Streaming checkpoint (offset + state) — local FS, khong phai MinIO | Spark Streaming | Spark Streaming (auto) |

### 2.2 Elasticsearch Index

He thong su dung **mot index duy nhat**: `phapluat`

| Field | Type | Analyzer / dims | Mo ta |
|---|---|---|---|
| doc_id | keyword | — | Article ID tu thuvienphapluat.vn (vd: 138085436) |
| chunk_text | text | `vietnamese` (ICU) | Noi dung chunk da clean |
| title | text | `vietnamese` (ICU) | Tieu de bai viet |
| url | keyword | — | URL goc |
| category | keyword | — | Danh muc (vd: chung-khoan, quyen-dan-su) |
| doc_type | keyword | — | Loai van ban |
| agency | keyword | — | Co quan ban hanh |
| embedding | dense_vector | 1536, cosine, HNSW index | OpenAI text-embedding-3-small |

**Vietnamese analyzer:** custom analyzer `vietnamese` dung `icu_tokenizer` + `icu_normalizer` + `icu_folding` + `lowercase` — xu ly dau tieng Viet va normalize Unicode dung.

**Document ID trong ES:** `_id = {doc_id}_{md5(doc_id:chunk_text)[:12]}`
- Dam bao moi chunk co ID duy nhat
- Cung bai viet, cung noi dung chunk → ghi de (upsert)
- Cho phep re-index ma khong tao ban sao

---

## 3. Truc Quan Hoa Du Lieu — Kibana

Kibana duoc deploy cung Elasticsearch, truy cap qua trinh duyet tai http://localhost:5601.

Kibana doc truc tiep tu Elasticsearch index — moi van ban Spark xu ly va ghi vao ES deu xuat hien ngay trong dashboard.

### 3.1 Cac dashboard da xay dung

| Dashboard | Mo ta | Visualizations |
|---|---|---|
| Legal Chatbot - Document Analytics | Buc tranh toan canh du lieu da thu thap | Total Chunks (Metric), Unique Documents (Metric), Chunks by Category (Pie), Top Agencies (Bar), Document Details (Table) |

### 3.2 Index pattern

| Index Pattern | Dung cho |
|---|---|
| phapluat | Tat ca dashboard — mot index duy nhat |

---

## 4. Yeu Cau Ky Nang Spark (theo De Bai)

De bai yeu cau the hien ky nang Spark trung cap. Bang duoi mapping tung yeu cau vao implementation cu the trong du an.

**Ghi chu trang thai:** DONE = da implement va chay duoc; TODO = da tao file skeleton, chua implement; PARTIAL = implement mot phan.

| Yeu cau de bai | Implementation trong du an | File | Trang thai |
|---|---|---|---|
| UDF tuy bien | `clean_text_udf`: bo HTML tags (BeautifulSoup), NFC normalize tieng Viet, xu ly encoding. `chunk_text_udf`: chia doan sentence-aware toi da 1000 chars, overlap 100 | spark/utils/udfs.py | DONE |
| Structured Streaming | Consume Kafka → clean → chunk → foreachBatch (dedup + embed + bulk ES). Output mode: append | spark/streaming/consumer.py | DONE |
| Multi-stage transform | Pipeline 5 buoc: readStream/read CSV → clean → chunk → batch embed → bulk write ES. Moi buoc la 1 transformation rieng | spark/streaming/consumer.py, spark/batch/pipeline.py | DONE |
| Exactly-once (thuc dung) | Combo: (a) Kafka offset checkpoint local, (b) `fetch_existing_doc_ids()` pre-write dedup, (c) deterministic `_id = doc_id + md5(doc_id:chunk)[0:12]` → ES upsert. Dam bao khong ghi trung va khong ton token OpenAI cho doc da co | spark/streaming/consumer.py, spark/batch/pipeline.py | DONE |
| Window function & aggregate nang cao | Du kien: thong ke so van ban theo loai/nam/thang, rank do lien quan theo thoi gian ban hanh | spark/analysis/stats_job.py | TODO (file trong) |
| Pivot / Unpivot | Du kien: pivot bang thong ke — rows = loai van ban, cols = nam, values = so luong | spark/analysis/pivot_job.py | TODO (file trong) |
| Broadcast join | Du kien: join chunks voi bang tu dien phap ly nho (loai van ban, co quan ban hanh) | spark/batch/enrich.py | TODO (file trong) |
| Sort-merge join | Du kien: join metadata van ban voi bang lich su cap nhat de loc van ban da thay the | spark/batch/filter_expired.py | TODO (file trong) |
| Partition pruning | Du kien: partition theo nam ban hanh — khi re-process chi doc partition can thiet | spark/batch/pipeline.py | TODO |
| Cache / Persistence | Du kien: cache DataFrame tu dien phap ly (dung nhieu lan trong join). persist() intermediate results | spark/batch/enrich.py | TODO |
| Watermark + late data | Du kien: watermark 24h; hien tai streaming consumer chua khai bao `withWatermark` (chua gap event-time late data trong scope hien tai) | spark/streaming/consumer.py | TODO |
| Spark MLlib | Du kien: TF-IDF de pre-rank ket qua truoc khi dung vector similarity, so sanh voi cosine | spark/analysis/tfidf_rank.py | TODO (file trong) |

---

## 5. Luong Du Lieu Chi Tiet

### 5.1 Luong Realtime (Speed Layer — 24/7)

```
Crawler (cron hourly hoac run_realtime.sh)
    |
    +-- 0. `--list-categories`: scan hub page → lay toan bo slug
    |
    +-- 1. Voi moi category, crawl trang 1-3 (newest first)
    |
    +-- 2. Voi moi bai viet, query ES: "doc_id ton tai?"
    |       |-- CO + --stop-on-seen → dung ca category (moi bai sau do deu da co)
    |       |-- CO khong flag        → bo qua bai do
    |       +-- CHUA                 → extract noi dung
    |
    +-- 3. Publish document truc tiep vao Kafka (topic: van-ban-phap-luat)
    |       → Spark Streaming nhan sau ~trigger interval
    |       → kafka-consumer song song luu phapluat/raw/{YYYY}/{MM}/{id}.json
    |
    +-- 4. Upload CSV vao MinIO csv/backup/ CHI KHI co row moi
    |
    v
Kafka (topic: van-ban-phap-luat)
    |
    v
Spark Streaming (spark/streaming/consumer.py)
    |-- 5. readStream Kafka, parse JSON theo kafka_schema
    |-- 6. clean_text (UDF) → bo HTML, NFC normalize
    |-- 7. chunk_text (UDF) → chia doan sentence-aware 1000 chars, overlap 100
    |-- 8. foreachBatch(batch_id):
    |      a. Collect rows → unique doc_ids
    |      b. fetch_existing_doc_ids() → query ES aggregation, loai bo doc_id da co
    |      c. batch_embed() → goi OpenAI text-embedding-3-small, 100 texts/request
    |      d. helpers.bulk(actions) → ghi ES voi _id = doc_id + md5(doc_id:chunk)[0:12]
    |      e. invalidate_chat_cache(redis_url) → xoa moi key chat:* neu success > 0
    +-- (checkpoint local: checkpoints/streaming_python/)
```

**Thoi gian tu luc crawler phat hien bai moi → chatbot tra loi duoc: < 1 phut**

**Cache consistency:** vi Redis chat cache co TTL 1h, neu khong invalidate thi van ban moi phai doi den ca tieng moi xuat hien trong response cache. Event-driven invalidation o buoc 8e dam bao cau hoi cu chay lai se thay doc moi ngay lap tuc.

### 5.2 Luong Batch (chay thu cong khi can)

```
1. Upload CSV vao MinIO (phapluat/csv/)
2. Chay: docker-compose run --rm data-ingest
       |
       +-- data-ingest doc CSV tu MinIO
       +-- Parse tung row → push Kafka
       +-- Move CSV sang csv/processed/
       |
       v
3. Kafka → Spark Streaming → ES (cung pipeline nhu realtime)
```

**Kich hoat khi:** (a) initial seed lan dau, (b) re-index sau su co, (c) nang cap model embedding

### 5.3 Luong Serving (Nguoi dung hoi chatbot)

```
1. React UI gui cau hoi → FastAPI POST /api/chat
2. FastAPI kiem tra Redis cache (key = "chat:" + sha256(cau hoi))
   |-- Cache hit → tra ve ngay
   +-- Cache miss:
       3. Goi OpenAI embed cau hoi → vector 1536 chieu
       4. Elasticsearch hybrid search tren index phapluat, top_k = 10:
          - kNN cosine (field=embedding, boost=0.3, num_candidates=100)
          - match chunk_text (boost=5.0)
          - match title (boost=3.0)
          Cac score cong lai → chon top 10
       5. Ghep 10 chunks lam context [0..9] + goi GPT-4o-mini (temperature 0.3)
          Prompt yeu cau GPT tra ve JSON:
             {"answer": "...", "used_sources": [0, 2, 5]}
       6. Kiem tra "không tìm thấy thông tin" trong answer →
          neu co: sources = []
          neu khong va GPT co used_sources: lay chunks[used_sources], dedup theo URL
          neu GPT quen trich dan: fallback top 3 chunks
       7. Luu {answer, sources} vao Redis (TTL 3600s)
       8. Tra ve React UI
```

**Event-driven cache invalidation:** Khi Spark Streaming/Batch ghi doc moi vao ES, no se SCAN + DELETE moi key `chat:*` trong Redis. Nguoi dung hoi lai cau hoi cu se tu dong thay doc moi — khong phai doi TTL het han.

---

## 6. Muc Tieu Du Lieu

### 6.1 So luong van ban

| Muc do | So van ban | So chunks ES (est.) | Danh gia |
|---|---|---|---|
| Toi thieu (demo duoc) | 1,000 | ~15,000 | Du de chatbot tra loi cac cau hoi co ban |
| Muc tieu thuc te | 5,000 – 10,000 | ~75,000 – 150,000 | Phu tot cac linh vuc phap luat chinh |
| Ly tuong | 20,000+ | ~300,000+ | Gan voi he thong production thuc te |

### 6.2 Loai van ban uu tien crawl

- Chung khoan
- Giao thong van tai
- Quyen dan su
- So huu tri tue
- Tai chinh nha nuoc
- The thao y te
- Thu tuc to tung
- Tien te ngan hang
- Xuat nhap khau

### 6.3 Schema van ban (Kafka message)

| Field | Type | Mo ta | Vi du |
|---|---|---|---|
| id | string | Article ID tu URL | 138085436 |
| title | string | Tieu de bai viet | Mua vang nhan co phai chuyen khoan khong? |
| content | string | Noi dung day du | Theo quy dinh tai khoan 10... |
| category | string | Danh muc | tien-te-ngan-hang |
| doc_type | string | Loai van ban | (co the trong) |
| doc_number | string | So hieu | (co the trong) |
| agency | string | Co quan ban hanh | (co the trong) |
| published_date | string (ISO 8601) | Ngay + gio dang (format "YYYY-MM-DDTHH:MM:SS") | 2026-04-07T10:30:00 |
| url | string | Link nguon goc | https://thuvienphapluat.vn/... |
| crawled_at | datetime | Thoi gian crawl | 2026-04-09T08:30:00Z |

---

## 7. Services va Ports

| Service | Port | Vai tro | Chay |
|---|---|---|---|
| Zookeeper | 2181 | Kafka coordination | Always on |
| Kafka | 9092 | Message queue | Always on |
| MinIO | 9000 / 9001 | Object storage (CSV backup) | Always on |
| Elasticsearch | 9200 | Search + vector DB | Always on |
| Kibana | 5601 | Analytics dashboard | Always on |
| Redis | 6379 | Response cache | Always on |
| Spark Master | 7077 / 8080 | Processing cluster | Always on |
| Spark Worker | — | Processing node | Always on |
| Spark Job | — | Streaming: Kafka → ES | Always on |
| Backend | 8000 | FastAPI (RAG pipeline) | Always on |
| Frontend | 3000 | React UI (Nginx) | Always on |
| data-ingest | — | Batch: MinIO → Kafka | Manual trigger |

**Khoi dong:** `docker-compose up -d`
**Chay batch:** `docker-compose run --rm data-ingest`

---

## 8. Yeu Cau Phi Chuc Nang

| Tieu chi | Yeu cau | Cach do |
|---|---|---|
| Latency chatbot | < 10s cho cau hoi thong thuong (bao gom ca GPT response time) | Do tu request den response trong React |
| Latency khi cache hit | < 500ms | Redis GET latency |
| Thoi gian van ban moi co trong chatbot | < 60 giay sau khi crawler push vao Kafka | Log timestamp Kafka → ES |
| Elasticsearch search latency | < 200ms cho kNN query top-5 | ES slow log threshold |
| Uptime Spark Streaming | > 99% (tu restart sau crash nho K8s) | Kubernetes liveness probe |
| RAM tong he thong | < 14GB tren Mac Mini M4 16GB | kubectl top nodes |

---

## 9. Cau Truc Repository

```
legal-chatbot/
├── crawler/                    # Patchright-based crawler (bypass Cloudflare)
│   ├── playwright_scrape.py    # Main crawl script (patchright + BeautifulSoup)
│   ├── run_hourly.sh           # Cron: discover categories + sweep pages 1-3
│   ├── output/                 # CSV output files (per-category)
│   ├── scrapy.cfg              # Legacy — Scrapy khong con duoc dung
│   └── spiders/                # Trong — giu cho tuong lai
├── kafka/                      # Kafka producer/consumer + MinIO ingest
│   ├── producer.py             # DocumentProducer class
│   ├── consumer.py             # DocumentConsumer — Kafka → MinIO raw JSON
│   ├── entrypoint_consumer.py  # Container entrypoint
│   ├── minio_ingest.py         # Batch: poll MinIO csv/ → Kafka → move csv/processed/
│   └── Dockerfile
├── spark/
│   ├── streaming/              # Speed layer
│   │   └── consumer.py         # Kafka → clean/chunk → batch embed → ES (+ cache invalidate)
│   ├── batch/                  # Batch layer
│   │   ├── pipeline.py         # CSV/MinIO → ES (manual)
│   │   ├── enrich.py           # TODO: broadcast join tu dien phap ly
│   │   └── filter_expired.py   # TODO: sort-merge join loc van ban thay the
│   ├── analysis/               # Spark skills demo (TODO — all stubs)
│   │   ├── stats_job.py        # TODO: window functions
│   │   ├── pivot_job.py        # TODO: pivot/unpivot
│   │   └── tfidf_rank.py       # TODO: MLlib TF-IDF
│   ├── utils/
│   │   └── udfs.py             # UDFs: clean_text, chunk_text (+ embed_text unused)
│   └── Dockerfile
├── backend/                    # FastAPI + RAG pipeline
│   ├── main.py
│   ├── config.py               # pydantic-settings env config
│   ├── routers/chat.py         # POST /api/chat
│   ├── services/
│   │   ├── search.py           # ES hybrid search (kNN boost 0.3 + text 5.0 + title 3.0)
│   │   ├── llm.py              # OpenAI embed + GPT-4o-mini JSON response
│   │   └── cache.py            # Redis cache, sha256 key, TTL 3600s
│   └── Dockerfile
├── frontend/                   # React + TypeScript (Vite)
├── scripts/
│   └── init_elasticsearch.py   # ES index + Vietnamese ICU analyzer + HNSW mapping
├── kibana/
│   ├── dashboard-export.ndjson # Kibana dashboard
│   └── init-kibana.sh          # Auto-import on startup
├── k8s/                        # Kubernetes manifests (Minikube)
│   ├── infrastructure/         # Kafka, MinIO, ES, Redis
│   ├── app/                    # Backend, Frontend, Spark, Kafka workers
│   └── deploy.sh               # One-click K8s deploy
├── run_realtime.sh             # Manual realtime crawl (delegate to crawler/run_hourly.sh)
├── run_batch.sh                # Manual Spark batch via docker-compose
├── docker-compose.yml
├── .env
└── CLAUDE.md
```

---

## 10. Rui Ro va Giam Thieu

| Rui ro | Kha nang | Tac dong | Giam thieu |
|---|---|---|---|
| Cloudflare challenge block crawler | Cao | Khong crawl duoc bai moi | Dung patchright (Playwright + stealth patches) voi real Chrome channel, persistent profile giu cf_clearance cookie; bat buoc chay headed — headless bi flag |
| thuvienphapluat.vn rate-limit IP | Trung binh | Khong lay du data | Random delay 1-2s giua articles, dung `--stop-on-seen` de khong quet lai bai da co |
| Mac Mini M4 het RAM khi chay tat ca (16GB) | Thap | He thong crash khi demo | Giam ES heap xuong 512MB, tat Spark Batch khi khong can |
| OpenAI API cost vuot budget | Trung binh | Ton tien khong can thiet | (a) Cache aggressively Redis TTL 1h + event invalidation, (b) batch embedding 100 texts/call, (c) ES-side dedup truoc khi embed → khong embed lai doc da co, (d) dung text-embedding-3-small thay vi -large |
| Spark Streaming mat checkpoint | Thap | Xu ly trung van ban | Checkpoint local (`checkpoints/streaming_python/`) + chunk `_id` deterministic (upsert vao ES neu trung) |
| Elasticsearch index corrupt | Rat thap | Mat toan bo data tim kiem | Raw JSON per-doc van trong MinIO (`phapluat/raw/.../`), CSV backup trong `phapluat/csv/backup/`, chay Spark Batch de rebuild |

---

## 11. Changelog

### v2.1 (19/4/2026)
Cap nhat theo thuc te code (sau milestone realtime crawl + end-to-end pipeline):

- **Crawler:** chuyen tu Scrapy+Playwright → Patchright (stealth patch) + BeautifulSoup. Bat buoc real Chrome channel, headed mode, persistent profile de vuot Cloudflare.
- **Cron:** doi tu "every 2h" → hourly. Auto-discover toan bo categories moi run. Pages 1-3 thay vi 1-2. Them `--stop-on-seen` flag.
- **Dedup:** them tang Spark batch/streaming — query ES aggregation truoc khi embed de tranh ton token OpenAI cho doc da co.
- **Embedding:** chuyen tu per-row UDF sang driver-side `batch_embed()` 100 texts/call.
- **Cache:** them event-driven invalidation — Spark xoa `chat:*` Redis keys sau moi batch ghi ES thanh cong.
- **MinIO:** them path `phapluat/raw/{YYYY}/{MM}/{doc_id}.json` do kafka-consumer ghi per-document.
- **ES index:** them Vietnamese ICU analyzer (`icu_tokenizer` + `icu_normalizer` + `icu_folding`).
- **Hybrid search:** cap nhat boost weights (kNN 0.3, chunk_text 5.0, title 3.0), top_k = 10.
- **LLM:** GPT-4o-mini tra ve JSON co `used_sources` — chi show sources GPT thuc su dung, dedup theo URL.
- **published_date:** chuyen tu date-only → ISO 8601 full datetime (giu time de sort cung ngay).
- **Section 4 (Spark skills):** them cot "Trang thai" — danh dau cac job analysis (stats/pivot/tfidf/enrich/filter_expired) va watermark la TODO do file hien tai moi la skeleton.
