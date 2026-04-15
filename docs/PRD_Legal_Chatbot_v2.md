# PROJECT REQUIREMENTS DOCUMENT
# ChatBot Hoi Dap Phap Luat Viet Nam

**He thong xu ly du lieu lon - Kien truc Lambda - RAG + GPT-4o mini**

| | |
|---|---|
| **Mon hoc** | Xu ly Du lieu Lon |
| **Phien ban** | 2.0 |
| **Ngay tao** | 26/3/2026 |
| **Cap nhat** | 9/4/2026 |
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
  |  Crawler (Playwright + Scrapy)                                     |
  |  - Crawl thuvienphapluat.vn                                       |
  |  - Cron every 2h or manual                                        |
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
  +-------+-----------------------------------------------------------+
          |
  +-------+-----------------------------------------------------------+
  |       v             PROCESSING (Spark)                             |
  |                                                                    |
  |  Spark Streaming (consumer.py)                                     |
  |  1. Consume from Kafka                                             |
  |  2. Clean text (remove HTML, normalize)                            |
  |  3. Chunk text (1000 chars, 100 overlap)                           |
  |  4. Batch embed (OpenAI text-embedding-3-small)                    |
  |  5. Bulk write to Elasticsearch                                    |
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
| Crawler | Query ES `doc_id` | Khong crawl bai da co trong ES |
| Kafka | Message key = article ID | Tracing + consumer dedup |
| ES | `_id = doc_id + chunk hash` | Chunk trung bi ghi de (upsert) |

---

## 2. Stack Cong Nghe

| Thanh phan | Cong nghe | Vai tro cu the trong du an | Ly do chon |
|---|---|---|---|
| Thu thap du lieu | Scrapy + Playwright | Crawl van ban tu thuvienphapluat.vn, xu ly JS rendering | Python native, de debug, ho tro dynamic page |
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
| phapluat/csv/*.csv | CSV backup tu crawler realtime | Crawler | data-ingest (batch mode) |
| phapluat/csv/backup/*.csv | CSV backup tu realtime mode | Crawler | Khong ai (chi luu tru) |
| phapluat/csv/processed/*.csv | CSV da duoc batch ingest xu ly | data-ingest | Khong ai (luu tru) |
| phapluat/checkpoints/streaming/ | Spark Streaming checkpoint | Spark Streaming | Spark Streaming (auto) |

### 2.2 Elasticsearch Index

He thong su dung **mot index duy nhat**: `phapluat`

| Field | Type | Mo ta |
|---|---|---|
| doc_id | keyword | Article ID tu thuvienphapluat.vn (vd: 138085436) |
| chunk_text | text | Noi dung chunk da clean |
| title | text | Tieu de bai viet |
| url | keyword | URL goc |
| category | keyword | Danh muc (vd: chung-khoan, quyen-dan-su) |
| doc_type | keyword | Loai van ban |
| agency | keyword | Co quan ban hanh |
| embedding | dense_vector (1536, cosine) | OpenAI text-embedding-3-small |

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

De bai yeu cau the hien ky nang Spark trung cap. Bang duoi mapping tung yeu cau vao implementation cu the trong du an:

| Yeu cau de bai | Implementation trong du an | File |
|---|---|---|
| Window function & aggregate nang cao | Thong ke so van ban theo loai/nam/thang, rank do lien quan theo thoi gian ban hanh | spark/analysis/stats_job.py |
| Pivot / Unpivot | Pivot bang thong ke: rows = loai van ban, cols = nam, values = so luong | spark/analysis/pivot_job.py |
| UDF tuy bien | clean_text_udf(): bo HTML tags, chuan hoa tieng Viet, xu ly encoding. chunk_text_udf(): chia doan 1000 ky tu theo cau | spark/utils/udfs.py |
| Multi-stage transform | Pipeline 5 buoc: ingest → clean → chunk → embed → index. Moi buoc la 1 transformation rieng | spark/batch/pipeline.py |
| Broadcast join | Join chunks voi bang tu dien phap ly nho (loai van ban, co quan ban hanh) | spark/batch/enrich.py |
| Sort-merge join | Join metadata van ban voi bang lich su cap nhat de loc van ban da thay the | spark/batch/filter_expired.py |
| Partition pruning | Partition theo nam ban hanh — khi re-process chi doc partition can thiet | spark/batch/pipeline.py |
| Cache / Persistence | Cache DataFrame tu dien phap ly (dung nhieu lan trong join). persist() intermediate results | spark/batch/enrich.py |
| Structured Streaming | Consume Kafka → process → ghi Elasticsearch. Output mode: append | spark/streaming/consumer.py |
| Watermark + late data | Watermark 24h: van ban crawl tre trong 24h van duoc xu ly dung | spark/streaming/consumer.py |
| Exactly-once | Checkpoint vao MinIO + Kafka offset management. Dam bao khong ghi trung vao ES | spark/streaming/consumer.py |
| Spark MLlib | TF-IDF de pre-rank ket qua truoc khi dung vector similarity, so sanh voi cosine | spark/analysis/tfidf_rank.py |

---

## 5. Luong Du Lieu Chi Tiet

### 5.1 Luong Realtime (Speed Layer — 24/7)

```
Crawler (cron 2h hoac thu cong)
    |
    +-- 1. Crawl trang 1-2 tu thuvienphapluat.vn
    |
    +-- 2. Voi moi bai viet, query ES: "doc_id ton tai?"
    |       |-- CO  → bo qua
    |       +-- CHUA → crawl noi dung
    |
    +-- 3. Publish document truc tiep vao Kafka (topic: van-ban-phap-luat)
    |       → Spark Streaming nhan NGAY LAP TUC
    |
    +-- 4. Upload CSV vao MinIO csv/backup/ (chi backup, khong xu ly)
    |
    v
Kafka (topic: van-ban-phap-luat)
    |
    v
Spark Streaming (consumer.py)
    |-- 5. Parse JSON tu Kafka message
    |-- 6. clean_text_udf() → bo HTML, normalize Unicode
    |-- 7. chunk_text_udf() → chia doan 1000 chars, overlap 100
    |-- 8. Batch embed qua OpenAI API (text-embedding-3-small)
    +-- 9. Bulk write vao ES index: phapluat
              _id = {doc_id}_{md5_hash}
```

**Thoi gian tu luc crawler phat hien bai moi → chatbot tra loi duoc: < 1 phut**

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
2. FastAPI kiem tra Redis cache (key = hash(cau hoi))
   |-- Cache hit → tra ve ngay
   +-- Cache miss:
       3. Goi OpenAI embed cau hoi → vector 1536 chieu
       4. Elasticsearch hybrid search tren index phapluat
          (kNN cosine + full-text match), lay top 5 chunks
       5. Ghep 5 chunks vao prompt + goi GPT-4o mini
       6. Kiem tra cau tra loi co "khong du thong tin" →
          neu co: tra ve khong kem sources
          neu khong: tra ve kem danh sach sources (title + URL)
       7. Luu ket qua vao Redis (TTL 3600s)
       8. Tra ve React UI
```

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
| published_date | date | Ngay dang | 2026-04-07 |
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
├── crawler/                    # Scrapy + Playwright crawler
│   ├── playwright_scrape.py    # Main crawl script
│   ├── output/                 # CSV output files
│   └── HUONG-DAN-CRAWL-REALTIME.md  # Crawler docs
├── kafka/                      # Kafka producer/consumer
│   ├── producer.py             # DocumentProducer class
│   └── minio_ingest.py         # Batch: MinIO → Kafka
├── spark/
│   ├── streaming/              # Speed layer
│   │   └── consumer.py         # Kafka → ES (realtime)
│   ├── batch/                  # Batch layer
│   │   └── pipeline.py         # CSV → ES (manual)
│   ├── analysis/               # Spark skills demo
│   │   ├── stats_job.py        # Window functions
│   │   ├── pivot_job.py        # Pivot/Unpivot
│   │   └── tfidf_rank.py       # MLlib TF-IDF
│   └── utils/
│       └── udfs.py             # UDFs: clean_text, chunk_text
├── backend/                    # FastAPI + RAG pipeline
│   ├── main.py
│   ├── config.py
│   ├── routers/chat.py
│   └── services/
│       ├── search.py           # ES hybrid search
│       ├── llm.py              # OpenAI embed + GPT
│       └── cache.py            # Redis cache
├── frontend/                   # React + TypeScript (Vite)
├── scripts/
│   └── init_elasticsearch.py   # ES index initialization
├── kibana/
│   ├── dashboard-export.ndjson # Kibana dashboard
│   └── init-kibana.sh          # Auto-import on startup
├── k8s/                        # Kubernetes manifests
│   ├── infrastructure/         # Kafka, MinIO, ES, Redis
│   ├── app/                    # Backend, Frontend, Spark
│   └── deploy.sh               # One-click K8s deploy
├── docker-compose.yml
├── .env
└── CLAUDE.md
```

---

## 10. Rui Ro va Giam Thieu

| Rui ro | Kha nang | Tac dong | Giam thieu |
|---|---|---|---|
| thuvienphapluat.vn block IP do crawl qua nhanh | Trung binh | Khong lay du data | Rate limit 1-2s/request, dung random delay, User-Agent rotation |
| Mac Mini M4 het RAM khi chay tat ca (16GB) | Thap | He thong crash khi demo | Giam ES heap xuong 512MB, tat Spark Batch khi khong can |
| OpenAI API cost vuot budget | Trung binh | Ton tien khong can thiet | Cache aggressively voi Redis, batch embedding calls, dung text-embedding-3-small |
| Spark Streaming mat checkpoint | Thap | Xu ly trung van ban | Checkpoint tren MinIO (persistent), K8s restart policy |
| Elasticsearch index corrupt | Rat thap | Mat toan bo data tim kiem | Raw data van trong MinIO, chay Spark Batch de rebuild |
