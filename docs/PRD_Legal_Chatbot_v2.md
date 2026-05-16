# PROJECT REQUIREMENTS DOCUMENT
# ChatBot Hoi Dap Phap Luat Viet Nam

**He thong xu ly du lieu lon - Kien truc Lambda - RAG + GPT-4o mini**

| | |
|---|---|
| **Mon hoc** | Xu ly Du lieu Lon |
| **Phien ban** | 2.4 |
| **Ngay tao** | 26/3/2026 |
| **Cap nhat** | 16/5/2026 |
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

**Batch Layer (Spark Batch — chay theo schedule):** Re-process **master dataset** tu MinIO `phapluat/raw/{YYYY}/{MM}/*.json` (immutable per-doc JSON do kafka-consumer ghi). Cung 1 nguon o moi moi truong: K8s CronJob `spark-batch` (`0 */6 * * *`) va docker service `spark-batch-cron` (`BATCH_INTERVAL_SECONDS=21600`) doc cung path → khong con su khac biet local-FS vs container.

Mac dinh batch lam **dedup-skip** (chi xu ly doc_id chua co trong ES) de tiet kiem token OpenAI. Khi can authoritative re-derive (vd upgrade embedding model, fix chunker), kich hoat `BATCH_FORCE=true`:

- Local: `./run_batch.sh --force`
- K8s: `kubectl apply -f k8s/app/spark-batch-force.yaml`

**Serving Layer:** Elasticsearch (vector search + full-text) + Redis (cache) + FastAPI (RAG pipeline) + GPT-4o mini (sinh cau tra loi).

**Observability Layer:** Prometheus (scrape metrics tu pods/services) + Grafana (dashboard infrastructure) + Kibana (dashboard data analytics + audit log).

**Governance & Fault Tolerance:** Audit log (index `phapluat-audit`, PII-sanitized) + Snapshot ES → MinIO (CronJob `es-snapshot`, mac dinh moi 24 gio) + RAG eval framework (`eval/` — precision@k, recall@k, category hit rate).

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
| Metrics | Prometheus | Scrape metrics tu pods/services trong namespace `law-chatbot` | Chuan de-facto cho monitoring K8s, tich hop san service discovery |
| Monitoring | Grafana | Dashboard infrastructure metrics (CPU/RAM/network theo pod, ES/Kafka/Redis health) | Tich hop san voi Prometheus datasource |
| Orchestration | Kubernetes (Minikube) | Trien khai toan bo service trong namespace `law-chatbot` | Production-like, mot lenh `deploy.sh` khoi dong toan he thong |

### 2.1 Cau truc luu tru MinIO

| Path trong MinIO | Noi dung | Ghi boi | Doc boi |
|---|---|---|---|
| phapluat/csv/*.csv | CSV upload thu cong de chay batch ingest | Nguoi dung (hoac crawler neu chay batch-seed) | data-ingest |
| phapluat/csv/backup/*.csv | CSV backup tu crawler realtime (ten file co timestamp) | Crawler (sau moi run co row moi) | Khong ai (chi luu tru) |
| phapluat/csv/processed/*.csv | CSV da duoc batch ingest xu ly | data-ingest | Khong ai (luu tru) |
| phapluat/raw/{YYYY}/{MM}/{doc_id}.json | JSON per-document dump tu kafka-consumer, dung de replay / audit | kafka-consumer | Khong ai (chi luu tru) |
| checkpoints/streaming_python/ | Spark Structured Streaming checkpoint (offset + state) — local FS, khong phai MinIO | Spark Streaming | Spark Streaming (auto) |
| bucket `es-snapshots` | Elasticsearch snapshots (repository S3 `phapluat-snapshots`) — disaster recovery cho ca `phapluat` va `phapluat-audit` | CronJob `es-snapshot` (24h) | `_snapshot/_restore` API khi recovery |

### 2.2 Elasticsearch Indices

He thong su dung **2 index**:

- **`phapluat`** — corpus phap luat (full-text + kNN vector search).
- **`phapluat-audit`** — audit log cau hoi nguoi dung (PII da redact).

#### Index `phapluat`

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

#### Index `phapluat-audit`

Audit log cua moi cau hoi vao `/api/chat`. PII bi redact truoc khi luu (xem Section 8.1).

| Field | Type | Mo ta |
|---|---|---|
| timestamp | date | Thoi diem nhan request (UTC, ISO 8601) |
| question_hash | keyword | SHA-256 cua cau hoi goc (de dedup ma khong luu nguyen ban) |
| question_sanitized | text | Cau hoi sau khi redact PII ([EMAIL], [PHONE], [ID], [TAX_ID]) |
| answer_length | integer | Do dai cau tra loi (so ky tu) |
| source_count | integer | So nguon trich dan trong response |
| latency_ms | integer | Tong latency tu request den response |
| cache_hit | boolean | Cau hoi tra ve tu Redis cache hay khong |
| pii_redactions | integer | So PII bi redact trong cau hoi |

---

## 3. Truc Quan Hoa Du Lieu va Monitoring

He thong co hai lop truc quan hoa rieng biet:

- **Kibana** — dashboard du lieu nghiep vu (so van ban, so chunks, phan loai theo category). Doc truc tiep tu Elasticsearch.
- **Grafana** — dashboard ha tang (CPU/RAM tung pod, trang thai ES/Kafka/Redis). Doc tu Prometheus.

### 3.1 Kibana — Data Analytics

Kibana deploy cung Elasticsearch, truy cap qua `minikube service kibana -n law-chatbot`.

Kibana doc truc tiep tu Elasticsearch index — moi van ban Spark xu ly va ghi vao ES deu xuat hien ngay trong dashboard. Job `kibana-init` (chay 1 lan sau khi Kibana ready) tu dong import data view + dashboard tu file `kibana/dashboard-export.ndjson`.

| Dashboard | Mo ta | Visualizations |
|---|---|---|
| Legal Chatbot - Document Analytics | Buc tranh toan canh du lieu da thu thap | Total Chunks (Metric), Unique Documents (Metric), Avg Chunks per Document (Metric), Chunks by Category (Pie), Documents per Category (Bar), Top 10 Longest Documents (Bar), Document Details (Table) |

**Index pattern:** `phapluat` — mot index duy nhat cho moi visualization.

### 3.2 Grafana — Infrastructure Monitoring

Grafana truy cap qua `minikube service grafana -n law-chatbot` (login: admin / admin123).

Datasource Prometheus duoc cau hinh san (provisioning). Prometheus scrape:

- Pods trong namespace `law-chatbot` co annotation `prometheus.io/scrape: "true"`
- Services trong namespace `law-chatbot` (cung annotation)
- Kubernetes nodes (cAdvisor metrics)
- Elasticsearch, Kafka, Redis (static targets)

Retention: 15 ngay. Storage: PVC 10Gi.

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
1. Upload CSV vao MinIO (phapluat/csv/) qua console http://<minikube>/minio
2. Deployment `data-ingest` (long-running poller, mac dinh 10s/lan):
       |
       +-- data-ingest doc CSV tu MinIO
       +-- Parse tung row → push Kafka
       +-- Move CSV sang csv/processed/
       |
       v
3. Kafka → Spark Streaming → ES (cung pipeline nhu realtime)

Master dataset (immutable): MinIO phapluat/raw/{YYYY}/{MM}/{doc_id}.json
                            (ghi boi kafka-consumer khi nhan moi message tu Kafka)

Scheduled re-process (Lambda batch layer — dedup-skip mode):
    K8s     : CronJob `spark-batch` (schedule `0 */6 * * *` — moi 6 gio)
    Docker  : service `spark-batch-cron` (loop sleep ${BATCH_INTERVAL_SECONDS}, mac dinh 21600s)
    Source  : MinIO `phapluat/raw/*/*/*.json` (cung 1 nguon o ca 2 moi truong)

Force re-embed (authoritative — sau khi upgrade model/chunker):
    Local   : ./run_batch.sh --force
    K8s     : kubectl apply -f k8s/app/spark-batch-force.yaml
    Effect  : skip ES dedup, re-embed toan corpus

Legacy local CSV (back-compat cho dev local):
    Local   : ./run_batch.sh --local '*.csv'    # doc crawler/output/*.csv
```

**CronJob dam bao** corpus duoc re-process dinh ky kem dedup-skip → bat duoc doc moi (do speed layer co the bo lo neu Kafka outage), va idempotent o tang chunk `_id` nen ghi de an toan.

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

Tat ca service chay trong K8s namespace `law-chatbot`. Cot "Service port" la port noi bo cluster (ClusterIP). Cot "NodePort" chi co o nhung service can truy cap tu host (Minikube tunnel).

| Service | Service port | NodePort | Loai | Vai tro |
|---|---|---|---|---|
| Zookeeper | 2181 | — | Deployment | Kafka coordination |
| Kafka | 9092 | — | Deployment | Message queue |
| kafka-init | — | — | Job (one-shot) | Tao topic `van-ban-phap-luat` luc khoi dong |
| MinIO | 9000 / 9001 | — | Deployment | Object storage |
| Elasticsearch | 9200 / 9300 | — | Deployment + PVC 10Gi | Search + vector DB |
| Kibana | 5601 | — | Deployment | Data analytics dashboard |
| kibana-init | — | — | Job (one-shot) | Import dashboard `dashboard-export.ndjson` luc khoi dong |
| es-init | — | — | Job (one-shot) | Tao index `phapluat` voi Vietnamese ICU analyzer + HNSW mapping |
| Redis | 6379 | — | Deployment | Response cache |
| Spark Master | 7077 / 8080 | — | Deployment | Processing cluster |
| Spark Worker | — | — | Deployment | Processing node |
| spark-job | — | — | Deployment | Streaming: Kafka → ES (24/7) |
| spark-batch | — | — | CronJob (`0 */6 * * *`) | Batch re-process master dataset MinIO `raw/` (Lambda batch layer, dedup-skip) |
| spark-batch-force | — | — | Job (manual apply) | Force re-embed corpus, skip ES dedup. Use sau khi upgrade embedding model/chunker |
| es-snapshot-init | — | — | Job (one-shot) | Tao bucket `es-snapshots` + register repository S3 `phapluat-snapshots` |
| es-snapshot | — | — | CronJob (`0 2 * * *`) | Snapshot daily `phapluat` + `phapluat-audit` → MinIO |
| kafka-consumer | — | — | Deployment | Kafka → MinIO raw JSON dump |
| data-ingest | — | — | Deployment | Poll MinIO csv/ → Kafka |
| Backend | 8000 | — | Deployment | FastAPI (RAG pipeline) |
| Frontend | 80 | 30080 | Deployment (NodePort) | React UI (Nginx, proxy /api/ → backend) |
| Prometheus | 9090 | 30090 | Deployment + PVC 10Gi + RBAC | Scrape pods/services metrics |
| Grafana | 3000 | 30300 | Deployment + PVC 5Gi | Dashboard infrastructure (admin/admin123) |

**Khoi dong toan bo:** `bash k8s/deploy.sh`
**Truy cap dashboard:**
- `minikube service frontend -n law-chatbot`
- `minikube service kibana -n law-chatbot`
- `minikube service grafana -n law-chatbot`
- `minikube service prometheus -n law-chatbot`

---

## 8. Governance, Chat Luong Du Lieu & Chiu Loi

### 8.1 Audit log + PII sanitization

Moi cau hoi vao `/api/chat` duoc ghi vao ES index `phapluat-audit` (xem Section 2.2). Truoc khi luu, `AuditService` (file `backend/services/audit.py`) redact PII bang regex:

| Pattern | Vi du dau vao | Output sau redact |
|---|---|---|
| Email | `user@example.com` | `[EMAIL]` |
| Phone VN | `0912345678`, `+84912345678`, `0912.345.678` | `[PHONE]` |
| CMND 9 / CCCD 12 so | `123456789`, `012345678901` | `[ID]` |
| Ma so thue 10 so | `0312345678-001` | `[TAX_ID]` |

Cau hoi goc khong duoc luu — chi luu `question_hash` (SHA-256) de tracing va `question_sanitized` cho analytics. Audit ghi best-effort: neu ES not reachable, request chinh van thanh cong, chi log warning.

Kibana co the visualize index `phapluat-audit` de theo doi: query volume theo gio, top categories duoc hoi, cache hit rate, latency P95, so PII redactions per query (signal cho compliance).

### 8.2 RAG Evaluation Framework

Thu muc `eval/`:

| File | Mo ta |
|---|---|
| `eval/gold_set.json` | ~12 cau hoi tieng Viet tieu bieu (doanh nghiep, giao thong, thue, lao dong, ...) voi expected keywords + categories |
| `eval/run_eval.py` | Chay tung cau qua backend (`--backend`) hoac ES truc tiep (`--direct`), tinh `precision@k`, `recall@k`, `category_hit_rate`, mean latency |
| `eval/README.md` | Huong dan su dung |

**Metric:**
- **precision@k** — ti le sources tra ve match expected keywords trong title/text/url.
- **recall@k** — 1.0 neu it nhat 1 source match, 0.0 neu khong.
- **category hit rate** — ti le query co source dung category mong doi.

Chay offline khong can LLM key voi `--direct` mode → eval rieng tang retrieval, tach roi rui ro GPT.

### 8.3 Unit testing

Thu muc `tests/` voi pytest:

| File | Test |
|---|---|
| `tests/test_chunker.py` | `clean_text` (strip HTML, NFC normalize, whitespace collapse, empty input) + `chunk_text` (short text, max_length, long single sentence, sentence boundaries) |
| `tests/test_audit.py` | PII sanitization cho email / phone / CMND / CCCD / clean text khong bi sai redact |

Chay: `pytest tests/ -v` (16 tests, ~0.1s). Pure Python — khong can SparkSession nho refactor `spark/utils/text.py` tach pure functions khoi UDF wrappers.

### 8.4 Fault tolerance: ES snapshot → MinIO

Disaster recovery cho ca corpus va audit log:

| Buoc | Component |
|---|---|
| 1. Install `repository-s3` plugin | `elasticsearch/Dockerfile` (custom image `law-chatbot/elasticsearch:latest`) |
| 2. Tao bucket `es-snapshots` + register repo `phapluat-snapshots` | Job `es-snapshot-init` (chay 1 lan luc deploy) |
| 3. Snapshot dinh ky | CronJob `es-snapshot` (`0 2 * * *` — moi 24h) hoac docker-compose service `es-snapshot-cron` (default `SNAPSHOT_INTERVAL_SECONDS=86400`) |
| 4. Restore | `POST _snapshot/phapluat-snapshots/snap-YYYYMMDD-HHMMSS/_restore` |

Script: `scripts/snapshot_elasticsearch.py` — idempotent, dung `httpx` PUT thay vi client lib de tranh version drift.

### 8.5 Toi uu chi phi cho Batch Layer (Lambda) — Lessons Learned

He thong su dung **convergent Lambda** (speed + batch ghi cung 1 ES index `phapluat`). Day la deliberate trade-off: tranh maintain 2 index rieng + complicated query-time merge, danh doi chinh "authoritative re-derivation" classic. Cac ky thuat dang dung va co the dung de tiet kiem chi phi:

| Ky thuat | Trang thai | Mo ta | Tiet kiem |
|---|---|---|---|
| **Idempotent writes (deterministic ID)** | ✅ implemented | `_id = doc_id + md5(doc_id:chunk_text)[:12]` → batch chay lai chi upsert, khong duplicate | Khong ton dung luong ES, khong sai du lieu |
| **Dedup-skip (BATCH_FORCE=false)** | ✅ implemented | Truoc khi embed, query ES aggregation `doc_id` → bo doc da co. Chi embed doc moi | ~99% chi phi OpenAI cho cac batch run sau initial seed |
| **Driver-side batch embedding** | ✅ implemented | 100 texts/call OpenAI thay vi 1 text/call → giam so request | Giam ~99% so HTTP overhead |
| **Cache invalidation event-driven** | ✅ implemented | Sau khi batch ghi ES thanh cong, xoa Redis `chat:*` → user thay doc moi ngay ma khong can doi TTL | Tranh stale response cho cau hoi cu |
| **Authoritative escape hatch (BATCH_FORCE=true)** | ✅ implemented | Cho phep skip dedup khi can re-derive (upgrade model/chunker). Cost ~full initial embedding bill | Co the toi uu khi can mà van controlled |
| **Hot/cold tiering (Parquet cho cold)** | ⚠️ partial | MinIO `raw/` JSON la cold archive; hot path la ES. Co the chuyen JSON sang Parquet khi corpus > 10k docs (column pruning) | ~10x giam IO khi scan cold archive |
| **Incremental batch (watermark)** | ❌ not yet | Theo doi "last batch run timestamp", chi xu ly file MinIO co `LastModified > watermark` | Bo qua scan toan bo raw/ moi chu ky |
| **Versioned embeddings** | ❌ not yet | Them field `embedding_model_version` vao ES; batch chi re-embed neu version khac → khong can BATCH_FORCE toan corpus | Cho phep gradual upgrade |
| **Spot/preemptible compute** | n/a | Khong ap dung trong Minikube demo, nhung la lesson chinh cho production (batch chiu duoc kill, dung spot tiet kiem 60-80%) | — |
| **API rate-limit circuit breaker** | ❌ not yet | Cap so OpenAI call per batch run, dung som neu vuot quota | Tranh runaway cost neu logic bug |

**Lesson chinh:** Lambda batch trong thuc te khong phai "re-compute tat ca tu raw moi run" (do la textbook). De economical, batch can co incremental + idempotent + versioning. Day la su khac biet giua dien dan academic Lambda va production Lambda.

### 8.6 Mapping tu nhom "Bai hoc kinh nghiem"

| Nhom bai hoc | Implementation |
|---|---|
| Thu thap du lieu | Section 5.1, 5.2; crawler patchright + ES `--stop-on-seen` dedup |
| Xu ly du lieu voi Spark | Section 4, 8.5; `fetch_existing_doc_ids` + batch embed 100 texts/call + BATCH_FORCE escape hatch |
| Xu ly luong | Section 5.1; Kafka offset checkpoint + deterministic chunk `_id` |
| Luu tru du lieu | Section 2.1, 2.2; MinIO `raw/` = master dataset (immutable JSON) + ES dense_vector + ES snapshot cold archive |
| Tich hop he thong | Section 5.3; K8s service DNS + Redis cache + best-effort audit |
| Toi uu hieu nang | Section 5.3, 8.5; Redis TTL 1h + event-driven invalidation + hybrid search tuning + batch dedup-skip |
| Giam sat & go loi | Section 3; Prometheus/Grafana + Kibana data dashboard + audit log analytics |
| Mo rong (Scaling) | Section 7; Spark master/worker, ES single-node co the scale len multi-node |
| Chat luong du lieu & kiem thu | Section 8.2, 8.3; eval framework + pytest |
| Bao mat & quan tri | Section 8.1; PII sanitization + K8s Secret cho OPENAI_API_KEY (xem `k8s/app/backend.yaml`) |
| Chiu loi | Section 8.4 + Section 10; ES snapshot + Spark checkpoint + chunk upsert |

---

## 9. Yeu Cau Phi Chuc Nang

| Tieu chi | Yeu cau | Cach do |
|---|---|---|
| Latency chatbot | < 10s cho cau hoi thong thuong (bao gom ca GPT response time) | Do tu request den response trong React |
| Latency khi cache hit | < 500ms | Redis GET latency |
| Thoi gian van ban moi co trong chatbot | < 60 giay sau khi crawler push vao Kafka | Log timestamp Kafka → ES |
| Elasticsearch search latency | < 200ms cho kNN query top-5 | ES slow log threshold |
| Uptime Spark Streaming | > 99% (tu restart sau crash nho K8s) | Kubernetes liveness probe |
| RAM tong he thong | < 14GB tren Mac Mini M4 16GB | kubectl top nodes |

---

## 10. Cau Truc Repository

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
│   │   ├── text.py             # Pure Python clean_text/chunk_text (unit-testable)
│   │   └── udfs.py             # Spark UDF wrappers + embed_text
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
│   ├── init_elasticsearch.py   # ES indices (`phapluat` + `phapluat-audit`) + Vietnamese ICU analyzer + HNSW mapping
│   └── snapshot_elasticsearch.py  # Register S3 snapshot repo + take snapshot to MinIO
├── tests/                      # Pytest unit tests
│   ├── test_chunker.py         # clean_text / chunk_text
│   └── test_audit.py           # PII sanitization (email/phone/CMND/CCCD)
├── eval/                       # RAG evaluation framework
│   ├── gold_set.json           # Gold queries with expected keywords + categories
│   ├── run_eval.py             # precision@k, recall@k, category hit rate
│   └── README.md
├── kibana/
│   ├── dashboard-export.ndjson # Kibana dashboard
│   └── init-kibana.sh          # Auto-import on startup
├── k8s/                        # Kubernetes manifests (Minikube) — deployment target
│   ├── infrastructure/         # Kafka + kafka-init Job, MinIO, ES (custom image with
│   │                           # repository-s3 plugin) + Kibana + es-init Job, kibana-init
│   │                           # Job, Redis, Prometheus (+ RBAC), Grafana, es-snapshot.yaml
│   │                           # (snapshot init Job + daily CronJob)
│   ├── app/                    # Backend, Frontend (NodePort 30080), Spark (master/worker/
│   │                           # streaming Deployment + batch CronJob `0 */6 * * *`),
│   │                           # Kafka workers (kafka-consumer + data-ingest Deployments)
│   ├── namespace.yaml          # Namespace `law-chatbot`
│   └── deploy.sh               # One-click K8s deploy: build images → apply manifests → wait
├── run_realtime.sh             # Manual realtime crawl (delegate to crawler/run_hourly.sh)
├── run_batch.sh                # Manual Spark batch (local-dev shortcut)
├── .env
└── CLAUDE.md
```

---

## 11. Rui Ro va Giam Thieu

| Rui ro | Kha nang | Tac dong | Giam thieu |
|---|---|---|---|
| Cloudflare challenge block crawler | Cao | Khong crawl duoc bai moi | Dung patchright (Playwright + stealth patches) voi real Chrome channel, persistent profile giu cf_clearance cookie; bat buoc chay headed — headless bi flag |
| thuvienphapluat.vn rate-limit IP | Trung binh | Khong lay du data | Random delay 1-2s giua articles, dung `--stop-on-seen` de khong quet lai bai da co |
| Mac Mini M4 het RAM khi chay tat ca (16GB) | Thap | He thong crash khi demo | Giam ES heap xuong 512MB, tat Spark Batch khi khong can |
| OpenAI API cost vuot budget | Trung binh | Ton tien khong can thiet | (a) Cache aggressively Redis TTL 1h + event invalidation, (b) batch embedding 100 texts/call, (c) ES-side dedup truoc khi embed → khong embed lai doc da co, (d) dung text-embedding-3-small thay vi -large |
| Spark Streaming mat checkpoint | Thap | Xu ly trung van ban | Checkpoint local (`checkpoints/streaming_python/`) + chunk `_id` deterministic (upsert vao ES neu trung) |
| Elasticsearch index corrupt | Rat thap | Mat toan bo data tim kiem | (a) Snapshot daily → MinIO bucket `es-snapshots` (CronJob `es-snapshot`, Section 8.4) restore tu snapshot gan nhat; (b) Raw JSON per-doc trong MinIO `phapluat/raw/.../`; (c) CSV backup trong `phapluat/csv/backup/`; (d) chay Spark Batch de rebuild tu ngoai |
| MinIO mat data | Rat thap | Mat snapshot ES + raw JSON | Single-node + emptyDir/PVC khong erasure-coded → bo sung off-cluster backup khi production. Trong scope demo: chap nhan rui ro, du lieu nguon van co the crawl lai tu thuvienphapluat.vn |
| Lo PII trong audit log | Trung binh | Compliance / privacy | PII regex sanitization (Section 8.1) — email/phone/CMND/CCCD/MST redact truoc khi index. Cau hoi goc khong bao gio luu — chi luu hash SHA-256 |

---

## 12. Changelog

### v2.4 (16/5/2026)

Lambda architecture cleanup — batch layer reads tu master dataset chung, k8s + docker doc cung 1 source:

- **Master dataset:** MinIO `phapluat/raw/{YYYY}/{MM}/{doc_id}.json` (do `kafka-consumer` ghi tu Kafka) chinh thuc tro thanh nguon doc cho batch layer. Khong con doc tu `crawler/output/*.csv` (local FS).
- **`spark/batch/pipeline.py`:** dung boto3 (da co trong requirements-spark) → list + fetch JSON tu MinIO, then `spark.createDataFrame`. Khong them hadoop-aws/s3a (boto3 noi chuyen voi MinIO truc tiep, ko can Spark Hadoop FileSystem layer).
- **`BATCH_FORCE` env flag:** mac dinh `false` (dedup-skip — tiet kiem ~99% OpenAI cost). Set `true` → skip ES dedup, re-embed toan corpus. Trigger:
  - `./run_batch.sh --force`
  - `kubectl apply -f k8s/app/spark-batch-force.yaml` (one-shot Job)
- **`run_batch.sh`:** them `--force` va `--local <glob>` flags. Default doc MinIO raw/; `--local` cho phep fall-back doc CSV trong `crawler/output/` (back-compat dev).
- **Docker / K8s parity:** `spark-batch-cron` (docker) va `spark-batch` (K8s CronJob) doc cung MinIO `raw/`. K8s khong con "no data" gap, docker khong con phu thuoc bind-mount.
- **Section 8.5 moi:** "Toi uu chi phi cho Batch Layer" — bai hoc Lambda kinh dien vs production reality, liet ke 9 ky thuat tiet kiem (5 da apply, 4 to-do).
- **Architecture framing:** chinh thuc goi la "convergent Lambda" — speed + batch ghi 1 ES index chung, danh doi authoritative re-derivation classic de tranh complex query-time merge.

### v2.3 (16/5/2026)

Cap nhat de bao phu day du 11 nhom "Bai hoc kinh nghiem" theo de bai IT4931:

- **Batch layer scheduled:** chuyen `spark-batch` tu Job thu cong sang CronJob (K8s `0 */6 * * *`) va service `spark-batch-cron` (docker-compose, loop `BATCH_INTERVAL_SECONDS=21600`). Lambda batch layer thuc thu chay tu dong, khong con phu thuoc thao tac thu cong.
- **Audit log:** them ES index `phapluat-audit` + service `backend/services/audit.py`. Moi cau hoi vao `/api/chat` duoc log voi sanitized question + latency + cache_hit + redaction count. PII (email, phone VN, CMND/CCCD, ma so thue) bi redact bang regex truoc khi ghi.
- **ES snapshot → MinIO:** them `repository-s3` plugin trong custom image `law-chatbot/elasticsearch:latest`. `scripts/snapshot_elasticsearch.py` register repo `phapluat-snapshots` + take snapshot. K8s `es-snapshot-init` Job + `es-snapshot` CronJob (`0 2 * * *`). Docker-compose: services `es-snapshot-init` + `es-snapshot-cron`.
- **RAG eval:** them `eval/gold_set.json` (12 query tieng Viet) + `eval/run_eval.py` (precision@k, recall@k, category hit rate, hai mode: `--backend` qua FastAPI hoac `--direct` qua ES — eval rieng retrieval khong ton LLM cost).
- **Unit testing:** them `tests/` voi pytest (16 tests). Refactor `spark/utils/udfs.py` → `spark/utils/text.py` (pure Python) de chunker/cleaner unit-testable khong can SparkSession.
- **Section 2.2:** tach `phapluat` va `phapluat-audit` thanh 2 sub-section voi mapping rieng.
- **Section 8 moi:** Governance, Chat luong du lieu & Chiu loi (audit log, eval, tests, snapshot, mapping 11 nhom bai hoc).
- **Section 10 (Rui ro):** them mitigation snapshot daily, bo sung rui ro MinIO single-node + rui ro lo PII.

### v2.2 (15/5/2026)
Cap nhat theo state thuc te tren branch `main` sau cac PR k8s parity:

- **Observability:** them Prometheus (scrape pods/services trong namespace, RBAC cluster-level, PVC 10Gi, retention 15 ngay) + Grafana (datasource Prometheus provisioned, NodePort 30300, admin/admin123).
- **K8s parity voi local dev:** them Job `kibana-init` (import dashboard tu ConfigMap `kibana-dashboards`) va Deployment `data-ingest` (long-running MinIO csv/ poller, mirror dev `data-ingest` service).
- **Section 3 (Truc quan hoa):** tach thanh 2 muc — Kibana (data analytics) va Grafana (infrastructure monitoring).
- **Section 3.1 (Kibana dashboard):** cap nhat list visualization theo file `kibana/dashboard-export.ndjson` thuc te (them Avg Chunks per Document, Documents per Category, Top 10 Longest Documents; bo "Top Agencies").
- **Section 5.2 (Batch flow):** thay command dev `docker-compose run --rm data-ingest` bang K8s Deployment thuong truc + Job `spark-batch` cho full re-index.
- **Section 7 (Services va Ports):** chuyen sang mo hinh K8s — ClusterIP service port + NodePort (Frontend 30080, Prometheus 30090, Grafana 30300). Liet ke them tat ca Job va Deployment (kafka-init, es-init, kibana-init, spark-batch, kafka-consumer, data-ingest).
- **Section 9 (Repository):** mo ta chi tiet hon k8s/infrastructure va k8s/app, bo dong `docker-compose.yml` (dev tool, khong phai deployment target).

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
