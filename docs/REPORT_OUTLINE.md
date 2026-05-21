# Outline Báo cáo Đồ án IT4931
# ChatBot Hỏi-Đáp Pháp Luật Việt Nam — Kiến trúc Lambda Hội tụ

> Outline cập nhật theo state hiện tại của repo (commit `6ea4193`, PRD v2.5).
> Đã validate end-to-end trên Minikube: 1.122 docs ingest, chat trả về câu trả lời có trích dẫn, snapshot phục hồi được, speed layer round-trip ~13s.
> Khung báo cáo theo format đề bài: Định nghĩa bài toán → Kiến trúc & thiết kế →
> Chi tiết triển khai → 11 nhóm Bài học kinh nghiệm.

---

## 1. Định nghĩa bài toán

### 1.1. Bài toán lựa chọn

- **Hệ thống Hỏi-Đáp pháp luật Việt Nam** (Retrieval-Augmented Generation chatbot).
- Người dùng đặt câu hỏi bằng ngôn ngữ tự nhiên → hệ thống truy xuất văn bản pháp luật liên quan từ **thuvienphapluat.vn** → LLM (GPT-4o mini) sinh câu trả lời kèm trích dẫn nguồn.

### 1.2. Phân tích mức độ phù hợp với Dữ liệu lớn

| 5V | Hiện trạng dự án |
|---|---|
| **Volume** | Hàng trăm nghìn văn bản pháp luật, mỗi văn bản có thể >100 trang; embedding 1536 chiều × hàng triệu chunks |
| **Velocity** | Văn bản mới ban hành, sửa đổi, hết hiệu lực liên tục → cần streaming pipeline 24/7 |
| **Variety** | HTML từ web; cấu trúc phân cấp Luật → Chương → Điều → Khoản; nhiều loại văn bản (Luật, Nghị định, Thông tư...) |
| **Veracity** | Tính chính xác cao là yêu cầu cốt lõi → cần trích dẫn nguồn + kiểm soát chất lượng |
| **Value** | Hỗ trợ tra cứu cho người dân, sinh viên luật, doanh nghiệp |

### 1.3. Phạm vi và giới hạn

**Trong phạm vi:**
- Crawler thuvienphapluat.vn (Patchright + BeautifulSoup, vượt Cloudflare)
- Pipeline xử lý batch + streaming với Apache Spark
- RAG search hybrid (BM25 + kNN HNSW) trên Elasticsearch 8.x
- Web UI React + FastAPI backend
- Triển khai full stack trên Kubernetes (Minikube)
- Observability: Prometheus + Grafana + Kibana
- Audit log + PII sanitization
- Disaster recovery: ES snapshot → MinIO
- RAG evaluation framework (precision@k, recall@k)
- Unit tests (pytest)

**Ngoài phạm vi:**
- Tư vấn pháp lý chuyên sâu (chỉ tra cứu văn bản)
- Đa ngôn ngữ, mobile app
- Deploy lên cloud public (Mac Mini M4 + Minikube)
- User authentication

---

## 2. Kiến trúc và thiết kế

### 2.1. Kiến trúc tổng thể — Convergent Lambda

> Lưu ý: dự án sử dụng "Lambda hội tụ" (convergent Lambda) thay vì Lambda kinh điển — speed + batch ghi cùng 1 Elasticsearch index, dùng deterministic chunk `_id` để upsert. Không có batch-view và speed-view tách riêng. Chi tiết trade-off ở mục 4 (Bài học 2).

**Speed Layer** (real-time, 24/7):
- Crawler → Kafka topic `van-ban-phap-luat`
- `kafka-consumer` Deployment → ghi raw JSON xuống MinIO `phapluat/raw/{YYYY}/{MM}/{doc_id}.json` (master dataset)
- `spark-streaming` Deployment → readStream Kafka → clean + chunk + embed + bulk write ES

**Batch Layer** (scheduled re-process):
- K8s CronJob `spark-batch` (`0 */6 * * *`) hoặc docker service `spark-batch-cron` (loop 21600s)
- Đọc master dataset MinIO `raw/` qua boto3 (không cần hadoop-aws/s3a)
- Default `BATCH_FORCE=false`: dedup-skip mode, chỉ xử lý doc_id chưa có trong ES (tiết kiệm ~99% OpenAI cost)
- Force re-embed: `kubectl apply -f k8s/app/spark-batch-force.yaml` → one-shot Job với `BATCH_FORCE=true`

**Serving Layer:**
- Elasticsearch (full-text + kNN HNSW, 1536-dim cosine)
- Redis (response cache, TTL 1h, event-driven invalidation)
- FastAPI (RAG pipeline + audit log)
- GPT-4o-mini (OpenAI, sinh câu trả lời + select sources)
- React + TypeScript frontend

**Observability Layer:**
- Prometheus (scrape pods/services in namespace `law-chatbot`)
- Grafana (infrastructure dashboards)
- Kibana (data analytics + audit log analytics)

**Governance / Fault Tolerance:**
- Audit log: ES index `phapluat-audit`, PII redaction (email/phone/CMND/CCCD/tax_id)
- ES snapshot → MinIO bucket `es-snapshots/` (CronJob daily `0 2 * * *`)

### 2.2. Các thành phần và vai trò

| Thành phần | Công nghệ | Vai trò |
|---|---|---|
| Crawler | Patchright + BeautifulSoup | Crawl thuvienphapluat.vn, vượt Cloudflare |
| Message Queue | Apache Kafka | Truyền văn bản crawler → spark-streaming |
| Object Storage | MinIO (S3-compatible) | Master dataset (raw/) + ES snapshot + CSV backup |
| Stream Processing | Spark Structured Streaming | Speed layer 24/7 |
| Batch Processing | Spark Batch (PySpark + boto3) | Batch layer scheduled |
| Search & Vector DB | Elasticsearch 8.x (HNSW) | Hybrid search (kNN + full-text) |
| Cache | Redis | Response cache, event-driven invalidation |
| LLM | GPT-4o-mini (OpenAI) | Sinh câu trả lời với JSON `used_sources` |
| Embedding | text-embedding-3-small | 1536-dim vector cho chunks + questions |
| Backend | FastAPI (async) | RAG orchestration + audit + PII sanitization |
| Frontend | React + TypeScript (Vite) | Chat UI với citation |
| Metrics | Prometheus + Grafana | Infrastructure monitoring |
| Data Analytics | Kibana | Dashboard corpus + audit |
| Orchestration | Kubernetes (Minikube) | Full stack deployment |

### 2.3. Sơ đồ luồng dữ liệu

(Xem PRD Section 1.4 — Full flow diagram, đã update lên v2.4)

**Convergence point:** speed + batch cùng ghi vào ES `phapluat` với deterministic chunk `_id = doc_id_md5(doc_id:chunk_text)[:12]` → upsert an toàn, không duplicate.

---

## 3. Chi tiết triển khai

### 3.1. Mã nguồn

Repo structure (chi tiết tại PRD Section 10):
```
crawler/             # Patchright crawler
kafka/               # producer / consumer / minio_ingest
spark/
  streaming/         # speed layer
  batch/             # batch layer (reads MinIO raw/)
  utils/             # text.py (pure), udfs.py (Spark wrappers)
backend/             # FastAPI + RAG + audit
frontend/            # React + Vite
scripts/             # init_elasticsearch, snapshot_elasticsearch
tests/               # pytest (16 tests)
eval/                # RAG evaluation framework
k8s/                 # Minikube deployment manifests
docs/                # PRD + this outline
```

### 3.2. Cấu hình theo môi trường

| File | Mô tả |
|---|---|
| `.env` | Dev: OpenAI key, MinIO creds, ES URL, Redis URL |
| `docker-compose.yml` | Dev orchestration |
| `k8s/infrastructure/` | ConfigMap + Secret + Deployment cho infra |
| `k8s/app/` | Backend, frontend, spark, kafka-workers, spark-batch-force |
| `k8s/deploy.sh` | One-click full deploy |

### 3.3. Chiến lược triển khai và giám sát

- **CI**: `pytest tests/` (chunker + audit PII)
- **CD**: `bash k8s/deploy.sh` builds images → applies manifests → waits ready
- **Monitoring**: Prometheus scrape (retention 15d, PVC 10Gi), Grafana datasource provisioned
- **Logging**: stdout → `kubectl logs`
- **Data analytics**: Kibana dashboard `Legal Chatbot - Document Analytics`
- **Audit**: ES index `phapluat-audit` (Kibana visualizations on top)

---

## 4. Bài học kinh nghiệm (Lessons Learned)

> Mỗi bài học theo template: **Mô tả vấn đề** → **Cách tiếp cận đã thử** → **Giải pháp cuối cùng** → **Điểm rút ra**.

### Bài học 1: Thu thập dữ liệu

**Mô tả vấn đề**
- thuvienphapluat.vn dùng Cloudflare anti-bot, JS-heavy
- Văn bản pháp luật có nhiều phiên bản (luật sửa đổi, hết hiệu lực)
- Crawl chậm dễ gặp rate-limit

**Cách tiếp cận đã thử**
1. Scrapy + Playwright cổ điển → bị Cloudflare flag (`headless: True`)
2. Playwright thường + stealth plugin → cookie không persist
3. **Patchright + BeautifulSoup + persistent Chrome profile** ✅

**Giải pháp cuối cùng**
- Patchright (Playwright drop-in với stealth patches), real Chrome channel, headed mode
- Persistent profile giữ cookie `cf_clearance`
- Cron hourly + pre-check ES `doc_id` + `--stop-on-seen` → bỏ qua bài đã có
- Random delay 1-2s giữa articles
- Dedup 4 tầng: Crawler (ES pre-check) → Kafka (key) → Spark (aggregation) → ES (deterministic `_id`)

**Điểm rút ra**
- Crawler chống bot phải dùng stealth tool chuyên dụng — patch headless không đủ
- Cookie session phải persistent giữa runs, không reset mỗi lần
- Dedup nên multi-layer để giảm cost (đặc biệt khi downstream là LLM API)

---

### Bài học 2: Xử lý dữ liệu với Spark — Cost Optimization

**Mô tả vấn đề**
- Lambda kinh điển yêu cầu batch re-compute toàn corpus mỗi run
- Mỗi chunk gọi OpenAI embedding API → cost tỷ lệ thuận với số chunks
- 10k docs × 15 chunks/doc × $0.00002 = ~$3/run; chạy mỗi 6h = $12/day

**Cách tiếp cận đã thử**
1. Per-row UDF gọi OpenAI mỗi chunk → 99% thời gian là HTTP overhead
2. Re-embed toàn bộ mỗi batch run → chi phí tuyến tính theo số runs

**Giải pháp cuối cùng — Convergent Lambda với 5 cost optimizations**
1. **Idempotent writes**: `_id = doc_id_md5(doc_id:chunk_text)[:12]` → re-runs upsert, không duplicate
2. **Dedup-skip default**: `fetch_existing_doc_ids` ES aggregation trước khi embed → skip docs đã có (`BATCH_FORCE=false`)
3. **Batch embedding**: 100 texts/OpenAI call (driver-side) → giảm 99% HTTP overhead
4. **Event-driven cache invalidation**: Spark xóa Redis `chat:*` sau khi ghi ES thành công
5. **Force-rebuild escape hatch**: `BATCH_FORCE=true` qua `kubectl apply -f spark-batch-force.yaml` — controlled, expensive, only when needed

**Điểm rút ra**
- Textbook Lambda ("re-compute mọi run") không kinh tế khi downstream là paid API
- Production Lambda = idempotent + incremental + versioned + force-rebuild escape hatch
- Convergent Lambda (1 ES index cho cả 2 layer) trade-off "authoritative re-derivation" để tránh complex query-time merge
- Future work: incremental watermark, versioned embeddings, rate-limit circuit breaker (chi tiết PRD Section 8.5)

---

### Bài học 3: Xử lý luồng (Stream Processing)

**Mô tả vấn đề**
- Cần exactly-once delivery: văn bản không trùng, không thiếu
- Xử lý late-arriving data (văn bản backfill từ những năm cũ)
- Recovery sau crash

**Cách tiếp cận đã thử**
1. At-most-once: dễ mất event khi consumer crash
2. At-least-once đơn thuần: dễ duplicate

**Giải pháp cuối cùng — "Exactly-once thực dụng"**
- Kafka offset checkpoint local FS (`checkpoints/streaming_python/`)
- `fetch_existing_doc_ids()` pre-write dedup trong `foreachBatch`
- Deterministic chunk `_id` → ES upsert (idempotent target)
- Combo 3 lớp → effectively exactly-once mà không cần distributed transaction

**Điểm rút ra**
- "Exactly-once" trong distributed system thường là illusion — solution thực tế là "at-least-once delivery + idempotent target"
- Watermark + window chưa cần thiết ở scope hiện tại (sẽ làm khi gặp event-time skew)

---

### Bài học 4: Lưu trữ dữ liệu — Master Dataset

**Mô tả vấn đề**
- Cần immutable source-of-truth cho batch layer
- Phải scale được khi corpus tăng
- Disaster recovery: ES corrupt thì có gì để rebuild?

**Cách tiếp cận đã thử**
1. CSV local trong `crawler/output/` → không persistent, không multi-environment
2. CSV trong MinIO `csv/` → CSV không tối ưu cho per-doc replay
3. **Per-document JSON trong MinIO `raw/{YYYY}/{MM}/{doc_id}.json`** ✅

**Giải pháp cuối cùng**
- `kafka-consumer` Deployment subscribe Kafka → ghi mỗi message thành 1 file JSON tại `phapluat/raw/{YYYY}/{MM}/{doc_id}.json`
- Batch layer (`spark-batch`) đọc MinIO `raw/*/*/*.json` qua boto3 (không cần hadoop-aws plugin)
- Partition theo year/month → list cheap
- ES snapshot → MinIO `es-snapshots/` (24h CronJob, repository-s3 plugin)

**Điểm rút ra**
- Master dataset phải tách khỏi serving layer — ES có thể được rebuild từ MinIO raw/
- Per-doc JSON > monolithic CSV vì cho phép selective replay
- Cùng 1 nguồn cho Docker + K8s → không có environment skew
- boto3 thay s3a:// — đơn giản hơn nhiều, tránh hadoop-aws jar bloat (~70MB)

---

### Bài học 5: Tích hợp hệ thống — K8s pitfalls thực chiến

**Mô tả vấn đề**
- 15+ services cần discovery + retry + circuit-break
- OpenAI API có thể rate-limit
- Service order dependency
- **K8s default behavior xung đột với app expectations** — nhiều bug chỉ surface khi deploy thật

**Cách tiếp cận đã thử + bug gặp phải (commit `6ea4193`)**

1. **`KAFKA_PORT=tcp://...` collision**: K8s auto-injects env var `<SVC_NAME>_PORT=tcp://<ip>:<port>` cho mọi Service trong namespace. Confluent kafka container đọc `$KAFKA_PORT` như integer config → entrypoint exit code 1 ngay sau dòng "port is deprecated". **Fix**: `enableServiceLinks: false`.

2. **Spark NumberFormatException `tcp://10.96…:7077`**: cùng vấn đề — Spark đọc `$SPARK_MASTER_PORT` qua parseInt, K8s đã inject `tcp://...` URL. **Fix**: `enableServiceLinks: false` trên mọi Spark pod.

3. **Spark master bind fail "Cannot assign requested address"**: `SPARK_MASTER_HOST=spark-master` resolve thành Service ClusterIP, không phải interface của pod. **Fix**: Downward API `status.podIP`.

4. **Kafka Controller bootstrap chicken-and-egg**: Kafka broker tự kết nối `kafka:9092` (Service DNS) lúc startup, nhưng Service chưa có endpoint vì pod chưa Ready → broker không khởi động được → readiness probe fail → loop. **Fix**: `publishNotReadyAddresses: true` trên Service Kafka.

5. **Kafka readiness probe exec hang**: `kafka-topics --list` block trong window ZK reconnect → kubelet kill exec → probe fail → flap. **Fix**: chuyển sang `tcpSocket` probe trên port 9092.

6. **Spark-streaming silently không invalidate Redis cache**: `invalidate_chat_cache()` short-circuit khi `REDIS_URL` empty. Batch CronJob có env này nhưng Deployment `spark-job` (streaming) bị thiếu — không có error log, chỉ bí ẩn "cache stale 1 tiếng". **Fix**: thêm `REDIS_URL` env vào spark-job.

7. **Best-effort patterns** giúp một số failure không cascade:
   - Audit log fail → log warning, không break user request.
   - Spark: `raise_on_error=False` trong `helpers.bulk()` → partial success acceptable.
   - OpenAI failure → fill zeros, log error, tiếp tục → không crash pipeline.

**Điểm rút ra**
- "Run on K8s" không phải drop-in; nhiều default K8s behavior xung đột với app expectations theo những cách chỉ surface khi deploy thật.
- `enableServiceLinks: false` nên là default cho mọi workload trong namespace có nhiều Services.
- `publishNotReadyAddresses: true` cần khi service phải tự-reachable lúc bootstrap (kafka, etcd, ...).
- TCP probe đơn giản và đáng tin hơn exec probe cho khanh hệ thống stable-state.
- Bug "thiếu env var" lặng lẽ là loại bug khó nhất — luôn validate end-to-end qua một test ngắn (delete + re-ingest) sau khi deploy.
- Audit + monitoring không bao giờ block user path.

---

### Bài học 6: Tối ưu hiệu năng

**Mô tả vấn đề**
- Latency chatbot cần < 10s (bao gồm GPT response)
- Search latency cần < 200ms
- OpenAI cost cần kiểm soát

**Giải pháp cuối cùng**
- Redis cache: SHA-256 key, TTL 3600s, event-driven invalidation
- Hybrid search: kNN boost 0.3 + chunk_text boost 5.0 + title boost 3.0, top_k = 10
- Batch embedding 100 texts/call
- ES analyzer: ICU tokenizer + folding cho tiếng Việt
- ES HNSW index (1536-dim cosine)
- Kết quả: cache hit < 500ms, cache miss 5-8s

**Điểm rút ra**
- Cache hit rate là metric quan trọng nhất cho cost (mỗi miss = 1 OpenAI call)
- Event-driven invalidation > TTL-only (user không bao giờ thấy stale data sau batch run)
- Hybrid search tuning là empirical — cần eval framework để pick weights

---

### Bài học 7: Giám sát & gỡ lỗi

**Mô tả vấn đề**
- 10+ services trong K8s namespace
- Spark streaming pipeline 24/7
- OpenAI cost cần track per-request

**Giải pháp cuối cùng**
- Prometheus + RBAC + service discovery via annotation `prometheus.io/scrape: "true"`
- Grafana datasource provisioned, retention 15d
- Kibana dashboard `Legal Chatbot - Document Analytics`
- Audit ES index → Kibana visualize: query volume by hour, top categories, cache hit rate, latency P95, PII redaction count
- Spark log to stdout → `kubectl logs`
- ES `_count` query để track corpus size

**Điểm rút ra**
- Audit log là dashboard cho cả "system health" và "user behavior"
- Cost analytics (LLM tokens, embedding calls) phải log structured ngay từ đầu
- Kibana cho data analytics, Grafana cho infra — không lẫn

---

### Bài học 8: Mở rộng (Scaling)

**Mô tả vấn đề**
- Mac Mini M4 16GB → giới hạn resource
- Production có thể cần handle 10x volume

**Giải pháp cuối cùng (Minikube scope)**
- Spark master + worker tách Deployment → có thể scale workers
- ES single-node với HNSW index in-memory
- Redis single-node (LRU eviction)
- ES heap 512MB, Spark worker 2GB
- K8s resource requests/limits per container

**Điểm rút ra (production lessons)**
- Horizontal scaling: Spark workers, FastAPI replicas (stateless), Kafka brokers
- Vertical: ES heap RAM (HNSW index in-memory)
- Cost trade-off: self-host embedding model vs OpenAI API (depends on QPS)
- Spot/preemptible compute cho batch (tolerates kill) → 60-80% saving production

---

### Bài học 9: Chất lượng dữ liệu & Kiểm thử

**Mô tả vấn đề**
- Văn bản từ web có thể có HTML rác, encoding sai
- Chunker logic phức tạp dễ regression
- RAG quality khó đo (subjective)

**Giải pháp cuối cùng**
- **Unit tests** (`tests/`, pytest, 16 tests):
  - `test_chunker.py`: clean_text (HTML strip, NFC normalize, whitespace), chunk_text (boundaries, oversized sentences)
  - `test_audit.py`: PII regex (email/phone/CMND/CCCD/tax_id)
- **Refactor cho testability**: tách `spark/utils/text.py` (pure Python) khỏi `udfs.py` (Spark wrappers) → unit test không cần SparkSession
- **RAG evaluation framework** (`eval/`):
  - `gold_set.json`: 12 query tiếng Việt với expected keywords + categories
  - `run_eval.py`: 2 modes — `--backend` (full RAG, có LLM cost) và `--direct` (retrieval-only, không LLM)
  - Metrics: precision@k, recall@k, category hit rate, mean latency
- **Schema validation**: Kafka schema trong `spark/streaming/consumer.py`, RAW_DOC_SCHEMA trong `spark/batch/pipeline.py`
- **Dedup detection** (Spark `fetch_existing_doc_ids` + ES `_id` deterministic)

**Điểm rút ra**
- Pure Python > UDF when testability matters
- RAG eval cần gold-set offline để measure regression không tốn LLM cost mỗi lần
- Schema-on-read (Spark) + schema-on-write (ES) = belt + suspenders

---

### Bài học 10: Bảo mật & Quản trị

**Mô tả vấn đề**
- OpenAI API key cần bảo vệ
- User query có thể chứa PII (số điện thoại, email, CMND/CCCD)
- Cần audit trail cho compliance

**Giải pháp cuối cùng**
- **K8s Secrets**: `OPENAI_API_KEY` trong `backend-secret`, mount qua `envFrom: secretRef`
- **PII sanitization** (`backend/services/audit.py`):
  - Email regex → `[EMAIL]`
  - Phone VN regex (0xx, +84xx, có/không separator) → `[PHONE]`
  - CMND 9-digit / CCCD 12-digit → `[ID]`
  - Mã số thuế 10-digit → `[TAX_ID]`
- **Audit log không lưu câu hỏi gốc** — chỉ SHA-256 hash + sanitized version
- **Best-effort**: audit fail không block user request
- **ES Snapshot** cho compliance backup

**Điểm rút ra**
- PII sanitization phải làm ở layer thấp nhất (audit service), không phụ thuộc upstream
- Hash + sanitized version đủ cho tracing/debugging mà không vi phạm privacy
- Secret rotation: K8s `kubectl rollout restart` sau update Secret → automatic

---

### Bài học 11: Chịu lỗi (Fault Tolerance)

**Mô tả vấn đề**
- ES có thể corrupt → cần backup phục hồi được
- Spark streaming có thể crash → cần checkpoint + idempotent target
- MinIO single-node là single-point-of-failure
- Kafka replication factor = 1 (demo scope)
- ES 8.x đã tighten security — credentials không thể inline trong repo body

**Cách tiếp cận đã thử**
1. Inline `access_key`/`secret_key` trong body khi register repo (chuẩn ES 6/7) → ES 8.12.2 trả về `"Setting [access_key] is insecure, but property [allow_insecure_settings] is not set"`.
2. Bật `repositories.s3.allow_insecure_settings=true` qua ES env var → ES 8.12.2 boot fail với `"unknown setting"` — escape hatch đã bị xóa khỏi 8.x.
3. **ES keystore initContainer pattern** ✅ — duy nhất production-grade path.

**Giải pháp cuối cùng**
- **ES snapshot → MinIO** với ES keystore:
  - `repository-s3` module bundled trong ES 8.12.2 (không cần install plugin).
  - Secret `minio-creds` chứa `access_key` + `secret_key`.
  - InitContainer `load-keystore` (chạy ES image): `elasticsearch-keystore create` → `add s3.client.default.access_key/.secret_key` từ env → copy keystore vào emptyDir.
  - Main ES container mount keystore qua subPath read-only.
  - Deployment strategy `Recreate` (PVC `ReadWriteOnce` — rolling update race node.lock).
  - CronJob `es-snapshot` (`0 2 * * *` daily) chạy `scripts/snapshot_elasticsearch.py snapshot`.
  - Restore: `POST _snapshot/phapluat-snapshots/snap-XXX/_restore`.
- **Spark streaming checkpoint**: local FS `checkpoints/streaming_python/`.
- **Idempotent target**: deterministic chunk `_id` → safe retry, no duplicate.
- **Master dataset replication**: MinIO `raw/` JSON là immutable, có thể replay toàn corpus qua `BATCH_FORCE=true`.
- **CSV backup** (legacy): `phapluat/csv/backup/` từ crawler.

**Verified end-to-end (18/5/2026)**
- Snapshot `snap-20260517-160729` state=SUCCESS, indices `[phapluat, phapluat-audit]`, 28 binary files (~5MB) trên MinIO bucket `es-snapshots/`.

**Known gaps**
- Kafka replication factor = 1 (single broker, demo only). Production cần ≥3 brokers.
- MinIO single-node, không erasure-coding distributed. Production cần MinIO cluster mode hoặc off-cluster S3 backup.

**Điểm rút ra**
- ES 8.x security hardening loại bỏ inline credentials → keystore là con đường duy nhất; trên K8s pattern chuẩn là initContainer build keystore từ Secret + emptyDir + subPath mount.
- 3-tier backup: ES snapshot (fast restore) + MinIO `raw/` (replay from scratch) + CSV backup (last resort).
- PVC `ReadWriteOnce` ép Deployment strategy phải là `Recreate` cho stateful workload — RollingUpdate race node.lock.
- Cần off-cluster backup cho production

**Điểm rút ra**
- 3-tier backup strategy: ES snapshot (fast restore) + MinIO raw/ (replay from scratch) + CSV backup (last resort)
- Disaster recovery testing chưa làm — production phải có chaos engineering
- Idempotent design giúp "recovery" trở thành "rerun" — no manual intervention

---

## 5. Kết luận và Hướng phát triển

### 5.1. Kết quả đạt được (đã validate end-to-end trên Minikube)

- **Batch layer**: 1.122 docs ingest qua `spark-batch` CronJob (~5.800 chunks, ~$0.06 OpenAI cost). Dedup-skip default + `BATCH_FORCE=true` escape hatch.
- **Speed layer**: delete doc → upload CSV vào MinIO `csv/` → data-ingest → Kafka → spark-streaming → ES re-index trong **~13 giây** (well under PRD target <60s).
- **Convergent Lambda**: speed + batch ghi cùng ES index `phapluat`, deterministic chunk `_id` → upsert an toàn.
- **Hybrid search**: BM25 + kNN HNSW tunable boost weights, top_k=10.
- **Chat returns grounded answers** với trích dẫn `doc_id` + URL gốc (4/5 query mẫu trả lời được, 1/5 đúng "không tìm thấy" khi corpus không có).
- **Audit + PII sanitization**: 13 entries trong `phapluat-audit`, email/phone/CMND/CCCD redacted, SHA-256 hash thay vì raw question.
- **ES snapshot DR → MinIO**: `snap-20260517-160729` state=SUCCESS, 28 binary files (~5MB) trên bucket `es-snapshots/`. Daily CronJob `0 2 * * *` configured.
- **16 unit tests pass**, RAG eval framework ready (precision@k / recall@k / category hit rate).
- **Prometheus + Grafana** running, datasource provisioned.
- **K8s deployment** (one-click `bash k8s/deploy.sh`) — manifests đã chiu test runtime; bug + fix documented trong commit `6ea4193`.

### 5.2. Hạn chế

- Kafka single-broker (replication factor = 1)
- MinIO single-node (không erasure-coding)
- Watermark + late-arrival window chưa implement (chưa gặp event-time skew)
- Spark MLlib (TF-IDF) chưa demo
- ES single-node (chưa cluster)

### 5.3. Hướng phát triển

- Versioned embeddings → granular model upgrade (replace BATCH_FORCE)
- Incremental watermark batch → tránh scan toàn MinIO raw/ mỗi cycle
- Multi-source crawler (vbpl.vn, congbao.gov.vn)
- Fine-tune embedding model cho tiếng Việt pháp lý
- Off-cluster ES snapshot (s3://aws hoặc gcs://)
- A/B test framework cho RAG quality
- Self-host embedding model (sentence-transformers) → cost optimization

---

## Tham chiếu

- PRD chi tiết: `docs/PRD_Legal_Chatbot_v2.md` (v2.5)
- Source code: branch `main`, commits từ `f691680` đến `6ea4193` (tổng 11 commits)
- Test suite: `pytest tests/` (16 tests, ~0.1s)
- RAG eval: `python eval/run_eval.py --direct`
- Deploy: `bash k8s/deploy.sh` (Minikube), `docker-compose up -d` (dev)
- Force batch re-embed: `./run_batch.sh --force` hoặc `kubectl apply -f k8s/app/spark-batch-force.yaml`
