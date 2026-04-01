# Next Steps - Tuan tiep theo

## Trang thai hien tai

| Module | Trang thai | Files |
|--------|-----------|-------|
| Frontend (React) | DONE | App.tsx, ChatMessage, ChatInput, types, CSS |
| Backend (FastAPI) | DONE | config, cache, search, llm, chat router |
| Kafka | DONE | producer.py, consumer.py |
| Crawler | CHUA LAM | spiders/thuvienphapluat.py (chi co docstring) |
| Spark UDFs | CHUA LAM | spark/utils/udfs.py (chi co docstring) |
| Spark Batch | CHUA LAM | spark/batch/pipeline.py, enrich.py, filter_expired.py |
| Spark Streaming | CHUA LAM | spark/streaming/consumer.py |
| Spark Analysis | CHUA LAM | spark/analysis/stats_job.py, pivot_job.py, tfidf_rank.py |
| ES Index Setup | CHUA LAM | Chua chay curl tao index |
| Kibana Dashboards | CHUA LAM | Chua tao dashboard |

---

## Viec can lam theo thu tu uu tien

### GIAI DOAN 1: Du lieu (tuan nay)

**Tat ca member deu co viec lam.**

#### Crawler person — Crawl Q&A data

Doc ky: `docs/crawler-expected-output.md`

1. Viet spider crawl https://thuvienphapluat.vn/hoi-dap-phap-luat/
2. Parse trang chi tiet: lay 10 field (id, question, answer, category, author, ...)
3. Xuat ra CSV files trong `crawler/output/`
4. Muc tieu: 1,000 Q&A trong tuan nay, 5,000+ tuan sau

#### Spark + ES person — ES index setup + Spark UDFs

1. **Tao 3 ES index** (chay curl commands trong `docs/guide-spark-elasticsearch-kibana.md`)
   - `phapluat-batch`
   - `phapluat-realtime`
   - `phapluat-search-logs`
2. **Viet spark/utils/udfs.py** — 2 UDF: `clean_text_udf()` va `chunk_text_udf()`
3. **Viet spark/batch/pipeline.py** — doc CSV tu MinIO, clean, chunk, embed, ghi vao ES
   - LUU Y: pipeline bay gio doc CSV (khong phai JSON) vi crawler xuat CSV
   - Schema CSV: xem `docs/crawler-expected-output.md`

#### FE team — Ket noi API that

Frontend hien tai dung mock response. Can doi sang goi API that:

```typescript
// Thay mockResponse bang:
const response = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question }),
});
const data: ChatResponse = await response.json();
```

Ngoai ra, them:
- Loading spinner khi dang cho tra loi
- Error handling khi backend khong tra loi
- Hien thi sources dang link click duoc

#### Team lead (ban) — Ket noi pipeline + test

1. Doi crawler person xuat CSV xong
2. Upload CSV len MinIO: `mc cp crawler/output/*.csv local/phapluat/raw/csv/`
3. Test Kafka producer voi du lieu that
4. Test backend API voi ES co du lieu that
5. Test end-to-end: hoi cau hoi -> nhan tra loi

---

### GIAI DOAN 2: Spark pipelines (tuan sau)

#### Spark + ES person

1. **spark/batch/pipeline.py** — Pipeline day du: CSV -> clean -> chunk -> embed -> ES
2. **spark/batch/enrich.py** — Broadcast join voi tu dien phap ly
3. **spark/batch/filter_expired.py** — Sort-merge join loc van ban het hieu luc
4. **spark/streaming/consumer.py** — Structured Streaming tu Kafka -> ES

#### Spark + ES person (song song)

5. **spark/analysis/stats_job.py** — Window functions + aggregates
6. **spark/analysis/pivot_job.py** — Pivot/unpivot
7. **spark/analysis/tfidf_rank.py** — TF-IDF ranking voi MLlib

---

### GIAI DOAN 3: Dashboard + Polish (tuan cuoi)

#### Spark + ES person

1. Tao 3 Kibana index patterns
2. Tao 4 Kibana dashboards (Tong quan, Monitoring, Phan tich, Search analytics)

#### FE team

1. Polish UI: responsive, Vietnamese fonts, loading states
2. Hien thi thong ke tu Kibana (optional)

#### Team lead

1. Integration test toan bo he thong
2. Chuan bi demo
3. Viet bao cao (neu can)

---

## Dependency chart

```
[Crawler CSV] ──────────────────────┐
                                     │
[ES Index Setup] ───────────────────┤
                                     ├──> [Upload CSV to MinIO]
[Spark UDFs] ───────────────────────┤        │
                                     │        v
                                     │   [Spark Batch Pipeline]
                                     │        │
[FE ket noi API] ───────────────────┤        v
                                     ├──> [Test end-to-end]
[Backend da xong] ──────────────────┘        │
                                              v
                                    [Spark Analysis + Kibana]
```

**Critical path:** Crawler phai xong CSV truoc → upload MinIO → Spark chay pipeline → ES co du lieu → Backend + FE test duoc.
