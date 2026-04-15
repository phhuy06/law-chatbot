# Crawler Expected Output

## Nguon du lieu

URL: https://thuvienphapluat.vn/hoi-dap-phap-luat/

Day la phan Hoi dap phap luat — moi entry la 1 cap cau hoi + tra loi cua chuyen gia phap ly.

Moi category co 1000+ trang, moi trang ~10 items. Tong cong uoc tinh **100,000+ Q&A pairs**.

### URL phan trang theo category

```
https://thuvienphapluat.vn/hoi-dap-phap-luat/quyen-dan-su?page=1
https://thuvienphapluat.vn/hoi-dap-phap-luat/quyen-dan-su?page=2
...
https://thuvienphapluat.vn/hoi-dap-phap-luat/quyen-dan-su?page=1571
```

### URL trang chi tiet

```
https://thuvienphapluat.vn/hoi-dap-phap-luat/mua-vang-nhan-co-phai-chuyen-khoan-khong-138085238.html
```

Pattern: `/hoi-dap-phap-luat/[slug]-[id].html`

### Cac category can crawl

- Quyen dan su
- Tien te - Ngan hang
- Doanh nghiep
- Lao dong - Tien luong
- Bat dong san
- Giao thong - Van tai
- Xuat nhap khau
- Chung khoan
- So huu tri tue
- Thu tuc To tung
- Tai chinh nha nuoc
- The thao - Y te

---

## Output format: CSV

Moi category xuat ra 1 file CSV. Tat ca file CSV dat trong thu muc `crawler/output/`.

### Ten file

```
crawler/output/quyen-dan-su.csv
crawler/output/tien-te-ngan-hang.csv
crawler/output/doanh-nghiep.csv
crawler/output/lao-dong-tien-luong.csv
crawler/output/bat-dong-san.csv
crawler/output/giao-thong-van-tai.csv
crawler/output/xuat-nhap-khau.csv
crawler/output/chung-khoan.csv
crawler/output/so-huu-tri-tue.csv
crawler/output/thu-tuc-to-tung.csv
crawler/output/tai-chinh-nha-nuoc.csv
crawler/output/the-thao-y-te.csv
```

### Schema CSV

| Column | Type | Mo ta | Vi du |
|--------|------|-------|-------|
| `id` | string | ID lay tu URL (so cuoi cung) | `138085238` |
| `question` | string | Cau hoi day du | `Mua vang nhan co phai chuyen khoan khong?` |
| `answer` | string | Cau tra loi day du (plain text, da strip HTML) | `Theo Nghi dinh 24/2012/ND-CP...` |
| `category` | string | Linh vuc phap luat | `Tien te - Ngan hang` |
| `author` | string | Ten chuyen gia tra loi | `Luong Thi Tam Nhu` |
| `published_date` | string | Ngay dang (YYYY-MM-DD) | `2026-03-29` |
| `legal_refs` | string | Cac van ban phap luat tham chieu, ngan cach bang dau `;` | `24/2012/ND-CP;232/2025/ND-CP` |
| `tags` | string | Tu khoa, ngan cach bang dau `;` | `Mua ban vang;Chuyen khoan` |
| `views` | integer | So luot xem | `17` |
| `url` | string | Link goc | `https://thuvienphapluat.vn/hoi-dap-phap-luat/...138085238.html` |

### Vi du 1 dong CSV

```csv
id,question,answer,category,author,published_date,legal_refs,tags,views,url
138085238,"Mua vang nhan co phai chuyen khoan khong?","Theo Nghi dinh 24/2012/ND-CP sua doi boi Nghi dinh 232/2025/ND-CP, giao dich mua ban vang tu 20 trieu dong tro len phai thuc hien qua chuyen khoan...","Tien te - Ngan hang","Luong Thi Tam Nhu","2026-03-29","24/2012/ND-CP;232/2025/ND-CP","Mua ban vang",17,"https://thuvienphapluat.vn/hoi-dap-phap-luat/mua-vang-nhan-co-phai-chuyen-khoan-khong-138085238.html"
```

### Luu y khi tao CSV

- Dung `utf-8` encoding (bat buoc cho tieng Viet)
- Dung dau phay `,` lam delimiter
- Wrap tat ca field string trong dau ngoac kep `"..."`
- Neu trong answer hoac question co dau `"`, thay bang `""` (CSV escaping)
- Strip tat ca HTML tags khoi answer — chi giu plain text
- Field `legal_refs` va `tags`: gom nhieu gia tri bang dau `;` (VD: `24/2012/ND-CP;232/2025/ND-CP`)
- Neu field nao khong co du lieu, de trong `""`

---

## Upload CSV di dau?

### Buoc 1: Luu CSV vao `crawler/output/`

```
crawler/output/
├── quyen-dan-su.csv
├── tien-te-ngan-hang.csv
├── doanh-nghiep.csv
└── ... (12 file)
```

### Buoc 2: Upload len MinIO

Dung MinIO Console (http://localhost:9001) hoac CLI:

```bash
# Cai MinIO client
brew install minio/stable/mc
mc alias set local http://localhost:9000 minioadmin minioadmin

# Tao bucket (neu chua co)
mc mb local/phapluat

# Upload tat ca CSV
mc cp crawler/output/*.csv local/phapluat/raw/csv/
```

CSV se nam tai: `s3://phapluat/raw/csv/*.csv`

### Buoc 3: Spark doc CSV tu MinIO

Spark Batch pipeline se doc CSV tu MinIO, xu ly (clean, chunk, embed), roi ghi vao Elasticsearch.

```python
# Spark doc CSV
df = spark.read.csv(
    "s3a://phapluat/raw/csv/*.csv",
    header=True,
    encoding="utf-8",
    multiLine=True,
    escape='"',
)
```

---

## Muc tieu so luong

| Muc do | So Q&A | So category | Danh gia |
|--------|--------|-------------|----------|
| Toi thieu (demo) | 1,000 | 2-3 | Du de chatbot tra loi cau hoi co ban |
| Muc tieu | 5,000 - 10,000 | 6-8 | Phu tot cac linh vuc chinh |
| Ly tuong | 50,000+ | 12 | Du lieu phong phu, chatbot tra loi chinh xac |

### Uu tien crawl theo thu tu

1. **Doanh nghiep** — nhieu cau hoi thuc te
2. **Lao dong - Tien luong** — rat pho bien
3. **Bat dong san** — nhieu nguoi quan tam
4. **Quyen dan su** — rong, nhieu chu de
5. **Thu tuc To tung** — hay gap
6. Cac category con lai

---

## Checklist cho crawler person

- [ ] Crawl duoc trang danh sach, lay duoc link tung Q&A
- [ ] Parse duoc trang chi tiet: lay du 10 field
- [ ] Xuat ra CSV dung format (utf-8, header, escape)
- [ ] Crawl 100 Q&A dau tien, kiem tra du lieu
- [ ] Scale len 1,000+ Q&A
- [ ] Upload CSV len MinIO
