# Olist E-Commerce Analytics Pipeline

> Pipeline analitik end-to-end berbasis Apache Airflow · Apache Spark · ClickHouse · Metabase

---

## Daftar Isi

1. [Gambaran Umum](#gambaran-umum)
2. [Arsitektur Sistem](#arsitektur-sistem)
3. [Struktur Direktori](#struktur-direktori)
4. [Teknologi & Dependensi](#teknologi--dependensi)
5. [Cara Menjalankan](#cara-menjalankan)
6. [Arsitektur DAG Airflow](#arsitektur-dag-airflow)
7. [Script: fetch_olist_stream.py](#script-fetch_olist_streampy)
8. [Script: process_olist_spark.py](#script-process_olist_sparkpy)
9. [Skema ClickHouse](#skema-clickhouse)
10. [Analitik & Query SQL](#analitik--query-sql)
11. [Dashboard Metabase](#dashboard-metabase)
12. [Insights & Temuan Utama](#insights--temuan-utama)

---

## Gambaran Umum

Proyek ini membangun pipeline data analitik **end-to-end** untuk dataset **Brazilian E-Commerce Public Dataset by Olist** — dataset e-commerce Brasil yang mencakup ~100.000 pesanan dari tahun 2016–2018.

### Rumusan Masalah

| # | Pertanyaan |
|---|-----------|
| 1 | Bagaimana distribusi kepuasan pelanggan (review score)? |
| 2 | Apakah keterlambatan pengiriman berkorelasi dengan review buruk? |
| 3 | Apa akar masalah utama review bintang 1? |
| 4 | Siapa yang lebih bertanggung jawab atas keterlambatan: Seller atau Carrier? |
| 5 | Bagaimana tren rasio keterlambatan per bulan? |
| 6 | Negara bagian mana yang paling banyak terdampak keterlambatan? |
| 7 | Bagaimana perbandingan rute intra-state vs inter-state memengaruhi waktu kirim? |
| 8 | Seller mana yang paling bermasalah dan apa dampak finansialnya? |

---

## Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE                              │
│                                                                     │
│  ┌─────────────┐    ┌──────────────────────────────────────────┐    │
│  │  PostgreSQL │    │          Apache Airflow                  │    │
│  │  (Metadata  │◄───│  ┌─────────────┐  ┌──────────────────┐   │    │
│  │   Airflow)  │    │  │  Webserver  │  │    Scheduler     │   │    │
│  └─────────────┘    │  │  :8080      │  │  (DAG Runner)    │   │    │
│                     │  └─────────────┘  └──────────────────┘   │    │
│                     └──────────────────────────────────────────┘    │
│                                    │                                │
│                          ┌─────────▼──────────┐                     │
│                          │   DAG Execution     │                    │
│                          │                     │                    │
│            ┌─────────────▼──────────┐  ┌──────▼─────────────┐       │
│            │  Task 1: fetch_olist   │  │  Task 2: spark_     │      │
│            │  _stream.py            │  │  process_and_load   │      │
│            │                        │  │  _clickhouse        │      │
│            │  CSV → Parquet         │  │                     │      │
│            │  (Data Lake)           │  │  Parquet → Spark    │      │
│            └────────────────────────┘  │  → ClickHouse       │      │
│                                        └──────────┬──────────┘      │
│                                                   │                 │
│                        ┌──────────────────────────▼──────────┐      │
│                        │         ClickHouse Server           │      │
│                        │         analytics.*  :8123 / :9000  │      │
│                        └──────────────────────────┬──────────┘      │
│                                                   │                 │
│                        ┌──────────────────────────▼──────────┐      │
│                        │         Metabase Dashboard          │      │
│                        │         Visualisasi & BI  :3000     │      │
│                        └─────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
```

### Alur Data (Data Flow)

```
Dataset CSV (Lokal)
        │
        ▼
[fetch_olist_stream.py]
  Normalisasi kolom → Parquet
        │
        ▼
 Data Lake (/data_lake/olist/)
  ├── orders/
  ├── order_items/
  ├── order_reviews/
  ├── customers/
  ├── sellers/
  ├── products/
  └── ...
        │
        ▼
[process_olist_spark.py]
  PySpark ETL:
  ├── Enrich Orders (SLA + delay metrics)
  ├── Enrich Items  (seller & customer state)
  ├── Enrich Reviews (delivery context + NLP keyword)
  └── Load ke ClickHouse (11 tabel)
        │
        ▼
 ClickHouse (analytics.*)
        │
        ▼
 Metabase Dashboard
```

---

## Struktur Direktori

```
Code/
├── Dockerfile                      # Image custom Airflow + Java + PySpark
├── docker-compose.yml              # Orchestrasi seluruh service
├── requirements.txt                # Dependensi Python
├── README.md                       
│
├── dags/
│   ├── olist_pipeline_dag.py       # Definisi DAG Airflow
│   ├── dataset/                    # File CSV sumber 
│   │   ├── orders.csv
│   │   ├── order_items.csv
│   │   ├── order_payments.csv
│   │   ├── order_reviews.csv
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   ├── sellers.csv
│   │   ├── geolocation.csv
│   │   ├── mql.csv
│   │   ├── closed_deals.csv
│   │   └── category_translation.csv
│   └── scripts/
│       ├── fetch_olist_stream.py   # Task 1: Ingest CSV → Parquet
│       └── process_olist_spark.py  # Task 2: Spark ETL → ClickHouse
│
└── data_lake/
    └── olist/                      
        ├── orders/
        ├── order_items/
        ├── order_reviews/
        └── ...
```

---

## Teknologi & Dependensi

### Stack Utama

| Komponen | Teknologi | Versi | Port |
|---------|----------|-------|------|
| Workflow Orchestration | Apache Airflow | 2.9.1 | 8080 |
| Distributed Processing | Apache Spark (PySpark) | 3.5.1 | — |
| Data Warehouse | ClickHouse | latest | 8123 / 9000 |
| BI & Visualisasi | Metabase | latest | 3000 |
| Metadata DB (Airflow) | PostgreSQL | 13 | — |
| Containerization | Docker + Compose | 3.8 | — |

### Python Dependencies (`requirements.txt`)

```txt
pyspark==3.5.1          # Distributed data processing engine
clickhouse-driver==0.2.7 # Koneksi Python → ClickHouse (native protocol)
pandas==2.2.1           # DataFrame manipulation & CSV I/O
requests==2.31.0        # HTTP client (utility)
pyarrow==15.0.2         # Parquet read/write backend
```

### Custom Dockerfile

```dockerfile
FROM apache/airflow:2.9.1-python3.11
USER root

# Java diperlukan oleh PySpark sebagai JVM runtime
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
```

> **Catatan**: Java (JRE) di-install di dalam image Airflow karena PySpark membutuhkan JVM untuk menjalankan Spark driver secara lokal.

---

## Cara Menjalankan

### Prasyarat

- Docker Desktop terinstall dan berjalan
- Minimal 8 GB RAM dialokasikan ke Docker
- File dataset CSV sudah ada di `dags/dataset/`

### Langkah-langkah

```bash
# 1. Clone/masuk ke direktori proyek
cd Code/

# 2. Bangun image custom Airflow 
docker-compose build

# 3. Inisialisasi database Airflow
docker-compose run --rm airflow-init

# 4. Jalankan semua service
docker-compose up -d

# 5. Akses Airflow UI
# Buka: http://localhost:8080
# Username: admin | Password: admin

# 6. Aktifkan DAG 'olist_analytics_pipeline' dari UI
# Atau trigger manual via tombol Run

# 7. Akses Metabase Dashboard
# Buka: http://localhost:3000
# Koneksikan ke ClickHouse: host=code-clickhouse-server1, port=8123
```

---

## Arsitektur DAG Airflow

### File: `dags/olist_pipeline_dag.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'fp_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'olist_analytics_pipeline',
    default_args=default_args,
    schedule_interval='@daily',   # Jalan sekali sehari
    catchup=False,
    max_active_runs=1,
    description='Olist E-Commerce: Local CSV → Parquet → Spark → ClickHouse'
) as dag:

    ingest = BashOperator(
        task_id='fetch_olist_csv',
        bash_command='python /opt/airflow/dags/scripts/fetch_olist_stream.py'
    )

    process = BashOperator(
        task_id='spark_process_and_load_clickhouse',
        bash_command='python /opt/airflow/dags/scripts/process_olist_spark.py'
    )

    ingest >> process
```

### Topologi DAG

```
[fetch_olist_csv]  ──►  [spark_process_and_load_clickhouse]
   Task 1                          Task 2
   ~30 detik                       ~5-10 menit
```

### Konfigurasi DAG

| Parameter | Nilai | Penjelasan |
|-----------|-------|-----------|
| `schedule_interval` | `@daily` | Berjalan otomatis setiap hari sekali |
| `catchup` | `False` | Tidak menjalankan ulang run yang terlewat |
| `max_active_runs` | `1` | Hanya 1 run aktif sekaligus (hindari race condition) |
| `retries` | `1` | Otomatis coba ulang jika gagal |
| `retry_delay` | `2 menit` | Jeda sebelum retry |

### Dependency Task

Task **`ingest`** harus selesai sukses sebelum task **`process`** dimulai. Ini penting karena Spark perlu membaca file Parquet yang sudah ditulis oleh task ingest.

---

## Script: fetch_olist_stream.py

**Lokasi**: `dags/scripts/fetch_olist_stream.py`
**Tujuan**: Membaca 11 file CSV dari folder `dataset/` dan mengonversinya ke format Parquet di Data Lake.

### Cara Kerja

```
Dataset CSV (lokal)
        │
        ▼  pd.read_csv()
   DataFrame Pandas
        │
        ▼  Normalisasi kolom (lowercase + replace spasi dengan _)
   Kolom bersih
        │
        ▼  df.to_parquet()
   /data_lake/olist/{table_name}/{table_name}_{timestamp}.parquet
```

### Daftar Tabel yang Di-ingest

| Key | File Sumber |
|-----|------------|
| `orders` | orders.csv |
| `order_items` | order_items.csv |
| `order_payments` | order_payments.csv |
| `order_reviews` | order_reviews.csv |
| `customers` | customers.csv |
| `products` | products.csv |
| `sellers` | sellers.csv |
| `geolocation` | geolocation.csv |
| `mql` | mql.csv |
| `closed_deals` | closed_deals.csv |
| `category_translation` | category_translation.csv |

### Detail Implementasi

- **Normalisasi Kolom**: `col.strip().lower().replace(" ", "_")` — memastikan nama kolom konsisten tanpa spasi atau huruf kapital
- **Timestamp pada Parquet**: Nama file menyertakan `YYYYMMDD_HHMMSS` sehingga setiap run menghasilkan file unik (idempotent)
- **Error Handling**: File yang tidak ditemukan di-skip (warning), error lain akan melempar exception dan menggagalkan task

---

## Script: process_olist_spark.py

**Lokasi**: `dags/scripts/process_olist_spark.py`
**Tujuan**: ETL menggunakan PySpark — membaca Parquet, melakukan feature engineering, lalu memuat ke ClickHouse.

### Inisialisasi Spark Session

```python
spark = SparkSession.builder \
    .appName("Olist_Simple_Pipeline") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

> Spark berjalan dalam mode **local** (bukan cluster) dengan alokasi memori driver 2 GB 

---

### Tahap 1 — Enrich Orders (SLA & Delay Metrics)

Ini adalah tahap paling krusial. Spark menghitung metrik waktu pengiriman dari timestamp mentah.

```
order_purchase_timestamp  ──► approval_time_hours
         │
         ▼ order_approved_at
         │
         ▼ order_delivered_carrier_date
seller_processing_days = (carrier_date - approved_date) / 86400
         │
         ▼ order_delivered_customer_date
carrier_delivery_days = (customer_date - carrier_date) / 86400
         │
total_delivery_days = (customer_date - purchase_date) / 86400
         │
delay_days = total_delivery_days - estimated_days
         │
is_late = 1 jika customer_date > estimated_date (dan status = 'delivered')
```

**Kolom yang dihasilkan:**

| Kolom | Tipe | Penjelasan |
|-------|------|-----------|
| `approval_time_hours` | Float64 | Jeda antara pembelian dan persetujuan (jam) |
| `seller_processing_days` | Float64 | Waktu seller menyiapkan barang (hari) |
| `carrier_delivery_days` | Float64 | Waktu carrier mengantarkan barang (hari) |
| `total_delivery_days` | Float64 | Total waktu pengiriman dari pesan hingga terima (hari) |
| `estimated_days` | Float64 | Estimasi awal yang dijanjikan ke pelanggan (hari) |
| `delay_days` | Float64 | Selisih aktual vs estimasi (positif = terlambat) |
| `is_late` | Int32 | Flag biner: 1 = terlambat, 0 = tepat waktu |
| `order_month` | String | Format `yyyy-MM` untuk agregasi temporal |
| `order_year` | Int32 | Tahun pesanan |

---

### Tahap 2 — Enrich Items (Seller & Customer Context)

```python
df_items_enriched = df_items
    .join(df_orders, "order_id", "left")          # Status & timestamp
    .join(df_customers, "customer_id", "left")     # customer_state, customer_city
    .join(df_sellers, "seller_id", "left")         # seller_state, seller_city
    .join(df_products, "product_id", "left")       # product_category_name
    .join(df_cat, "product_category_name", "left") # Terjemahan kategori (EN)
    .withColumn("category", coalesce(EN_name, PT_name, "unknown"))
    .withColumn("is_same_state", seller_state == customer_state)
```

Kolom `is_same_state` digunakan untuk analisis rute intra-state dengan inter-state.

---

### Tahap 3 — Enrich Reviews (NLP Keyword + Delivery Context)

```python
df_reviews_enriched = df_reviews
    .join(df_orders_enriched, "order_id", "left")  # Gabungkan konteks pengiriman
    .withColumn("is_delivery_complaint",
        when((review_score <= 2) & (is_late == 1), 1).otherwise(0))
    .withColumn("comment_has_delay_keyword",
        when(lower(comment).rlike(
            "atraso|atrasado|demorou|demora|tardou|tarde|"   # Portugis
            "delayed|late|slow|waiting|waited|never arrived"  # Inggris
        ), 1).otherwise(0))
```

**Kolom baru pada reviews:**

| Kolom | Penjelasan |
|-------|-----------|
| `is_delivery_complaint` | Review ≤ 2 bintang DAN pesanan terlambat |
| `comment_has_delay_keyword` | Komentar menyebutkan kata kunci keterlambatan |

---

### Tahap 4 — Load ke ClickHouse (11 Tabel)

Setelah semua DataFrame dienrich, Spark mengonversinya ke Pandas lalu dimasukkan ke ClickHouse melalui `clickhouse-driver`:

```python
client = Client(host='clickhouse-server', user='admin', password='rahasia')
client.execute('CREATE DATABASE IF NOT EXISTS analytics')

# Pola untuk setiap tabel:
# 1. CREATE TABLE IF NOT EXISTS
# 2. TRUNCATE TABLE (full-refresh setiap run)
# 3. INSERT INTO ... VALUES (batch rows)
```

> **Strategi**: Full-refresh (TRUNCATE + INSERT) dipilih karena dataset bersifat statis historis. Ini memastikan idempotency, pipeline bisa dijalankan ulang kapan saja tanpa duplikasi data.

---

### Tahap 5 — Cleanup Parquet

Setelah berhasil dimuat ke ClickHouse, semua file `.parquet` di Data Lake dihapus untuk menghemat disk:

```python
for table_name in TABLE_NAMES:
    files = glob.glob(f"{DATA_LAKE_PATH}/{table_name}/*.parquet")
    for f in files:
        os.remove(f)
```
### Bukti Airflow Berhasil Berjalan
<img width="1918" height="1082" alt="image" src="https://github.com/user-attachments/assets/e88a1aa0-ba83-4817-aeae-1131c3c76fc4" />

---

## Skema ClickHouse

Semua tabel berada di database `analytics`. ClickHouse menggunakan engine **MergeTree** yang dioptimalkan untuk query analitik (OLAP) dengan kecepatan tinggi.

### `analytics.olist_orders`
*Tabel utama dengan metrik SLA pengiriman.*

```sql
CREATE TABLE analytics.olist_orders (
    order_id                        String,
    customer_id                     String,
    order_status                    String,
    order_purchase_timestamp        String,
    order_approved_at               String,
    order_delivered_carrier_date    String,
    order_delivered_customer_date   String,
    order_estimated_delivery_date   String,
    approval_time_hours             Float64,
    seller_processing_days          Float64,   
    carrier_delivery_days           Float64,   
    total_delivery_days             Float64,  
    estimated_days                  Float64,
    delay_days                      Float64,   
    is_late                         Int32,     
    order_month                     String,
    order_year                      Int32
) ENGINE = MergeTree() ORDER BY (order_month, order_id)
```

### `analytics.olist_order_items`
*Item pesanan dengan konteks geografis seller & customer.*

```sql
CREATE TABLE analytics.olist_order_items (
    order_id                String,
    order_item_id           Int32,
    product_id              String,
    category                String,    
    seller_id               String,
    seller_state            String,
    seller_city             String,
    customer_id             String,
    customer_state          String,
    customer_city           String,
    order_status            String,
    price                   Float64,
    freight_value           Float64,   
    shipping_limit_date     String,
    is_same_state           Int32,     
    order_purchase_timestamp String
) ENGINE = MergeTree() ORDER BY (seller_id, order_id)
```

### `analytics.olist_order_reviews`
*Review pelanggan dengan konteks pengiriman & analisis komentar.*

```sql
CREATE TABLE analytics.olist_order_reviews (
    review_id                   String,
    order_id                    String,
    review_score                Int32,     
    review_comment_title        String,
    review_comment_message      String,
    review_creation_date        String,
    review_answer_timestamp     String,
    order_status                String,
    total_delivery_days         Float64,
    estimated_days              Float64,
    delay_days                  Float64,
    is_late                     Int32,
    seller_processing_days      Float64,
    carrier_delivery_days       Float64,
    order_month                 String,
    is_delivery_complaint       Int32,     
    comment_has_delay_keyword   Int32      
) ENGINE = MergeTree() ORDER BY (order_month, review_score)
```

### Tabel Pendukung Lainnya

| Tabel | ORDER BY | Keterangan |
|-------|---------|-----------|
| `olist_order_payments` | `(order_id, payment_sequential)` | Metode & nilai pembayaran |
| `olist_customers` | `customer_id` | Profil & lokasi pelanggan |
| `olist_sellers` | `seller_id` | Profil & lokasi seller |
| `olist_products` | `product_id` | Dimensi & kategori produk |
| `olist_mql` | `mql_id` | Marketing Qualified Leads |
| `olist_closed_deals` | `mql_id` | Deals yang berhasil ditutup |
| `olist_category_translation` | `product_category_name` | Mapping PT → EN |
| `olist_geolocation` | `(state, zip_prefix)` | Koordinat lat/lng per kode pos |

---

## Analitik & Query SQL

Semua query berikut dijalankan langsung di **ClickHouse** dan divisualisasikan melalui **Metabase**.

---

### 1. Distribusi Review Score

**Tujuan**: Melihat sebaran kepuasan pelanggan secara keseluruhan.

```sql
SELECT
    review_score,
    COUNT(*) AS total_reviews,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    CASE review_score
        WHEN 1 THEN 'Sangat Buruk'
        WHEN 2 THEN 'Buruk'
        WHEN 3 THEN 'Cukup'
        WHEN 4 THEN 'Baik'
        WHEN 5 THEN 'Sangat Baik'
    END AS label
FROM analytics.olist_order_reviews
GROUP BY review_score
ORDER BY review_score
```

**Penjelasan Teknis:**
- `SUM(COUNT(*)) OVER ()` adalah **window function** yang menghitung total keseluruhan tanpa GROUP BY, sehingga `percentage` menunjukkan porsi setiap skor terhadap total.

**Visualisasi**:
<img width="778" height="788" alt="Metabase-distribusi_review_score-6_10_2026, 8_45_18 PM" src="https://github.com/user-attachments/assets/231c1669-8d46-448f-b9a8-8a5a85f086d4" />

---

### 2. Korelasi Keterlambatan dengan Review Score

**Tujuan**: Membuktikan apakah pesanan yang terlambat cenderung mendapat review lebih buruk.

```sql
SELECT
    review_score,
    COUNT(*) AS total_reviews,
    ROUND(AVG(delay_days), 1) AS avg_delay_days,
    ROUND(AVG(total_delivery_days), 1) AS avg_delivery_days,
    SUM(is_late) AS total_late_orders,
    ROUND(SUM(is_late) * 100.0 / COUNT(*), 1) AS late_pct
FROM analytics.olist_order_reviews
WHERE order_status = 'delivered'
GROUP BY review_score
ORDER BY review_score
```

**Penjelasan Teknis:**
- `AVG(delay_days)` menghitung rata-rata selisih hari antara aktual dan estimasi — nilai negatif berarti lebih cepat dari estimasi.
- `SUM(is_late) * 100.0 / COUNT(*)` menghitung persentase pesanan terlambat per skor. Diharapkan: semakin rendah skor, semakin tinggi `late_pct`.
- Filter `WHERE order_status = 'delivered'` penting agar hanya pesanan yang sudah selesai yang dihitung.

**Hipotesis yang divalidasi**: Review score 1 seharusnya memiliki `avg_delay_days` tertinggi dan `late_pct` tertinggi.

**Visualisasi**:
<img width="887" height="145" alt="image" src="https://github.com/user-attachments/assets/b8ed811c-b8d2-4bf1-94ae-b5a8fa50d977" />

---

### 3. Breakdown Masalah Review Bintang 1

**Tujuan**: Mengidentifikasi akar masalah dari review paling buruk apakah karena keterlambatan atau faktor lain?

```sql
SELECT
    CASE
        WHEN is_late = 1 AND is_delivery_complaint = 1
            THEN 'Terlambat + Komplain Pengiriman'
        WHEN is_late = 1 AND is_delivery_complaint = 0
            THEN 'Terlambat (Alasan Lain)'
        WHEN comment_has_delay_keyword = 1
            THEN 'Menyebut Keterlambatan'
        ELSE 'Alasan Non-Pengiriman'
    END AS kategori_masalah,
    COUNT(*) AS total
FROM analytics.olist_order_reviews
WHERE review_score = 1
GROUP BY kategori_masalah
ORDER BY total DESC
```

**Penjelasan Teknis:**
- Logika `CASE` bertingkat mengklasifikasikan setiap review bintang 1 ke dalam 4 kategori eksklusif secara berurutan (prioritas pertama yang cocok).
- `is_delivery_complaint` (dari Spark) = 1 hanya jika **sekaligus**: review ≤ 2 bintang dan pesanan terlambat.
- `comment_has_delay_keyword` menangkap keluhan yang tidak tertangkap oleh flag keterlambatan formal (misalnya: estimasi di-update mundur sebelum pengiriman).
- Hasilnya menunjukkan berapa persen review bintang 1 yang **benar-benar disebabkan oleh pengiriman** dengan masalah produk/lainnya.

**Visualisasi**:
<img width="2096" height="844" alt="Metabase-Masalah Review B1-6_10_2026, 8_50_14 PM" src="https://github.com/user-attachments/assets/d39cd015-167b-41d0-a506-4c882d39f5ea" />

---

### 4. Seller vs Carrier: Kontribusi Waktu per Bulan

**Tujuan**: Memahami siapa yang lebih bertanggung jawab atas total waktu pengiriman, seller (proses) atau carrier (transit).

```sql
SELECT
    order_month,
    COUNT(*) AS total_orders,
    ROUND(AVG(seller_processing_days), 1) AS avg_seller_processing_days,
    ROUND(AVG(carrier_delivery_days), 1) AS avg_carrier_days,
    ROUND(
        AVG(seller_processing_days) * 100.0 /
        NULLIF(AVG(total_delivery_days), 0)
    , 1) AS seller_contribution_pct
FROM analytics.olist_orders
WHERE order_status = 'delivered'
  AND order_month != 'unknown'
GROUP BY order_month
ORDER BY order_month
```

**Penjelasan Teknis:**
- `seller_contribution_pct` menghitung berapa persen dari total waktu pengiriman disumbang oleh seller. Misalnya: jika avg seller = 3 hari dan total = 12 hari, maka seller berkontribusi 25%.
- `NULLIF(AVG(total_delivery_days), 0)` mencegah **division by zero** jika ada bulan dengan total hari 0.
- Tren bulan ke bulan berguna untuk melihat apakah masalah memburuk atau membaik seiring waktu.

**Visualisasi**: 
<img width="2096" height="1028" alt="Metabase-SELLER VS CARRIER PER BULAN-6_10_2026, 8_50_04 PM" src="https://github.com/user-attachments/assets/7c94c690-787f-4132-968e-eb0dc7c819b2" />

---

### 5. Rasio Keterlambatan per Bulan

**Tujuan**: Melacak tren rasio pesanan terlambat dari waktu ke waktu.

```sql
SELECT
    order_month,
    COUNT(*) AS total_orders,
    SUM(is_late) AS late_orders,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_late = 1 THEN order_id END) * 100.0 /
        COUNT(DISTINCT order_id)
    , 1) AS late_rate_pct
FROM analytics.olist_orders
WHERE order_status = 'delivered'
  AND order_month != 'unknown'
GROUP BY order_month
ORDER BY order_month
```

**Penjelasan Teknis:**
- Menggunakan `COUNT(DISTINCT CASE WHEN is_late = 1 THEN order_id END)` untuk menghitung **pesanan unik** yang terlambat, bukan baris.
- Dibagi dengan `COUNT(DISTINCT order_id)` — total pesanan unik per bulan.
- Ini lebih akurat daripada `SUM(is_late) / COUNT(*)` karena satu order bisa memiliki banyak item (join ke order_items bisa menghasilkan duplikasi).

**Visualisasi**:
<img width="2096" height="660" alt="Metabase-rasio telat perbulan-6_10_2026, 8_50_23 PM" src="https://github.com/user-attachments/assets/643f854e-b1d7-4640-a0d1-7593fde61cea" />

---

### 6. Perbandingan Seller vs Carrier (Statistik Deskriptif)

**Tujuan**: Membandingkan distribusi waktu seller processing vs carrier transit secara komprehensif.

```sql
SELECT
    'Seller Processing Time' AS komponen,
    ROUND(AVG(seller_processing_days), 1) AS rata_rata_hari,
    ROUND(quantile(0.50)(seller_processing_days), 1) AS median_hari,
    ROUND(quantile(0.90)(seller_processing_days), 1) AS p90_hari,
    ROUND(MAX(seller_processing_days), 0) AS maksimum_hari,
    CASE
        WHEN ROUND(quantile(0.90)(seller_processing_days), 1) > 5
        THEN 'Perlu Perhatian'
        ELSE 'Normal'
    END AS status_p90
FROM analytics.olist_orders
WHERE order_status = 'delivered'
  AND seller_processing_days > 0

UNION ALL

SELECT
    'Carrier Transit Time',
    ROUND(AVG(carrier_delivery_days), 1),
    ROUND(quantile(0.50)(carrier_delivery_days), 1),
    ROUND(quantile(0.90)(carrier_delivery_days), 1),
    ROUND(MAX(carrier_delivery_days), 0),
    CASE
        WHEN ROUND(quantile(0.90)(carrier_delivery_days), 1) > 5
        THEN 'Perlu Perhatian'
        ELSE 'Normal'
    END
FROM analytics.olist_orders
WHERE order_status = 'delivered'
  AND carrier_delivery_days > 0
```

**Penjelasan Teknis:**
- `quantile(0.50)` dan `quantile(0.90)` adalah fungsi agregat ClickHouse untuk menghitung persentil ke-50 (median) dan ke-90. Persentil ke-90 (P90) berarti "90% pesanan selesai dalam waktu ini" — metrik SLA yang umum digunakan.
- `status_p90 > 5 hari` adalah ambang batas kritis: jika P90 melebihi 5 hari, komponen tersebut perlu perhatian.
- `UNION ALL` menggabungkan dua set hasil menjadi satu tabel perbandingan yang mudah dibaca.
- Filter `> 0` menghilangkan data anomali (nilai negatif atau nol akibat data timestamp yang tidak lengkap).

**Visualisasi**:
<img width="924" height="73" alt="image" src="https://github.com/user-attachments/assets/d1091a77-61d3-4281-891d-82af06768c6e" />

---

### 7. Late Rate per State (Kontribusi Nasional)

**Tujuan**: Mengidentifikasi negara bagian yang paling berkontribusi terhadap masalah keterlambatan nasional.

```sql
SELECT
    i.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.is_late) * 100.0 / COUNT(*), 1) AS late_rate_pct_WRONG,  
    ROUND(
        COUNT(DISTINCT CASE WHEN o.is_late = 1 THEN o.order_id END) * 100.0 /
        COUNT(DISTINCT o.order_id)
    , 1) AS late_rate_pct,                                                
    ROUND(
        COUNT(DISTINCT CASE WHEN o.is_late = 1 THEN o.order_id END) * 100.0 /
        SUM(COUNT(DISTINCT CASE WHEN o.is_late = 1 THEN o.order_id END)) OVER ()
    , 2) AS contribution_to_national_late_pct,
    ROUND(
        AVG(o.total_delivery_days) - AVG(AVG(o.total_delivery_days)) OVER ()
    , 1) AS diff_vs_national_avg_days
FROM analytics.olist_orders o
JOIN analytics.olist_order_items i ON o.order_id = i.order_id
WHERE o.order_status = 'delivered'
GROUP BY i.customer_state
ORDER BY contribution_to_national_late_pct DESC
```

**Penjelasan Teknis:**
- **`late_rate_pct_WRONG`** (sengaja ditampilkan): Formula ini keliru karena tabel di-join dengan `order_items`, yang bisa menyebabkan satu `order_id` muncul beberapa kali (satu order bisa punya banyak item). `SUM(is_late) / COUNT(*)` terhitung berulang.
- **`late_rate_pct`** (benar): Menggunakan `COUNT(DISTINCT ...)` untuk memastikan setiap order dihitung sekali, terlepas dari berapa banyak item yang dimilikinya.
- **`contribution_to_national_late_pct`**: Window function membagi keterlambatan di state ini dengan total keterlambatan nasional. Ini lebih informatif daripada late rate lokal karena state kecil bisa punya late rate tinggi namun kontribusi nasionalnya kecil.
- **`diff_vs_national_avg_days`**: Selisih rata-rata pengiriman state ini dengan rata-rata nasional. Nilai positif = lebih lambat dari nasional.

**Visualisasi**:
<img width="2096" height="1120" alt="Metabase-late rate state-6_10_2026, 8_50_45 PM" src="https://github.com/user-attachments/assets/0e1b8e4b-9872-4da2-8f43-5770bdbc18a5" />

---

### 8. Breakdown Waktu Pengiriman per State

**Tujuan**: Melihat rincian waktu seller processing, carrier transit, dan hubungannya dengan review score per state.

```sql
SELECT
    i.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(AVG(o.seller_processing_days), 1) AS avg_seller_processing,
    ROUND(AVG(o.carrier_delivery_days), 1) AS avg_carrier_days,
    ROUND(AVG(o.total_delivery_days), 1) AS avg_total_days,
    ROUND(SUM(o.is_late) * 100.0 / COUNT(*), 1) AS late_rate_pct,
    ROUND(AVG(r.review_score), 2) AS avg_review_score
FROM analytics.olist_orders o
JOIN analytics.olist_order_items i ON o.order_id = i.order_id
LEFT JOIN analytics.olist_order_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY i.customer_state
ORDER BY late_rate_pct DESC
```

**Penjelasan Teknis:**
- `LEFT JOIN` ke reviews memastikan pesanan tanpa review tetap masuk — `avg_review_score` akan NULL untuk state yang pesanannya tidak ada review-nya.
- Mengurutkan berdasarkan `late_rate_pct DESC` memudahkan identifikasi state terburuk di baris teratas.

**Visualisasi**:
<img width="1130" height="673" alt="image" src="https://github.com/user-attachments/assets/f3455955-d50d-42e7-a1ea-2b2c7e33b94f" />

---

### 9. Rute Intra-State vs Inter-State (Fokus São Paulo)

**Tujuan**: Membuktikan apakah pesanan yang melintasi batas negara bagian lebih lama dan lebih sering terlambat.

```sql
SELECT
    CASE
        WHEN i.seller_state = 'SP' AND i.customer_state = 'SP'
            THEN 'SP → SP (Intra SP)'
        WHEN i.seller_state = 'SP' AND i.customer_state != 'SP'
            THEN 'SP → Luar SP'
        WHEN i.seller_state != 'SP' AND i.customer_state = 'SP'
            THEN 'Luar SP → SP'
        ELSE 'Non-SP → Non-SP'
    END AS rute_pengiriman,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(AVG(o.seller_processing_days), 1) AS avg_seller_processing,
    ROUND(AVG(o.carrier_delivery_days), 1) AS avg_carrier_days,
    ROUND(
        COUNT(DISTINCT CASE WHEN o.is_late = 1 THEN o.order_id END) * 100.0 /
        COUNT(DISTINCT o.order_id)
    , 1) AS late_rate_pct,
    ROUND(AVG(r.review_score), 2) AS avg_review_score
FROM analytics.olist_orders o
JOIN analytics.olist_order_items i ON o.order_id = i.order_id
LEFT JOIN analytics.olist_order_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY rute_pengiriman
ORDER BY late_rate_pct DESC
```

**Penjelasan Teknis:**
- São Paulo (SP) dipilih karena merupakan hub e-commerce terbesar di Brasil, sebagian besar seller berlokasi di SP.
- Empat rute memungkinkan perbandingan yang bersih antara pengiriman lokal (intra-SP) vs jarak jauh.
- Hasilnya diharapkan menunjukkan: `carrier_delivery_days` jauh lebih tinggi untuk rute yang melintasi state, menandakan carrier (bukan seller) yang menjadi bottleneck untuk pengiriman jarak jauh.

**Visualisasi**:
<img width="2096" height="936" alt="Metabase-intra-state vs inner-state-6_10_2026, 8_51_04 PM" src="https://github.com/user-attachments/assets/15090656-2f61-49c8-81cc-e47ce8e419f0" />

---

### 10. Seller Bermasalah (Top 50 Kontributor Keterlambatan)

**Tujuan**: Mengidentifikasi seller dengan kontribusi keterlambatan nasional tertinggi beserta klasi kasifikasi SLA-nya.

```sql
SELECT
    i.seller_id,
    i.seller_state,
    i.seller_city,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(AVG(o.seller_processing_days), 1) AS avg_processing_days,
    ROUND(
        AVG(o.seller_processing_days) -
        AVG(AVG(o.seller_processing_days)) OVER ()
    , 1) AS excess_vs_avg,
    ROUND(
        COUNT(DISTINCT CASE WHEN o.is_late = 1 THEN o.order_id END) * 100.0 /
        COUNT(DISTINCT o.order_id)
    , 1) AS late_rate_pct,
    ROUND(
        SUM(o.is_late) * 100.0 /
        SUM(SUM(o.is_late)) OVER ()
    , 2) AS contribution_national_late_pct,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    CASE
        WHEN AVG(o.seller_processing_days) <= 1 THEN 'Fast'
        WHEN AVG(o.seller_processing_days) <= 3 THEN 'Normal'
        WHEN AVG(o.seller_processing_days) <= 7 THEN 'Slow'
        ELSE 'Critical'
    END AS sla_status,
    CASE
        WHEN COUNT(DISTINCT o.order_id) >= 500 THEN 'High Volume'
        WHEN COUNT(DISTINCT o.order_id) >= 100 THEN 'Medium Volume'
        ELSE 'Low Volume'
    END AS seller_tier
FROM analytics.olist_orders o
JOIN analytics.olist_order_items i ON o.order_id = i.order_id
LEFT JOIN analytics.olist_order_reviews r ON o.order_id = r.order_id
WHERE o.order_status IN ('delivered', 'shipped')
  AND o.seller_processing_days >= 0
GROUP BY i.seller_id, i.seller_state, i.seller_city
HAVING COUNT(DISTINCT o.order_id) >= 50
ORDER BY contribution_national_late_pct DESC
LIMIT 50
```

**Penjelasan Teknis:**
- **`excess_vs_avg`**: Selisih rata-rata processing seller ini vs rata-rata semua seller (window function). Nilai positif berarti seller ini lebih lambat dari rata-rata nasional.
- **`contribution_national_late_pct`**: `SUM(SUM(is_late)) OVER ()` adalah nested window function, menjumlahkan total keterlambatan dari seluruh seller, lalu membagi keterlambatan seller ini dengan total tersebut.
- **`HAVING COUNT >= 50`**: Filter minimum volume untuk menghindari seller dengan hanya beberapa order tapi kebetulan semuanya terlambat (tidak representatif).
- **`sla_status`**: Klasifikasi berdasarkan rata-rata hari processing — ≤1 hari (Fast), 1-3 hari (Normal), 3-7 hari (Slow), >7 hari (Critical).
- Filter `seller_processing_days >= 0` menghilangkan anomali data (nilai negatif berarti carrier_date sebelum approved_date, kemungkinan error data).

**Visualisasi**:
<img width="2096" height="1120" alt="Metabase-Siapa seller bermasalah-6_10_2026, 8_51_15 PM" src="https://github.com/user-attachments/assets/1b51e7d1-700a-417d-8ad9-07ca567c29dd" />

---

### 11. Analisis Kerugian Finansial per Seller

**Tujuan**: Mengukur biaya finansial (freight yang "terbuang") akibat keterlambatan per seller.

```sql
SELECT
    i.seller_id,
    i.seller_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(AVG(o.seller_processing_days), 1) AS avg_processing_days,
    ROUND(
        COUNT(DISTINCT CASE WHEN o.is_late = 1 THEN o.order_id END) * 100.0 /
        COUNT(DISTINCT o.order_id)
    , 1) AS late_rate_pct,
    ROUND(SUM(i.price), 2) AS total_revenue,
    ROUND(SUM(i.freight_value), 2) AS total_freight_charged,
    ROUND(
        SUM(CASE WHEN o.is_late = 1 THEN i.freight_value ELSE 0 END)
    , 2) AS freight_on_late_orders,
    ROUND(
        SUM(CASE WHEN o.is_late = 1 THEN i.freight_value ELSE 0 END) * 100.0 /
        NULLIF(SUM(i.freight_value), 0)
    , 1) AS pct_freight_wasted,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    SUM(CASE WHEN r.review_score = 1 THEN 1 ELSE 0 END) AS total_1star
FROM analytics.olist_orders o
JOIN analytics.olist_order_items i ON o.order_id = i.order_id
LEFT JOIN analytics.olist_order_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY i.seller_id, i.seller_state
HAVING COUNT(DISTINCT o.order_id) >= 50
   AND AVG(o.seller_processing_days) > 2
   AND COUNT(DISTINCT CASE WHEN o.is_late = 1 THEN o.order_id END) * 100.0 /
       COUNT(DISTINCT o.order_id) > 20
ORDER BY freight_on_late_orders DESC
LIMIT 20
```

**Penjelasan Teknis:**
- `freight_on_late_orders`: Total ongkir yang dibayar pelanggan untuk pesanan yang justru terlambat. Ini adalah "biaya ketidakefisienan" — pelanggan membayar ongkir tapi pengiriman tetap terlambat.
- `pct_freight_wasted`: Persentase total ongkir yang berkaitan dengan pesanan terlambat. Seller dengan pct_freight_wasted tinggi = banyak ongkir yang "terbuang" karena gagal tepat waktu.
- `HAVING` clause memfilter hanya seller yang memiliki **kombinasi** masalah: volume cukup (≥50 order), slow processing (>2 hari), dan late rate tinggi (>20%).

**Visualisasi**:
<img width="1970" height="241" alt="image" src="https://github.com/user-attachments/assets/274f80c7-4587-48b3-b893-6b64109fa348" />

---

### 12. Rincian Distribusi Kerugian Freight (Bucket Analysis)

**Tujuan**: Mengelompokkan seller berdasarkan besar kecilnya freight yang terbuang.

```sql
SELECT
    kategori_kerugian,
    COUNT(*) AS jumlah_seller,
    SUM(freight_wasted) AS total_freight_wasted
FROM (
    SELECT
        i.seller_id,
        SUM(CASE WHEN o.is_late = 1 THEN i.freight_value ELSE 0 END) AS freight_wasted,
        CASE
            WHEN SUM(CASE WHEN o.is_late = 1 THEN i.freight_value ELSE 0 END) > 5000
                THEN 'Besar (>5000)'
            WHEN SUM(CASE WHEN o.is_late = 1 THEN i.freight_value ELSE 0 END) > 1000
                THEN 'Sedang (1000-5000)'
            ELSE 'Kecil (<1000)'
        END AS kategori_kerugian
    FROM analytics.olist_orders o
    JOIN analytics.olist_order_items i ON o.order_id = i.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY i.seller_id
    HAVING AVG(o.seller_processing_days) > 2
       AND COUNT(DISTINCT o.order_id) >= 50
) seller_bucket
GROUP BY kategori_kerugian
ORDER BY total_freight_wasted DESC
```

**Penjelasan Teknis:**
- Subquery mengklasifikasikan setiap seller ke dalam bucket berdasarkan total freight yang berkaitan dengan keterlambatan.
- Query luar melakukan agregasi per bucket berapa seller di setiap kategori dan berapa total freight wasted-nya.
- Pola ini (**bucket analysis**) berguna untuk prioritisasi: fokus pada seller di kategori "Besar" terlebih dahulu.

**Visualisasi**:
<img width="1018" height="121" alt="image" src="https://github.com/user-attachments/assets/2cb5ae4b-ee6f-4f56-8fdc-6dc568694728" />

---

### 13. Distribusi SLA Seller

**Tujuan**: Memahami sebaran performa seller berdasarkan kategori SLA dan dampaknya terhadap late rate & review.

```sql
SELECT
    sla_category,
    COUNT(DISTINCT seller_id) AS jumlah_seller,
    SUM(total_orders) AS total_orders_in_category,
    ROUND(
        SUM(total_orders) * 100.0 /
        SUM(SUM(total_orders)) OVER ()
    , 1) AS order_share_pct,
    ROUND(AVG(late_rate_pct), 1) AS avg_late_rate,
    ROUND(AVG(avg_review_score), 2) AS avg_review_score
FROM (
    SELECT
        i.seller_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        ROUND(AVG(o.seller_processing_days), 2) AS avg_processing_days,
        ROUND(SUM(o.is_late) * 100.0 / COUNT(*), 1) AS late_rate_pct,
        ROUND(AVG(r.review_score), 2) AS avg_review_score,
        CASE
            WHEN AVG(o.seller_processing_days) <= 1 THEN 'Fast (≤1 hari)'
            WHEN AVG(o.seller_processing_days) <= 3 THEN 'Normal (1-3 hari)'
            WHEN AVG(o.seller_processing_days) <= 7 THEN 'Slow (3-7 hari)'
            ELSE 'Critical (>7 hari)'
        END AS sla_category
    FROM analytics.olist_orders o
    JOIN analytics.olist_order_items i ON o.order_id = i.order_id
    LEFT JOIN analytics.olist_order_reviews r ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
      AND o.seller_processing_days >= 0
    GROUP BY i.seller_id
    HAVING COUNT(DISTINCT o.order_id) >= 10   -- Minimum 10 order agar representatif
) seller_summary
GROUP BY sla_category
ORDER BY
    CASE sla_category
        WHEN 'Fast (≤1 hari)'    THEN 1
        WHEN 'Normal (1-3 hari)' THEN 2
        WHEN 'Slow (3-7 hari)'   THEN 3
        ELSE 4
    END
```

**Penjelasan Teknis:**
- Menggunakan **nested subquery** (bukan CTE) — subquery dalam menghitung metrik per seller, subquery luar mengagregasi per kategori SLA.
- `ORDER BY CASE ... END` adalah custom sort order — mengurutkan secara logis (Fast → Normal → Slow → Critical.
- `HAVING COUNT >= 10` di subquery dalam lebih longgar dibanding query lain (≥50) karena tujuannya adalah distribusi populasi seller secara menyeluruh.

**Visualisasi**:
<img width="1018" height="121" alt="image" src="https://github.com/user-attachments/assets/f2a39161-ba0a-4eba-94cf-f0801bebe2fa" />
---

### 14. Cost of Inefficiency — Prioritas Intervensi Seller

**Tujuan**: Tabel prioritisasi komprehensif yang menggabungkan seluruh metrik untuk menentukan seller mana yang paling perlu diintervensi.

```sql
SELECT
    i.seller_id,
    i.seller_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.is_late) AS late_orders,
    ROUND(SUM(o.is_late) * 100.0 / COUNT(*), 1) AS late_rate_pct,
    ROUND(AVG(o.seller_processing_days), 1) AS avg_processing_days,
    ROUND(AVG(o.seller_processing_days) - 2, 1) AS excess_days_vs_sla,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    SUM(CASE WHEN r.review_score = 1 THEN 1 ELSE 0 END) AS total_bintang_1,
    ROUND(
        SUM(CASE WHEN o.is_late = 1 THEN i.freight_value ELSE 0 END)
    , 2) AS freight_paid_on_late_orders,
    ROUND(
        SUM(CASE WHEN o.is_late = 1 THEN i.freight_value ELSE 0 END) * 100.0 /
        NULLIF(SUM(i.freight_value), 0)
    , 1) AS pct_freight_on_late_orders,
    ROUND(SUM(i.price), 2) AS total_revenue,
    CASE
        WHEN AVG(o.seller_processing_days) > 7
             AND SUM(o.is_late) * 100.0 / COUNT(*) > 30
            THEN 'Intervensi Segera'
        WHEN AVG(o.seller_processing_days) > 3
             AND SUM(o.is_late) * 100.0 / COUNT(*) > 20
            THEN 'Perlu Monitoring'
        ELSE 'Pantau Berkala'
    END AS prioritas_intervensi
FROM analytics.olist_orders o
JOIN analytics.olist_order_items i ON o.order_id = i.order_id
LEFT JOIN analytics.olist_order_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY i.seller_id, i.seller_state
HAVING COUNT(DISTINCT o.order_id) >= 50
   AND ROUND(SUM(o.is_late) * 100.0 / COUNT(*), 1) > 20
   AND AVG(o.seller_processing_days) > 2
ORDER BY freight_paid_on_late_orders DESC
LIMIT 20
```

**Penjelasan Teknis:**
- `excess_days_vs_sla`: Mengasumsikan SLA ideal = 2 hari. Nilai positif menunjukkan berapa hari seller melebihi SLA rata-rata.
- **Matriks Prioritas** (kolom `prioritas_intervensi`):
  - **Intervensi Segera**: Processing >7 hari DAN late rate >30% — krisis operasional
  - **Perlu Monitoring**: Processing >3 hari DAN late rate >20% — butuh perhatian
  - **Pantau Berkala**: Masalah ada tapi belum kritis

**Visualisasi**:
<img width="2397" height="289" alt="image" src="https://github.com/user-attachments/assets/aab774fb-c1fa-46dd-91fd-149c4e4a4200" />

---

## Dashboard Metabase

### Cara Koneksi Metabase ke ClickHouse

1. Buka Metabase di `http://localhost:3000`
2. Masuk ke **Admin Settings → Databases → Add Database**
3. Pilih driver **ClickHouse** (atau gunakan HTTP interface)
4. Konfigurasi:
   ```
   Host     : code-clickhouse-server1
   Port     : 8123
   Database : analytics
   Username : admin
   Password : rahasia
   ```
http://localhost:3000/public/dashboard/8b248429-69b9-4d58-920e-02dcec23e3b8

## Insights 

### Temuan 1: Keterlambatan = Review Buruk
Review bintang 1 memiliki rata-rata `delay_days` tertinggi dan `late_pct` tertinggi. Ini mengkonfirmasi bahwa **pengiriman yang terlambat adalah masalah utama ketidakpuasan pelanggan**.

### Temuan 2: Seller Processing sebagai Bottleneck
Berdasarkan analisis P90, waktu seller processing seringkali melebihi SLA 3 hari — kontribusi seller terhadap total waktu pengiriman lebih besar dari yang diharapkan, terutama untuk seller kategori "Slow" dan "Critical".

### Temuan 3: Jarak Geografis Memengaruhi Keterlambatan
Pengiriman dari São Paulo (pusat seller) ke daerah terpencil seperti Nordeste (RN, PB, AL) memiliki `carrier_delivery_days` jauh lebih tinggi. Carrier — bukan seller — menjadi bottleneck untuk pengiriman jarak jauh.

### Temuan 4: Konsentrasi Masalah pada Segelintir Seller
20 seller teratas menyumbang porsi yang tidak proporsional terhadap total keterlambatan nasional. Intervensi terarah pada seller-seller ini bisa memberikan dampak signifikan terhadap keseluruhan performa platform.

### Temuan 5: Freight yang Terbuang sebagai Kerugian Tersembunyi
Pelanggan tetap membayar ongkir meski pesanan terlambat. Ini menciptakan "reputational cost" yang tersembunyi — pelanggan merasa membayar untuk layanan yang gagal.

---

