import pandas as pd
import os
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# KONFIGURASI — sesuaikan path folder dataset lokal
# ─────────────────────────────────────────────────────────────
DATASET_PATH = "/opt/airflow/dags/dataset"
DATA_LAKE_PATH = "/opt/airflow/data_lake/olist"

# Daftar semua file CSV dan nama tabelnya
TABLES = {
    "orders":               "orders.csv",
    "order_items":          "order_items.csv",
    "order_payments":       "order_payments.csv",
    "order_reviews":        "order_reviews.csv",
    "customers":            "customers.csv",
    "products":             "products.csv",
    "sellers":              "sellers.csv",
    "geolocation":          "geolocation.csv",
    "mql":                  "mql.csv",
    "closed_deals":         "closed_deals.csv",
    "category_translation": "category_translation.csv",
}

def fetch_olist():
    print("Membuka keran data: Dataset Olist (Local CSV)...")
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    success_count = 0

    for table_name, filename in TABLES.items():
        filepath = os.path.join(DATASET_PATH, filename)

        try:
            print(f"  Membaca {filename}...")
            df = pd.read_csv(filepath)

            # Normalisasi nama kolom
            df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

            # Simpan ke Data Lake
            output_dir = os.path.join(DATA_LAKE_PATH, table_name)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{table_name}_{current_time}.parquet")
            df.to_parquet(output_path, index=False)

            print(f"  ✅ {table_name}: {len(df):,} baris → {output_path}")
            success_count += 1

        except FileNotFoundError:
            print(f"  ⚠️  File tidak ditemukan: {filepath} — skip")
        except Exception as e:
            print(f"  ❌ Gagal memproses {filename}: {e}")
            raise

    print(f"\n✅ Selesai! {success_count}/{len(TABLES)} tabel berhasil disimpan ke Data Lake.")

if __name__ == "__main__":
    fetch_olist()