from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from clickhouse_driver import Client
import os
import glob

DATA_LAKE_PATH = "/opt/airflow/data_lake/olist"

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("Olist_Simple_Pipeline") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Membaca seluruh data dari Data Lake...")

    # BACA SEMUA TABEL
    df_orders    = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/orders/")
    df_items     = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/order_items/")
    df_payments  = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/order_payments/")
    df_reviews   = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/order_reviews/")
    df_customers = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/customers/")
    df_products  = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/products/")
    df_sellers   = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/sellers/")
    df_mql       = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/mql/")
    df_deals     = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/closed_deals/")
    df_cat       = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/category_translation/")

    # ENRICH ORDERS: tambahkan kolom SLA & delay 
    print("Enriching orders dengan SLA calculation...")
    df_orders_enriched = df_orders \
        .withColumn("purchase_ts",
                    F.col("order_purchase_timestamp").cast(TimestampType())) \
        .withColumn("approved_ts",
                    F.col("order_approved_at").cast(TimestampType())) \
        .withColumn("carrier_ts",
                    F.col("order_delivered_carrier_date").cast(TimestampType())) \
        .withColumn("customer_ts",
                    F.col("order_delivered_customer_date").cast(TimestampType())) \
        .withColumn("estimated_ts",
                    F.col("order_estimated_delivery_date").cast(TimestampType())) \
        .withColumn("approval_time_hours",
                    F.round((F.unix_timestamp("approved_ts") -
                             F.unix_timestamp("purchase_ts")) / 3600, 1)) \
        .withColumn("seller_processing_days",
                    F.round((F.unix_timestamp("carrier_ts") -
                             F.unix_timestamp("approved_ts")) / 86400, 1)) \
        .withColumn("carrier_delivery_days",
                    F.round((F.unix_timestamp("customer_ts") -
                             F.unix_timestamp("carrier_ts")) / 86400, 1)) \
        .withColumn("total_delivery_days",
                    F.round((F.unix_timestamp("customer_ts") -
                             F.unix_timestamp("purchase_ts")) / 86400, 1)) \
        .withColumn("estimated_days",
                    F.round((F.unix_timestamp("estimated_ts") -
                             F.unix_timestamp("purchase_ts")) / 86400, 1)) \
        .withColumn("delay_days",
                    F.round(F.col("total_delivery_days") -
                            F.col("estimated_days"), 1)) \
        .withColumn("is_late",
                    F.when(
                        (F.col("order_status") == "delivered") &
                        (F.col("customer_ts") > F.col("estimated_ts")),
                        1).otherwise(0)) \
        .withColumn("order_month",
                    F.date_format(F.col("purchase_ts"), "yyyy-MM")) \
        .withColumn("order_year", F.year(F.col("purchase_ts"))) \
        .select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "approval_time_hours", "seller_processing_days",
            "carrier_delivery_days", "total_delivery_days",
            "estimated_days", "delay_days", "is_late",
            "order_month", "order_year"
        )

    # ENRICH ITEMS: tambahkan seller & customer state
    print("Enriching items dengan seller & customer info...")
    df_items_enriched = df_items \
        .join(df_orders.select("order_id", "customer_id", "order_status",
                               "order_purchase_timestamp"), "order_id", "left") \
        .join(df_customers.select("customer_id", "customer_state",
                                  "customer_city"), "customer_id", "left") \
        .join(df_sellers.select("seller_id", "seller_state",
                                "seller_city"), "seller_id", "left") \
        .join(df_products.select("product_id", "product_category_name"),
              "product_id", "left") \
        .join(df_cat, "product_category_name", "left") \
        .withColumn("category",
                    F.coalesce(F.col("product_category_name_english"),
                               F.col("product_category_name"),
                               F.lit("unknown"))) \
        .withColumn("is_same_state",
                    F.when(F.col("seller_state") == F.col("customer_state"),
                           1).otherwise(0)) \
        .select(
            "order_id", "order_item_id", "product_id", "category",
            "seller_id", "seller_state", "seller_city",
            "customer_id", "customer_state", "customer_city",
            "order_status", "price", "freight_value",
            "shipping_limit_date", "is_same_state",
            "order_purchase_timestamp"
        ) \
        .fillna({"seller_state": "unknown", "seller_city": "unknown",
                 "customer_state": "unknown", "customer_city": "unknown",
                 "category": "unknown"})

    # ENRICH REVIEWS
    print("Enriching reviews dengan delivery context...")
    df_reviews_enriched = df_reviews \
        .join(df_orders_enriched.select(
            "order_id", "order_status", "total_delivery_days",
            "estimated_days", "delay_days", "is_late",
            "seller_processing_days", "carrier_delivery_days", "order_month"),
            "order_id", "left") \
        .withColumn("is_delivery_complaint",
                    F.when((F.col("review_score") <= 2) &
                           (F.col("is_late") == 1), 1).otherwise(0)) \
        .withColumn("comment_has_delay_keyword",
                    F.when(
                        F.lower(F.col("review_comment_message")).rlike(
                            "atraso|atrasado|demorou|demora|tardou|tarde|"
                            "delayed|late|slow|waiting|waited|never arrived"
                        ), 1).otherwise(0)) \
        .fillna({
            "review_comment_title": "", "review_comment_message": "",
            "total_delivery_days": 0.0, "estimated_days": 0.0,
            "delay_days": 0.0, "is_late": 0, "seller_processing_days": 0.0,
            "carrier_delivery_days": 0.0, "order_month": "unknown",
            "order_status": "unknown", "is_delivery_complaint": 0,
            "comment_has_delay_keyword": 0
        })

    # KONVERSI KE PANDAS 
    print("\nKonversi ke Pandas...")
    orders_pd    = df_orders_enriched.toPandas()
    items_pd     = df_items_enriched.toPandas()
    reviews_pd   = df_reviews_enriched.toPandas()
    payments_pd  = df_payments.toPandas()
    customers_pd = df_customers.toPandas()
    products_pd  = df_products.fillna("unknown").toPandas()
    sellers_pd   = df_sellers.toPandas()
    mql_pd       = df_mql.toPandas()
    deals_pd     = df_deals.toPandas()
    cat_pd       = df_cat.toPandas()
    # Geolocation: deduplicate per zip code prefix agar tidak terlalu besar
    print("  Deduplicating geolocation...")
    df_geo  = spark.read.parquet(f"file:///{DATA_LAKE_PATH}/geolocation/")
    geo_pd  = df_geo.dropDuplicates(["geolocation_zip_code_prefix"]).toPandas()

    spark.stop()
    print("Spark session ditutup.")

    # LOAD KE CLICKHOUSE 
    print("\nMemuat ke ClickHouse Warehouse...")
    client = Client(host='clickhouse-server', user='admin', password='rahasia')
    client.execute('CREATE DATABASE IF NOT EXISTS analytics')

    # TABEL 1: orders (enriched) 
    print("  Loading olist_orders...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_orders (
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
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_orders')
    orders_pd = orders_pd.fillna({
        'approval_time_hours': 0.0, 'seller_processing_days': 0.0,
        'carrier_delivery_days': 0.0, 'total_delivery_days': 0.0,
        'estimated_days': 0.0, 'delay_days': 0.0, 'is_late': 0,
        'order_approved_at': '', 'order_delivered_carrier_date': '',
        'order_delivered_customer_date': '', 'order_year': 0
    })
    cols = ['order_id','customer_id','order_status',
            'order_purchase_timestamp','order_approved_at',
            'order_delivered_carrier_date','order_delivered_customer_date',
            'order_estimated_delivery_date',
            'approval_time_hours','seller_processing_days',
            'carrier_delivery_days','total_delivery_days',
            'estimated_days','delay_days','is_late','order_month','order_year']
    rows = [tuple(r) for r in orders_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_orders VALUES', rows)
    print(f" olist_orders ({len(rows):,} baris)")

    # TABEL 2: order_items (enriched) 
    print("  Loading olist_order_items...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_order_items (
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
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_order_items')
    cols = ['order_id','order_item_id','product_id','category',
            'seller_id','seller_state','seller_city',
            'customer_id','customer_state','customer_city',
            'order_status','price','freight_value',
            'shipping_limit_date','is_same_state','order_purchase_timestamp']
    rows = [tuple(r) for r in items_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_order_items VALUES', rows)
    print(f" olist_order_items ({len(rows):,} baris)")

    # TABEL 3: order_reviews (enriched) 
    print("  Loading olist_order_reviews...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_order_reviews (
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
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_order_reviews')
    cols = ['review_id','order_id','review_score',
            'review_comment_title','review_comment_message',
            'review_creation_date','review_answer_timestamp',
            'order_status','total_delivery_days','estimated_days',
            'delay_days','is_late','seller_processing_days',
            'carrier_delivery_days','order_month',
            'is_delivery_complaint','comment_has_delay_keyword']
    rows = [tuple(r) for r in reviews_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_order_reviews VALUES', rows)
    print(f"  olist_order_reviews ({len(rows):,} baris)")

    # TABEL 4: order_payments 
    print("  Loading olist_order_payments...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_order_payments (
            order_id              String,
            payment_sequential    Int32,
            payment_type          String,
            payment_installments  Int32,
            payment_value         Float64
        ) ENGINE = MergeTree() ORDER BY (order_id, payment_sequential)
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_order_payments')
    cols = ['order_id','payment_sequential','payment_type',
            'payment_installments','payment_value']
    rows = [tuple(r) for r in payments_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_order_payments VALUES', rows)
    print(f" olist_order_payments ({len(rows):,} baris)")

    # TABEL 5: customers 
    print("  Loading olist_customers...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_customers (
            customer_id               String,
            customer_unique_id        String,
            customer_zip_code_prefix  Int32,
            customer_city             String,
            customer_state            String
        ) ENGINE = MergeTree() ORDER BY customer_id
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_customers')
    cols = ['customer_id','customer_unique_id','customer_zip_code_prefix',
            'customer_city','customer_state']
    rows = [tuple(r) for r in customers_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_customers VALUES', rows)
    print(f"  ✅ olist_customers ({len(rows):,} baris)")

    # TABEL 6: sellers 
    print("  Loading olist_sellers...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_sellers (
            seller_id               String,
            seller_zip_code_prefix  Int32,
            seller_city             String,
            seller_state            String
        ) ENGINE = MergeTree() ORDER BY seller_id
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_sellers')
    cols = ['seller_id','seller_zip_code_prefix','seller_city','seller_state']
    rows = [tuple(r) for r in sellers_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_sellers VALUES', rows)
    print(f" olist_sellers ({len(rows):,} baris)")

    # TABEL 7: products
    print("  Loading olist_products...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_products (
            product_id                  String,
            product_category_name       String,
            product_name_lenght         Float64,
            product_description_lenght  Float64,
            product_photos_qty          Float64,
            product_weight_g            Float64,
            product_length_cm           Float64,
            product_height_cm           Float64,
            product_width_cm            Float64
        ) ENGINE = MergeTree() ORDER BY product_id
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_products')
    cols = ['product_id','product_category_name','product_name_lenght',
            'product_description_lenght','product_photos_qty',
            'product_weight_g','product_length_cm',
            'product_height_cm','product_width_cm']
    products_pd[cols] = products_pd[cols].fillna(0)
    rows = [tuple(r) for r in products_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_products VALUES', rows)
    print(f"  olist_products ({len(rows):,} baris)")

    # TABEL 8: mql 
    print("  Loading olist_mql...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_mql (
            mql_id              String,
            first_contact_date  String,
            landing_page_id     String,
            origin              String
        ) ENGINE = MergeTree() ORDER BY mql_id
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_mql')
    mql_pd = mql_pd.fillna({'origin': 'unknown', 'landing_page_id': ''})
    cols = ['mql_id','first_contact_date','landing_page_id','origin']
    rows = [tuple(r) for r in mql_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_mql VALUES', rows)
    print(f" olist_mql ({len(rows):,} baris)")

    # TABEL 9: closed_deals 
    print("  Loading olist_closed_deals...")
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.olist_closed_deals (
            mql_id                         String,
            seller_id                      String,
            won_date                       String,
            business_segment               String,
            lead_type                      String,
            lead_behaviour_profile         String,
            business_type                  String,
            declared_product_catalog_size  Float64,
            declared_monthly_revenue       Float64
        ) ENGINE = MergeTree() ORDER BY mql_id
    ''')
    client.execute('TRUNCATE TABLE analytics.olist_closed_deals')
    deals_pd = deals_pd.fillna({
        'business_segment': 'unknown', 'lead_type': 'unknown',
        'lead_behaviour_profile': 'unknown', 'business_type': 'unknown',
        'declared_product_catalog_size': 0.0, 'declared_monthly_revenue': 0.0,
        'won_date': ''
    })
    cols = ['mql_id','seller_id','won_date','business_segment',
            'lead_type','lead_behaviour_profile','business_type',
            'declared_product_catalog_size','declared_monthly_revenue']
    rows = [tuple(r) for r in deals_pd[cols].itertuples(index=False, name=None)]
    client.execute('INSERT INTO analytics.olist_closed_deals VALUES', rows)
    print(f"  olist_closed_deals ({len(rows):,} baris)")

    # TABEL 10: category_translation 
    print("  Loading olist_category_translation...")
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.olist_category_translation (
            product_category_name          String,
            product_category_name_english  String
        ) ENGINE = MergeTree() ORDER BY product_category_name
    """)
    client.execute("TRUNCATE TABLE analytics.olist_category_translation")
    cat_pd = cat_pd.fillna({"product_category_name": "unknown",
                             "product_category_name_english": "unknown"})
    cols = ["product_category_name", "product_category_name_english"]
    rows = [tuple(r) for r in cat_pd[cols].itertuples(index=False, name=None)]
    client.execute("INSERT INTO analytics.olist_category_translation VALUES", rows)
    print(f"  olist_category_translation ({len(rows):,} baris)")

    # TABEL 11: geolocation 
    print("  Loading olist_geolocation...")
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.olist_geolocation (
            geolocation_zip_code_prefix  Int32,
            geolocation_lat              Float64,
            geolocation_lng              Float64,
            geolocation_city             String,
            geolocation_state            String
        ) ENGINE = MergeTree()
        ORDER BY (geolocation_state, geolocation_zip_code_prefix)
    """)
    client.execute("TRUNCATE TABLE analytics.olist_geolocation")
    geo_pd = geo_pd.fillna({"geolocation_city": "unknown",
                             "geolocation_state": "unknown"})
    cols = ["geolocation_zip_code_prefix", "geolocation_lat",
            "geolocation_lng", "geolocation_city", "geolocation_state"]
    rows = [tuple(r) for r in geo_pd[cols].itertuples(index=False, name=None)]
    client.execute("INSERT INTO analytics.olist_geolocation VALUES", rows)
    print(f" olist_geolocation ({len(rows):,} baris, deduplicated)")

    # CLEANUP PARQUET 
    print("\nMembersihkan file Parquet lama dari Data Lake...")
    TABLE_NAMES = [
        "orders", "order_items", "order_payments", "order_reviews",
        "customers", "products", "sellers", "geolocation",
        "mql", "closed_deals", "category_translation"
    ]
    for table_name in TABLE_NAMES:
        files = glob.glob(f"{DATA_LAKE_PATH}/{table_name}/*.parquet")
        for f in files:
            try:
                os.remove(f)
            except OSError as e:
                print(f"  Error: {f} : {e.strerror}")

    print("\nPipeline Olist Selesai! 11 tabel dimuat ke ClickHouse.")
    print("   Query agregasi bisa dilakukan langsung di Metabase / ClickHouse SQL.")

if __name__ == "__main__":
    run_spark_analytics()