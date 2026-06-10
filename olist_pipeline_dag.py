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
    schedule_interval='@daily',  # Jalan sekali sehari (dataset statis)
    catchup=False,
    max_active_runs=1,
    description='Olist E-Commerce: Local CSV -> Parquet -> Spark -> ClickHouse'
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