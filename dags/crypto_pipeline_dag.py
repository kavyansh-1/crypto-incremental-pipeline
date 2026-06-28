import sys
sys.path.append("/home/kavyansh/projects/api-warehouse-pipeline")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from load import run_pipeline


def run_crypto_pipeline():
    run_pipeline()


with DAG(
    dag_id="crypto_incremental_pipeline",
    description="Pulls crypto prices from CoinGecko and loads them incrementally into Postgres",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["crypto", "etl"],
) as dag:

    extract_and_load_task = PythonOperator(
        task_id="extract_and_load_crypto_data",
        python_callable=run_crypto_pipeline,
    )

