from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.scraper import run_scraper
from etl.transform import run_transform
from etl.load import run_load

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="linkedin_etl_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["etl", "linkedin"],
    params={
        "max_pages": 2,
    }
) as dag:

    scrape = PythonOperator(
        task_id="scrape",
        python_callable=run_scraper,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    scrape >> transform >> load