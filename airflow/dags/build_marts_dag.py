from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from build_marts import build_marts


with DAG(
    dag_id="build_marts",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["delivery-project", "mart"],
) as dag:
    PythonOperator(
        task_id="build_daily_marts_with_pyspark",
        python_callable=build_marts,
    )
