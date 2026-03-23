from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from load_core import load_stage_to_core
from load_stage import load_raw_parquets_to_stage


with DAG(
    dag_id="load_core",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["delivery-project", "load"],
) as dag:
    stage_task = PythonOperator(
        task_id="load_parquet_to_stage",
        python_callable=load_raw_parquets_to_stage,
    )

    core_task = PythonOperator(
        task_id="normalize_stage_to_core",
        python_callable=load_stage_to_core,
    )

    stage_task >> core_task
