from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import execute_sql_file, get_sql_dir


def bootstrap_schema() -> None:
    execute_sql_file(get_sql_dir() / "10_ddl.sql")


with DAG(
    dag_id="bootstrap_schema",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["delivery-project", "bootstrap"],
) as dag:
    PythonOperator(
        task_id="create_dwh_objects",
        python_callable=bootstrap_schema,
    )
