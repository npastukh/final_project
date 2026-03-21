from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="orchestrate_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["delivery-project", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    bootstrap = TriggerDagRunOperator(
        task_id="trigger_bootstrap_schema",
        trigger_dag_id="bootstrap_schema",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    load_core = TriggerDagRunOperator(
        task_id="trigger_load_core",
        trigger_dag_id="load_core",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    build_marts = TriggerDagRunOperator(
        task_id="trigger_build_marts",
        trigger_dag_id="build_marts",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    finish = EmptyOperator(task_id="finish")

    start >> bootstrap >> load_core >> build_marts >> finish
