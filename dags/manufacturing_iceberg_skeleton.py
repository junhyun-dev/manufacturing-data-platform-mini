from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from manufacturing_data_platform.orchestration import build_spark_iceberg_cli_command


REPO_ROOT = Path(__file__).resolve().parents[1]


default_args = {
    "owner": "manufacturing-data-platform-mini",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=20),
}


with DAG(
    dag_id="manufacturing_iceberg_skeleton",
    description="Local Airflow wrapper for the Spark/Iceberg partition-overwrite skeleton.",
    default_args=default_args,
    start_date=datetime(2026, 6, 29),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["manufacturing-mini", "spark", "iceberg"],
) as dag:
    run_spark_iceberg_skeleton_task = BashOperator(
        task_id="run_spark_iceberg_skeleton_task",
        cwd=str(REPO_ROOT),
        bash_command=build_spark_iceberg_cli_command(
            warehouse='{{ dag_run.conf.get("warehouse", "/tmp/manufacturing-mini-airflow-iceberg-warehouse") }}',
            output_dir='{{ dag_run.conf.get("output_dir", "/tmp/manufacturing-mini-airflow-iceberg-evidence") }}',
            clean=True,
        ),
    )
