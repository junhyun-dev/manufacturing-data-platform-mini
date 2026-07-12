from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from manufacturing_data_platform.orchestration import (
    build_gold_iceberg_publish_cli_command,
    build_lakehouse_cli_command,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


default_args = {
    "owner": "manufacturing-data-platform-mini",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}


with DAG(
    dag_id="manufacturing_lakehouse_to_iceberg_daily",
    description="Run the JSON lakehouse pipeline, then publish successful gold to local Iceberg.",
    default_args=default_args,
    start_date=datetime(2026, 6, 29),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["manufacturing-mini", "lakehouse", "iceberg"],
) as dag:
    lakehouse_output_dir = '{{ dag_run.conf.get("lakehouse_output_dir", "/tmp/manufacturing-mini-airflow-lakehouse-to-iceberg/lakehouse") }}'
    business_date = '{{ dag_run.conf.get("business_date", "2026-06-29") }}'

    run_lakehouse_task = BashOperator(
        task_id="run_lakehouse_task",
        cwd=str(REPO_ROOT),
        bash_command=build_lakehouse_cli_command(
            business_date=business_date,
            raw_path='{{ dag_run.conf.get("raw_path", "data/raw/manufacturing_events.csv") }}',
            output_dir=lakehouse_output_dir,
            catalog_backend="json",
        ),
    )

    publish_gold_to_iceberg_task = BashOperator(
        task_id="publish_gold_to_iceberg_task",
        cwd=str(REPO_ROOT),
        bash_command=build_gold_iceberg_publish_cli_command(
            lakehouse_output_dir=lakehouse_output_dir,
            business_date=business_date,
            warehouse='{{ dag_run.conf.get("warehouse", "/tmp/manufacturing-mini-airflow-lakehouse-to-iceberg/warehouse") }}',
            output_dir='{{ dag_run.conf.get("iceberg_output_dir", "/tmp/manufacturing-mini-airflow-lakehouse-to-iceberg/evidence") }}',
            table='{{ dag_run.conf.get("table", "local.db.gold_daily_metrics") }}',
        ),
    )

    run_lakehouse_task >> publish_gold_to_iceberg_task
