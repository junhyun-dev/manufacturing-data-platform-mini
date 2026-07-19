from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from manufacturing_data_platform.orchestration import (
    build_spark_machine_event_batch_cli_command,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


default_args = {
    "owner": "manufacturing-data-platform-mini",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}


with DAG(
    dag_id="manufacturing_spark_machine_event_batch",
    description="Backfill one business_date from a K1.5 Kafka landing through a local Spark batch to Iceberg.",
    default_args=default_args,
    start_date=datetime(2026, 6, 29),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["manufacturing-mini", "spark", "iceberg", "kafka"],
) as dag:
    spark_machine_event_batch_task = BashOperator(
        task_id="spark_machine_event_batch_task",
        cwd=str(REPO_ROOT),
        bash_command=build_spark_machine_event_batch_cli_command(
            landing_dir='{{ dag_run.conf.get("landing_dir", "/tmp/manufacturing-mini-kafka-k1-evidence/raw") }}',
            business_date='{{ dag_run.conf.get("business_date", "2026-06-29") }}',
            adapter_output_dir='{{ dag_run.conf.get("adapter_output_dir", "/tmp/manufacturing-mini-spark-batch/adapter") }}',
            warehouse='{{ dag_run.conf.get("warehouse", "/tmp/manufacturing-mini-spark-batch/warehouse") }}',
            output_dir='{{ dag_run.conf.get("output_dir", "/tmp/manufacturing-mini-spark-batch/evidence") }}',
            table='{{ dag_run.conf.get("table", "local.db.gold_daily_metrics") }}',
        ),
    )
