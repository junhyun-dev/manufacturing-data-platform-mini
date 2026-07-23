from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from manufacturing_data_platform.orchestration import (
    build_recovered_telemetry_publish_cli_command,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


default_args = {
    "owner": "manufacturing-data-platform-mini",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}


with DAG(
    dag_id="manufacturing_recovered_telemetry_publish",
    description=(
        "Publish one recovered sealed edge session to the local Iceberg gold table, "
        "gated on complete recovery and exact session input."
    ),
    default_args=default_args,
    start_date=datetime(2026, 6, 29),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["manufacturing-mini", "edge-recovery", "spark", "iceberg"],
) as dag:
    recovered_telemetry_publish_task = BashOperator(
        task_id="recovered_telemetry_publish_task",
        cwd=str(REPO_ROOT),
        bash_command=build_recovered_telemetry_publish_cli_command(
            spool_root='{{ dag_run.conf.get("spool_root", "/tmp/manufacturing-mini-s8-edge-recovery/spool") }}',
            edge_source_id='{{ dag_run.conf.get("edge_source_id", "edge-plant-a") }}',
            boot_session_id='{{ dag_run.conf.get("boot_session_id", "boot-0001") }}',
            landing_dir='{{ dag_run.conf.get("landing_dir", "/tmp/manufacturing-mini-s8-edge-recovery/raw") }}',
            business_date='{{ dag_run.conf.get("business_date", "2026-06-29") }}',
            adapter_output_dir='{{ dag_run.conf.get("adapter_output_dir", "/tmp/manufacturing-mini-s9/adapter") }}',
            warehouse='{{ dag_run.conf.get("warehouse", "/tmp/manufacturing-mini-s9/warehouse") }}',
            output_dir='{{ dag_run.conf.get("output_dir", "/tmp/manufacturing-mini-s9/evidence") }}',
            table='{{ dag_run.conf.get("table", "local.db.gold_daily_metrics") }}',
        ),
    )
