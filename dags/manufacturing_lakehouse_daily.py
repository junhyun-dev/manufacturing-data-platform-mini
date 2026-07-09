from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


REPO_ROOT = Path(__file__).resolve().parents[1]


default_args = {
    "owner": "manufacturing-data-platform-mini",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


with DAG(
    dag_id="manufacturing_lakehouse_daily",
    description="Operational wrapper for the mini lakehouse CLI pipeline.",
    default_args=default_args,
    start_date=datetime(2026, 6, 29),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["manufacturing-mini", "lakehouse"],
) as dag:
    run_pipeline_task = BashOperator(
        task_id="run_pipeline_task",
        cwd=str(REPO_ROOT),
        bash_command=(
            "PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run "
            "--business-date '{{ dag_run.conf.get(\"business_date\", ds) }}' "
            "--raw-path '{{ dag_run.conf.get(\"raw_path\", \"data/raw/manufacturing_events.csv\") }}'"
        ),
    )

