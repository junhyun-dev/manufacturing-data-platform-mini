from __future__ import annotations

import importlib.util
import os
import tempfile
from pathlib import Path

import pytest


pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("airflow") is None,
    reason="optional Airflow dependency not installed; run `pip install -r requirements-airflow.txt`",
)


def _dagbag():
    os.environ.setdefault(
        "AIRFLOW_HOME",
        str(Path(tempfile.gettempdir()) / "manufacturing-mini-airflow-pytest-home"),
    )
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

    from airflow.dag_processing.dagbag import DagBag

    dag_folder = Path(__file__).resolve().parents[1] / "dags"
    return DagBag(dag_folder=str(dag_folder))


def test_airflow_dagbag_parses_project_dags():
    dagbag = _dagbag()

    assert dagbag.import_errors == {}
    assert "manufacturing_lakehouse_daily" in dagbag.dags
    assert "manufacturing_iceberg_skeleton" in dagbag.dags
    assert "manufacturing_lakehouse_to_iceberg_daily" in dagbag.dags
    assert "manufacturing_spark_machine_event_batch" in dagbag.dags
    assert "manufacturing_recovered_telemetry_publish" in dagbag.dags


def test_airflow_lakehouse_dag_calls_lakehouse_cli():
    dag = _dagbag().dags["manufacturing_lakehouse_daily"]
    task = dag.get_task("run_pipeline_task")

    assert "manufacturing_data_platform.pipeline.run" in task.bash_command
    assert "dag_run.conf.get" in task.bash_command
    assert "business_date" in task.bash_command
    assert "catalog_backend" in task.bash_command


def test_airflow_iceberg_dag_calls_spark_iceberg_cli():
    dag = _dagbag().dags["manufacturing_iceberg_skeleton"]
    task = dag.get_task("run_spark_iceberg_skeleton_task")

    assert "manufacturing_data_platform.pipeline.spark_iceberg_skeleton" in task.bash_command
    assert "dag_run.conf.get" in task.bash_command
    assert "warehouse" in task.bash_command
    assert "output_dir" in task.bash_command
    assert "--clean" in task.bash_command


def test_airflow_lakehouse_to_iceberg_dag_chains_pipeline_then_publish():
    dag = _dagbag().dags["manufacturing_lakehouse_to_iceberg_daily"]
    run_task = dag.get_task("run_lakehouse_task")
    publish_task = dag.get_task("publish_gold_to_iceberg_task")

    assert "manufacturing_data_platform.pipeline.run" in run_task.bash_command
    assert "manufacturing_data_platform.pipeline.publish_gold_to_iceberg" in publish_task.bash_command
    assert "--catalog-backend json" in run_task.bash_command
    assert "--lakehouse-output-dir" in publish_task.bash_command
    assert "--warehouse" in publish_task.bash_command
    assert publish_task.task_id in run_task.downstream_task_ids


def test_airflow_recovered_telemetry_publish_dag_is_single_task_wrapper():
    dag = _dagbag().dags["manufacturing_recovered_telemetry_publish"]

    assert dag.max_active_runs == 1
    assert len(dag.tasks) == 1
    task = dag.get_task("recovered_telemetry_publish_task")
    assert "manufacturing_data_platform.pipeline.recovered_telemetry_publish" in task.bash_command
    assert "dag_run.conf.get" in task.bash_command
    assert "--spool-root" in task.bash_command
    assert "--business-date" in task.bash_command
    # No recovery/coverage/transform/quality/Iceberg logic leaks into the DAG body.
    for leak in ("require_recovery_ready", "groupBy", "overwritePartitions", "coverage"):
        assert leak not in task.bash_command


def test_airflow_spark_machine_event_batch_dag_is_single_task_wrapper():
    dag = _dagbag().dags["manufacturing_spark_machine_event_batch"]

    assert dag.max_active_runs == 1
    assert len(dag.tasks) == 1
    task = dag.get_task("spark_machine_event_batch_task")
    assert "manufacturing_data_platform.pipeline.spark_machine_event_batch" in task.bash_command
    assert "dag_run.conf.get" in task.bash_command
    assert "--landing-dir" in task.bash_command
    assert "--business-date" in task.bash_command
    # No transform/quality/Iceberg logic leaks into the DAG body.
    assert "groupBy" not in task.bash_command
    assert "overwritePartitions" not in task.bash_command
