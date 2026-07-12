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
