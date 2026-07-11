import pytest

from manufacturing_data_platform.orchestration import build_lakehouse_cli_command


def test_airflow_wrapper_command_uses_lakehouse_cli_entrypoint():
    command = build_lakehouse_cli_command(
        business_date="2026-06-29",
        raw_path="data/raw/manufacturing_events.csv",
        output_dir="/tmp/manufacturing-airflow-check",
        catalog_backend="json",
    )

    assert command.startswith("PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run")
    assert "--business-date 2026-06-29" in command
    assert "--raw-path data/raw/manufacturing_events.csv" in command
    assert "--output-dir /tmp/manufacturing-airflow-check" in command
    assert "--catalog-backend json" in command


def test_airflow_wrapper_command_supports_jinja_runtime_parameters():
    command = build_lakehouse_cli_command(
        business_date='{{ dag_run.conf.get("business_date", ds) }}',
        raw_path='{{ dag_run.conf.get("raw_path", "data/raw/manufacturing_events.csv") }}',
        output_dir='{{ dag_run.conf.get("output_dir", "data/lakehouse_airflow") }}',
        catalog_backend='{{ dag_run.conf.get("catalog_backend", "json") }}',
    )

    assert "dag_run.conf.get" in command
    assert "business_date" in command
    assert "raw_path" in command
    assert "output_dir" in command
    assert "catalog_backend" in command


def test_airflow_wrapper_command_rejects_unknown_catalog_backend():
    with pytest.raises(ValueError, match="catalog_backend"):
        build_lakehouse_cli_command(
            business_date="2026-06-29",
            raw_path="data/raw/manufacturing_events.csv",
            catalog_backend="sqlite",
        )
