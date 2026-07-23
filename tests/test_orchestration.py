import pytest

from manufacturing_data_platform.orchestration import (
    build_gold_iceberg_publish_cli_command,
    build_lakehouse_cli_command,
    build_spark_iceberg_cli_command,
    build_recovered_telemetry_publish_cli_command,
    build_spark_machine_event_batch_cli_command,
)


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


def test_airflow_spark_iceberg_wrapper_command_uses_skeleton_entrypoint():
    command = build_spark_iceberg_cli_command(
        warehouse="/tmp/manufacturing-airflow-iceberg-warehouse",
        output_dir="/tmp/manufacturing-airflow-iceberg-evidence",
        clean=True,
    )

    assert command.startswith(
        "PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_iceberg_skeleton"
    )
    assert "--warehouse /tmp/manufacturing-airflow-iceberg-warehouse" in command
    assert "--output-dir /tmp/manufacturing-airflow-iceberg-evidence" in command
    assert command.endswith("--clean")


def test_airflow_spark_iceberg_wrapper_command_supports_jinja_runtime_parameters():
    command = build_spark_iceberg_cli_command(
        warehouse='{{ dag_run.conf.get("warehouse", "/tmp/warehouse") }}',
        output_dir='{{ dag_run.conf.get("output_dir", "/tmp/evidence") }}',
        clean=True,
    )

    assert "dag_run.conf.get" in command
    assert "warehouse" in command
    assert "output_dir" in command
    assert "--clean" in command


def test_spark_machine_event_batch_command_uses_batch_entrypoint():
    command = build_spark_machine_event_batch_cli_command(
        landing_dir="/tmp/manufacturing-mini-kafka-k1-evidence/raw",
        business_date="2026-06-29",
        adapter_output_dir="/tmp/spark-batch/adapter",
        warehouse="/tmp/spark-batch/warehouse",
        output_dir="/tmp/spark-batch/evidence",
    )

    assert command.startswith(
        "PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_machine_event_batch"
    )
    assert "--landing-dir /tmp/manufacturing-mini-kafka-k1-evidence/raw" in command
    assert "--business-date 2026-06-29" in command
    assert "--adapter-output-dir /tmp/spark-batch/adapter" in command
    assert "--warehouse /tmp/spark-batch/warehouse" in command
    assert "--output-dir /tmp/spark-batch/evidence" in command
    assert "--table local.db.gold_daily_metrics" in command


def test_spark_machine_event_batch_command_supports_jinja_runtime_parameters():
    command = build_spark_machine_event_batch_cli_command(
        landing_dir='{{ dag_run.conf.get("landing_dir", "/tmp/raw") }}',
        business_date='{{ dag_run.conf.get("business_date", ds) }}',
        adapter_output_dir='{{ dag_run.conf.get("adapter_output_dir", "/tmp/adapter") }}',
        warehouse='{{ dag_run.conf.get("warehouse", "/tmp/warehouse") }}',
        output_dir='{{ dag_run.conf.get("output_dir", "/tmp/evidence") }}',
    )

    assert "dag_run.conf.get" in command
    assert "landing_dir" in command
    assert "business_date" in command
    assert "adapter_output_dir" in command
    assert "warehouse" in command


def test_recovered_telemetry_publish_command_uses_s9_entrypoint():
    command = build_recovered_telemetry_publish_cli_command(
        spool_root="/tmp/s8/spool",
        edge_source_id="edge-plant-a",
        boot_session_id="boot-0001",
        landing_dir="/tmp/s8/raw",
        business_date="2026-06-29",
        adapter_output_dir="/tmp/s9/adapter",
        warehouse="/tmp/s9/warehouse",
        output_dir="/tmp/s9/evidence",
    )

    assert command.startswith(
        "PYTHONPATH=src python -m manufacturing_data_platform.pipeline.recovered_telemetry_publish"
    )
    assert "--spool-root /tmp/s8/spool" in command
    assert "--edge-source-id edge-plant-a" in command
    assert "--boot-session-id boot-0001" in command
    assert "--landing-dir /tmp/s8/raw" in command
    assert "--business-date 2026-06-29" in command
    assert "--adapter-output-dir /tmp/s9/adapter" in command
    assert "--warehouse /tmp/s9/warehouse" in command
    assert "--output-dir /tmp/s9/evidence" in command
    assert "--table local.db.gold_daily_metrics" in command


def test_recovered_telemetry_publish_command_supports_jinja_runtime_parameters():
    command = build_recovered_telemetry_publish_cli_command(
        spool_root='{{ dag_run.conf.get("spool_root", "/tmp/spool") }}',
        edge_source_id='{{ dag_run.conf.get("edge_source_id", "edge-plant-a") }}',
        boot_session_id='{{ dag_run.conf.get("boot_session_id", "boot-0001") }}',
        landing_dir='{{ dag_run.conf.get("landing_dir", "/tmp/raw") }}',
        business_date='{{ dag_run.conf.get("business_date", ds) }}',
        adapter_output_dir='{{ dag_run.conf.get("adapter_output_dir", "/tmp/adapter") }}',
        warehouse='{{ dag_run.conf.get("warehouse", "/tmp/warehouse") }}',
        output_dir='{{ dag_run.conf.get("output_dir", "/tmp/evidence") }}',
    )

    assert "dag_run.conf.get" in command
    for key in ("spool_root", "edge_source_id", "boot_session_id", "landing_dir",
                "business_date", "adapter_output_dir", "warehouse", "output_dir"):
        assert key in command


def test_gold_iceberg_publish_command_uses_publish_entrypoint():
    command = build_gold_iceberg_publish_cli_command(
        lakehouse_output_dir="/tmp/lakehouse",
        business_date="2026-06-29",
        warehouse="/tmp/warehouse",
        output_dir="/tmp/evidence",
    )

    assert command.startswith(
        "PYTHONPATH=src python -m manufacturing_data_platform.pipeline.publish_gold_to_iceberg"
    )
    assert "--lakehouse-output-dir /tmp/lakehouse" in command
    assert "--business-date 2026-06-29" in command
    assert "--warehouse /tmp/warehouse" in command
    assert "--output-dir /tmp/evidence" in command
    assert "--table local.db.gold_daily_metrics" in command


def test_gold_iceberg_publish_command_supports_jinja_runtime_parameters():
    command = build_gold_iceberg_publish_cli_command(
        lakehouse_output_dir='{{ dag_run.conf.get("lakehouse_output_dir", "/tmp/lh") }}',
        business_date='{{ dag_run.conf.get("business_date", ds) }}',
        warehouse='{{ dag_run.conf.get("warehouse", "/tmp/warehouse") }}',
        output_dir='{{ dag_run.conf.get("iceberg_output_dir", "/tmp/evidence") }}',
        table='{{ dag_run.conf.get("table", "local.db.gold_daily_metrics") }}',
    )

    assert "dag_run.conf.get" in command
    assert "lakehouse_output_dir" in command
    assert "business_date" in command
    assert "warehouse" in command
    assert "iceberg_output_dir" in command
    assert "local.db.gold_daily_metrics" in command
