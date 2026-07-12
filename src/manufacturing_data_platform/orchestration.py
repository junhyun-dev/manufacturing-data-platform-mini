from __future__ import annotations

import shlex


def build_lakehouse_cli_command(
    *,
    business_date: str,
    raw_path: str,
    output_dir: str = "data/lakehouse",
    catalog_backend: str = "mongo",
) -> str:
    """Build the Airflow BashOperator command for the lakehouse CLI.

    Airflow should orchestrate the existing CLI entrypoint, not hide business
    logic inside the DAG file. Keeping command construction here makes that
    wrapper contract testable without requiring Airflow to be installed.
    """
    is_jinja_expression = "{{" in catalog_backend and "}}" in catalog_backend
    if not is_jinja_expression and catalog_backend not in {"mongo", "json"}:
        raise ValueError("catalog_backend must be 'mongo' or 'json'")

    parts = [
        "PYTHONPATH=src",
        "python",
        "-m",
        "manufacturing_data_platform.pipeline.run",
        "--business-date",
        business_date,
        "--raw-path",
        raw_path,
        "--output-dir",
        output_dir,
        "--catalog-backend",
        catalog_backend,
    ]
    return " ".join(shlex.quote(part) for part in parts)


def build_spark_iceberg_cli_command(
    *,
    warehouse: str,
    output_dir: str,
    clean: bool = False,
) -> str:
    """Build the Airflow BashOperator command for the Spark/Iceberg skeleton."""
    parts = [
        "PYTHONPATH=src",
        "python",
        "-m",
        "manufacturing_data_platform.pipeline.spark_iceberg_skeleton",
        "--warehouse",
        warehouse,
        "--output-dir",
        output_dir,
    ]
    if clean:
        parts.append("--clean")
    return " ".join(shlex.quote(part) for part in parts)


def build_gold_iceberg_publish_cli_command(
    *,
    lakehouse_output_dir: str,
    business_date: str,
    warehouse: str,
    output_dir: str,
    table: str = "local.db.gold_daily_metrics",
    clean: bool = False,
) -> str:
    """Build the command that publishes a successful lakehouse gold run to Iceberg."""
    parts = [
        "PYTHONPATH=src",
        "python",
        "-m",
        "manufacturing_data_platform.pipeline.publish_gold_to_iceberg",
        "--lakehouse-output-dir",
        lakehouse_output_dir,
        "--business-date",
        business_date,
        "--warehouse",
        warehouse,
        "--output-dir",
        output_dir,
        "--table",
        table,
    ]
    if clean:
        parts.append("--clean")
    return " ".join(shlex.quote(part) for part in parts)
