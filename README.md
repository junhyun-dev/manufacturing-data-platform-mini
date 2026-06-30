# robot-data-platform-mini

**A thin, end-to-end slice of an ML/robot data platform: ingest sensor-ish files, catalog their metadata in MongoDB, and version each dataset for reproducibility.**

## Project Context

This is a personal learning and portfolio project. It uses only synthetic data and does not include company code, customer data, private business logic, credentials, or internal identifiers.

This repo is intended as a market-recognizable data engineering proof: metadata cataloging, medallion-style pipelines, data quality checks, lineage/catalog records, and reproducible dataset versioning.

The architecture source of truth for this project lives in this public repo: `README.md`, `DESIGN.md`, `ROADMAP.md`, `BENCHMARKS.md`, and tests.

This repo exists to close a concrete gap for robot/ML data-platform roles: **NoSQL/MongoDB + metadata catalog + dataset version manifest**. The data is synthetic sensor-ish CSV, not real ROS2 bag / MCAP / Jetson data. The point is the platform loop, kept deliberately small.

## Phase 1 Scope

v0 is the MongoDB catalog gate:

- CSV ingest
- MongoDB `datasets` document
- MongoDB `dataset_versions` manifest
- `source_hash`, `schema_hash`, `row_count`, `null_counts`
- `GET /datasets`
- `GET /datasets/{id}`

`/extract` is v0.5. Spark, Kafka, Iceberg/Delta, ROS2/MCAP, Jetson, auth, lineage graph, and worker queues are intentionally out of v0.

## Phase 2 Scope

Phase 2 adds a small lakehouse-style pipeline without changing the Phase 1 catalog contract:

```text
synthetic manufacturing CSV -> bronze -> silver -> gold -> quality -> Mongo catalog/lineage
```

- `bronze`: raw CSV copy plus a source manifest with `source_hash`, `schema_hash`, `business_date`, and row count.
- `silver`: typed, normalized, deduplicated manufacturing robot events. Built by a **pure `transform_silver`**; `write_silver` does IO only.
- `gold`: daily line/product metrics with units, defects, defect rate, average cycle time, and `closing_status`. Built by a **pure `transform_gold`**; `write_gold` does IO only.
- `quality`: a **dbt-style check suite** (see below), not just row counts. The run fails if any check fails.
- `catalog/lineage`: MongoDB `lakehouse_runs` and `lineage_events` documents describing the run, parent-child layer paths, and `schema_drift`.

### Quality, schema drift, and idempotency (Slice 1 hardening)

- **Quality suite** (`build_quality_checks`), each check is `{name, status, expected, actual, detail}`:
  - `row_count_source_to_silver` — reconciliation that **distinguishes expected filtering/dedup from real row loss** (`expected` = distinct natural keys on the active date, computed independently of how silver was built).
  - `unit_conservation_silver_to_gold` — aggregation preserves total units/defects.
  - `not_null_required_columns` (dbt `not_null`), `unique_natural_key` (dbt `unique`), `accepted_values_operation` (dbt `accepted_values`), `numeric_range_within_bounds`, `freshness_business_date`.
- **Schema drift**: `schema_hash` is computed from the **actual CSV header** (`read_rows` returns it), so an added/removed column — not just a type change in a required column — is detected. It is compared to the **previous successful run** for the dataset and reported as a `schema_drift` check. Policy = **`warn`** (surfaced, does not fail the run, so legitimate schema evolution is not blocked). Stored on the run/lineage doc.
- **Idempotency**: a re-run with the same `dataset_id + business_date + source_hash` that already has a successful run is **skipped** (returns the prior run, `status="skipped"`, increments `reuse_count`). This makes retries and backfills safe no-ops.

The Airflow DAG is an operational wrapper, not the business logic. The pipeline must run from the CLI first; Airflow only schedules, retries, passes dates, and triggers the same CLI entrypoint.

> **Known limitation (honest):** `transform_silver` casts numeric columns strictly, so an unparseable numeric value fails fast at transform time rather than being captured as a graceful quality `fail`. Graceful null/bad-row quarantine is **backlog**. Runtime MongoDB and runtime Airflow trigger are **not yet verified** in this environment (no Docker engine / Airflow not installed); the Mongo path is covered by `mongomock` tests and the offline path by the `--catalog-backend json` CLI.

See **[BENCHMARKS.md](BENCHMARKS.md)** for the reference patterns, JD keyword mapping, and what was deliberately excluded.

## Design Decisions

1. **`dataset` vs `dataset_version` are separate.**  
   `dataset` is the catalog identity. `dataset_version` is one ingest run. The same dataset can be ingested repeatedly while old manifests remain reproducible.

2. **Schema is stored in the catalog.**  
   Users should know what columns exist before opening the raw file. That is the core value of a metadata catalog.

3. **Hashes are the reproducibility primitive.**  
   `source_hash` says whether the input file is the same. `schema_hash` catches schema drift when columns or inferred types change.

The design borrows patterns from service-oriented projects such as honcho for API/config/Compose shape, and from OpenMetadata/DataHub/DVC/OpenLineage for catalog and manifest ideas, but only the v0 primitives are implemented.

## Run Locally

```bash
docker compose up -d
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=src uvicorn robot_data_platform.api:app --reload
```

> `src` layout이라 `PYTHONPATH=src`가 필요하다. (운영용으로 패키징하려면 `pip install -e .` + pyproject `[project]` 추가.)

In another shell:

```bash
curl -X POST http://127.0.0.1:8000/datasets/temp_sensor/ingest \
  -H 'Content-Type: application/json' \
  -d '{"path":"data/raw/temp_sensor_sample.csv","description":"synthetic sensor readings"}'

curl http://127.0.0.1:8000/datasets
curl http://127.0.0.1:8000/datasets/temp_sensor
```

## Run Phase 2 CLI

Start MongoDB first:

```bash
docker compose up -d
```

Then run the lakehouse slice:

```bash
PYTHONPATH=src python -m robot_data_platform.pipeline.run
```

Useful options:

```bash
PYTHONPATH=src python -m robot_data_platform.pipeline.run \
  --business-date 2026-06-29 \
  --raw-path data/raw/manufacturing_robot_events.csv \
  --output-dir data/lakehouse
```

For offline demos without MongoDB, use the JSON catalog backend:

```bash
PYTHONPATH=src python -m robot_data_platform.pipeline.run --catalog-backend json
```

## Airflow Wrapper

`dags/robot_lakehouse_daily.py` defines `robot_lakehouse_daily` with a single `run_pipeline_task`. It calls:

```bash
PYTHONPATH=src python -m robot_data_platform.pipeline.run
```

The DAG can receive `business_date` and `raw_path` through `dag_run.conf` for manual backfill-style runs. The next split, if needed, is `bronze_task -> silver_task -> gold_task -> quality_task -> catalog_task`, but the logic should stay in `robot_data_platform.pipeline`, not inside the DAG body.

## Test

```bash
pytest
```

Tests use `mongomock`, so they do not need a running MongoDB instance.

## Phase 1 Done Checklist

- [ ] `docker compose up`으로 Mongo 실행
- [ ] 샘플 CSV ingest 성공
- [ ] `datasets`·`dataset_versions`에 document 생성
- [ ] `source_hash`·`schema_hash`·`row_count`·`null_counts` 저장
- [ ] `GET /datasets/{id}`로 확인 가능
- [ ] README에 실행 명령 + 설계 결정 3개 설명
