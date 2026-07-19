# manufacturing-data-platform-mini

> 한국어판: [`README.ko.md`](README.ko.md)

**A thin, end-to-end slice of a manufacturing-style/tabular data platform: ingest synthetic event files, catalog their metadata, and version each dataset for reproducibility.**

## Project Context

This is a personal learning and portfolio project. It uses only synthetic data and does not include company code, customer data, private business logic, credentials, or internal identifiers.

This repo is intended as a market-recognizable data engineering proof: metadata cataloging, medallion-style pipelines, data quality checks, lineage/catalog records, and reproducible dataset versioning.

The architecture source of truth for this project lives in this public repo: `README.md`, `PROJECT_PROGRESS_MAP.md`, `DESIGN.md`, `ROADMAP.md`, `BENCHMARKS.md`, and tests.

This repo exists to close a concrete data-platform gap: **NoSQL/MongoDB-style metadata catalog + dataset version manifest + quality/lineage evidence**. The data is synthetic manufacturing-style CSV, not real ROS2 bag / MCAP / Jetson data. Until a machine/session source slice exists, describe this externally as a **manufacturing-style/tabular mini data platform**, not a production manufacturing data platform. The point is the platform loop, kept deliberately small.

**Overall design trace:** [`service purpose -> scenario -> questions -> contracts -> features -> evidence`](learn/system-design/01-system-traceability-map.ko.md) shows how the batch spine, EAV, operator evidence, Spark/Iceberg, Airflow, and Kafka slices fit into one platform.

**Kafka milestone walkthrough:** [`Kafka K1/K1.5: machine events -> recoverable raw landing -> trusted gold -> local Iceberg`](docs/portfolio/kafka-k1-k1-5/README.md) packages one representative failure/recovery scenario, runtime screens, reproduction commands, evidence, and explicit limitations. It is an ingestion-path milestone, not the whole platform architecture.

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
- `silver`: typed, normalized, deduplicated manufacturing-style events. Built by a **pure `transform_silver`**; `write_silver` does IO only.
- `gold`: daily line/product metrics with units, defects, defect rate, average cycle time, and `closing_status`. Built by a **pure `transform_gold`**; `write_gold` does IO only.
- `quality`: a **dbt-style check suite** (see below), not just row counts. The run fails if any check fails.
- `catalog/lineage`: MongoDB `lakehouse_runs` and `lineage_events` documents describing the run, parent-child layer paths, and `schema_drift`.

### Quality, schema drift, and idempotency (Slice 1 hardening)

- **Quality suite** (`build_quality_checks`), each check is `{name, status, expected, actual, detail}`:
  - `row_count_source_to_silver` — reconciliation that **distinguishes expected filtering/dedup from real row loss** (`expected` = distinct natural keys on the active date, computed independently of how silver was built).
  - `unit_conservation_silver_to_gold` — aggregation preserves total units/defects.
  - `not_null_required_columns` (dbt `not_null`), `unique_natural_key` (dbt `unique`), `accepted_values_operation` (dbt `accepted_values`), `numeric_range_within_bounds`, `freshness_business_date`.
- `freshness_business_date` is a partition/date-validity guard for the active `business_date`, not a dbt/DataHub-style data-age SLA. Age-based freshness is backlog.
- **Schema drift**: `schema_hash` is computed from the **actual CSV header** (`read_rows` returns it), so an added/removed column — not just a type change in a required column — is detected. It is compared to the **previous successful run** for the dataset and reported as a `schema_drift` check. Policy = **`warn`** (surfaced, does not fail the run, so legitimate schema evolution is not blocked). Stored on the run/lineage doc.
- **Idempotency**: a re-run with the same `dataset_id + business_date + source_hash` that already has a successful run is **skipped** (returns the prior run, `status="skipped"`, increments `reuse_count`). This makes retries and backfills safe no-ops.

The Airflow DAG is an operational wrapper, not the business logic. The pipeline must run from the CLI first; Airflow only schedules, retries, passes dates, and triggers the same CLI entrypoint.

> **Known limitation (honest):** `transform_silver` casts numeric columns strictly, so an unparseable numeric value fails fast at transform time rather than being captured as a graceful quality `fail`. Graceful null/bad-row quarantine is **backlog**. Runtime MongoDB is **not yet verified** in this environment (no Docker engine); the Mongo path is covered by `mongomock` tests and the offline path by the `--catalog-backend json` CLI. Airflow is verified locally via `airflow dags test` and an Airflow 3.3.0 `standalone` scheduler/LocalExecutor run, not as a production deployment.

See **[BENCHMARKS.md](BENCHMARKS.md)** for the reference patterns, JD keyword mapping, and what was deliberately excluded.

## Phase 2 — EAV mini slice (data modeling / multi-format intake)

A **core** slice that ingests several differently-shaped wide files and unifies them through a config-driven **EAV (entity–attribute–value)** model, then pivots back to a gold metric mart. It **reuses the Slice 1 spine** (idempotency, schema-drift, catalog/lineage, dbt-style quality) — only the dataset profile and the check pack differ.

```text
many wide CSVs (different columns/units) -> mapping config (JSON) -> EAV long -> pivot/aggregate -> gold -> quality -> Mongo catalog/lineage
```

- **3 synthetic formats** (generated into `data/raw/eav/` from `sample_eav.py`): Korean headers, English headers, mixed units (°F / bar) — fully synthetic, no company data.
- **Config-driven**: each `config/eav_mappings/*.json` maps a source's columns → standard fields (`units_produced, defect_count, temperature_c, pressure_kpa`) with optional deterministic unit conversions (`f_to_c`, `bar_to_kpa`). **A new file format is onboarded by adding one config — no pipeline code change** (covered by a test).
- **EAV (silver)**: `entity_id, business_date, attribute, value, value_type, source_id, source_file_id`. `source_file_id` is the file-content hash = the file-level idempotency key.
- **Gold**: pivot/aggregate per `(business_date, entity_id)` — sum for counts, average for sensor readings.
- **Quality** (dbt-style): `mapping_coverage`, `unmapped_source_columns` (warn), `not_null_value`, `accepted_values_attribute`, `value_type_valid`, `numeric_range_within_bounds`, `eav_to_gold_conservation`, `freshness_business_date`, plus shared `schema_drift`. Here too, `freshness_business_date` means active-date validity/partition correctness, not age-based source freshness. Unparseable values are captured gracefully (value=`None` + a `value_type_valid` failure), not crashed on.

### How the EAV experience is described honestly

In prior **professional** work I **operated and improved** an EAV-based structure that handled many heterogeneous file formats. In **this personal project** I **implemented** a wide → EAV → gold flow from scratch on **fully synthetic** data to reinforce my data-modeling understanding. The **file_id idempotency** and sync-style reprocessing are my own re-design/implementation. No company code, data, names, or schemas are used here.

> Interview line: "실무에서는 EAV 기반 구조를 운영·개선하며 다양한 파일 양식을 처리했고, 개인 프로젝트에서는 가상 데이터로 wide → EAV → gold 지표 흐름을 직접 구현해 데이터 모델링 이해를 보강했습니다."

## Scope: core vs optional

- **Core** (thesis = lakehouse data platform + modeling + quality + catalog/lineage):
  - Medallion pipeline — Slice 1 (done + hardened)
  - EAV mini — multi-format → mapping → EAV → gold (done)
  - Quality checks, catalog/lineage — cross-cutting (done)
  - Operator debugging walkthrough — gold metric -> run/source/quality/lineage evidence (done)
  - Spark/Iceberg single-gold-table walking skeleton — partition overwrite + snapshot evidence (done)
  - Lakehouse gold -> Iceberg publish DAG — successful JSON gold run -> local Iceberg current table (done)
  - Kafka K1 bounded raw ingestion — immutable JSONL landing + offset/recovery/replay evidence (done)
  - Kafka K1.5 landing -> batch bridge — deterministic provenance-preserving CSV -> existing quality/gold/Iceberg path (done)
  - Spark machine-event batch (S7) — Spark re-expresses silver/gold from the K1.5 canonical CSV with verified Python parity, quality-gated `overwritePartitions()` publish, and shuffle-plan evidence (done)
  - Full medallion Spark rewrite (backlog)
- **Optional** (only pursued if a specific interview, e.g. Labrador-style, makes it relevant):
  - AI Dataset QA slice
  - RAG / vectorDB / LLM-preprocessing

Optional slices would reuse the same spine; they are deliberately deferred so the core thesis stays sharp.

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
PYTHONPATH=src uvicorn manufacturing_data_platform.api:app --reload
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
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run
```

Useful options:

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run \
  --business-date 2026-06-29 \
  --raw-path data/raw/manufacturing_events.csv \
  --output-dir data/lakehouse
```

For offline demos without MongoDB, use the JSON catalog backend:

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json
```

## Inspect Operator Evidence

After a JSON-backed lakehouse run, inspect the run/source/quality/lineage evidence for one `business_date`:

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.operator_report \
  --output-dir data/lakehouse \
  --business-date 2026-06-29
```

This is a read-only operator report. It summarizes the gold row grain, `run_id`, `source_hash`, `schema_hash`, quality check status, row counts, and the path-level lineage trace (`gold -> silver -> bronze -> source`). It does not claim column-level lineage or an OpenLineage backend.

## Run EAV mini CLI

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run_eav --catalog-backend json --output-dir data/lakehouse_eav
```

Synthetic inputs (`data/raw/eav/`) and mapping configs (`config/eav_mappings/`) are used automatically; missing synthetic inputs are generated from `sample_eav.py`. Re-running the same inputs for the same date is idempotent (`status="skipped"`). To add a new file format, drop one more `config/eav_mappings/<source>.json` (+ its CSV) — no code change.

## Run Spark/Iceberg walking skeleton

This is an optional local skeleton, not the main lightweight install:

```bash
pip install -r requirements-spark.txt

PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_iceberg_skeleton \
  --warehouse /tmp/manufacturing-mini-iceberg-warehouse \
  --output-dir /tmp/manufacturing-mini-iceberg-evidence \
  --clean
```

It creates one local Iceberg table, `local.db.gold_daily_metrics`, partitioned by `business_date`. It writes initial rows, skips a same-`source_hash` rerun without creating a new snapshot, then overwrites the corrected `business_date` partition with `DataFrameWriterV2.overwritePartitions()`. Evidence is written as JSON under the output directory.

Honest boundary: this proves a single-gold-table Spark/Iceberg partition-overwrite contract. It is not a full Spark medallion rewrite, production lakehouse, or rollback system.

## Publish Lakehouse Gold to Iceberg

The bridge from the real lakehouse CLI to Iceberg is a separate publish step:

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run \
  --business-date 2026-06-29 \
  --raw-path data/raw/manufacturing_events.csv \
  --output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg/lakehouse \
  --catalog-backend json

PYTHONPATH=src python -m manufacturing_data_platform.pipeline.publish_gold_to_iceberg \
  --lakehouse-output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg/lakehouse \
  --business-date 2026-06-29 \
  --warehouse /tmp/manufacturing-mini-lakehouse-to-iceberg/warehouse \
  --output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg/evidence \
  --clean
```

This reads the latest successful JSON catalog state for the target `business_date`, loads that run's gold CSV, and publishes it to `local.db.gold_daily_metrics` with Iceberg `overwritePartitions()`. Re-publishing the same `pipeline_run_id + source_hash` is skipped without creating a new snapshot.

Honest boundary: this is a JSON-catalog-backed local publish slice. It does not implement Mongo-backed publish lookup, Spark-based quality checks, a full Spark medallion rewrite, production Airflow deployment, or cluster Spark.

## Airflow Wrapper

`dags/manufacturing_lakehouse_daily.py` defines `manufacturing_lakehouse_daily` with a single `run_pipeline_task`. It calls:

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run \
  --business-date <date> \
  --raw-path <path> \
  --output-dir <path> \
  --catalog-backend <mongo|json>
```

The DAG can receive `business_date`, `raw_path`, `output_dir`, and `catalog_backend` through `dag_run.conf` for manual backfill-style runs. The command contract is built by `manufacturing_data_platform.orchestration.build_lakehouse_cli_command` and covered by `tests/test_orchestration.py`, so the wrapper stays testable without Airflow installed.

Local Airflow runtime was verified with Airflow 3.3.0 in an isolated virtualenv:

```bash
python -m venv /tmp/manufacturing-mini-airflow-venv
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements-airflow.txt

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags test manufacturing_lakehouse_daily 2026-06-29 \
  -c '{"business_date":"2026-06-29","raw_path":"data/raw/manufacturing_events.csv","output_dir":"/tmp/manufacturing-mini-airflow-runtime","catalog_backend":"json"}'
```

That proves the DAG can import and trigger the same CLI task locally. Running the same `dags test` again against the same JSON output state returns pipeline `status="skipped"`, so Airflow retry/backfill safety still comes from the pipeline `source_hash` idempotency gate. It does not prove a production scheduler/worker/webserver deployment. The next split, if needed, is `bronze_task -> silver_task -> gold_task -> quality_task -> catalog_task`, but the logic should stay in `manufacturing_data_platform.pipeline`, not inside the DAG body.

## Airflow-triggered Spark/Iceberg Skeleton

`dags/manufacturing_iceberg_skeleton.py` defines `manufacturing_iceberg_skeleton` with a single `run_spark_iceberg_skeleton_task`. It calls the Spark/Iceberg skeleton CLI:

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_iceberg_skeleton \
  --warehouse <path> \
  --output-dir <path> \
  --clean
```

Local runtime verification:

```bash
AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags test manufacturing_iceberg_skeleton 2026-06-29 \
  -c '{"warehouse":"/tmp/manufacturing-mini-airflow-iceberg-warehouse","output_dir":"/tmp/manufacturing-mini-airflow-iceberg-evidence"}'
```

This verifies local Airflow orchestration of the Spark/Iceberg walking skeleton: the task creates the local Iceberg table, records `run_id -> snapshot_id` evidence, overwrites the corrected `business_date` partition, and leaves the other partition unchanged. It still does not prove production Airflow scheduler/worker deployment, cluster Spark, or a full Spark medallion pipeline.

`airflow dags test` runs a single DagRun locally. It verifies DAG import, task wiring, templated command rendering, and command execution, but it does not verify scheduler, queue, executor, worker, or webserver behavior.

Local Airflow standalone was also verified for the Spark/Iceberg skeleton. The worker environment must include Airflow, the project runtime deps, and Spark deps in the same venv:

```bash
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements-airflow.txt
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements.txt -r requirements-spark.txt

export AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-standalone-home
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH=src
export PATH="/tmp/manufacturing-mini-airflow-venv/bin:$PATH"

airflow standalone
```

The reproducible runbook is:

```bash
scripts/verify_airflow_standalone.sh
```

In that local standalone run, the API server responded on `127.0.0.1:8080`, the scheduler parsed both project DAGs, and a manual `airflow dags trigger manufacturing_iceberg_skeleton` run finished with `dag=success` and `task=success` through LocalExecutor. This is still development-only Airflow standalone, not a production scheduler/worker/webserver deployment or cluster Spark runtime.

## Lakehouse to Iceberg DAG

`dags/manufacturing_lakehouse_to_iceberg_daily.py` defines `manufacturing_lakehouse_to_iceberg_daily` with two tasks:

```text
run_lakehouse_task -> publish_gold_to_iceberg_task
```

The first task runs the JSON-backed lakehouse CLI. The second task reads the latest successful JSON catalog state for the same `business_date` and publishes that gold CSV to the local Iceberg table. This keeps quality/catalog ownership in the existing pipeline and keeps Spark/Iceberg publish logic outside the DAG body.

Local `airflow dags test` verification passed for this two-task DAG. It proves local DAG import, task ordering, command rendering, and command execution. It still does not prove a production scheduler/worker/webserver deployment or cluster Spark runtime.

## Kafka K1 Bounded Raw Ingestion

Kafka K1 is implemented as a bounded local raw-ingestion proof. The shared runbook
downloads the pinned Apache Kafka 4.3.1 binary, verifies its SHA-512, starts one
local KRaft broker, and installs `confluent-kafka==2.15.0` in an isolated virtualenv.

Environment-only Test 0:

```bash
./scripts/verify_kafka_test0.sh
```

Full K1 verification:

```bash
./scripts/verify_kafka_k1.sh
```

K1 publishes strict versioned JSON machine events keyed by `machine_id`. A bounded
consumer writes immutable JSONL batches containing the payload and
`topic/partition/offset` evidence, then commits the next offset only after fsync and
atomic directory rename. A failure-injection run crashes after landing but before
commit; the same consumer group receives the record again, reuses the persisted
coordinate, and commits without increasing the accepted set. The runbook also
verifies bounded offset replay and invalid-event quarantine.

Runtime evidence is written under `/tmp/manufacturing-mini-kafka-k1-evidence` and
the broker is stopped automatically. This does not verify a continuous streaming
service, multi-partition routing/rebalance, multi-broker availability, end-to-end
exactly-once, Spark Structured Streaming, or production Kafka operations.

## Kafka K1.5 Landing To Batch Bridge

K1.5 closes the bounded path from accepted Kafka landing to the existing batch
quality/gold/Iceberg flow without adding Spark Structured Streaming:

```text
accepted JSONL + Kafka manifest
-> deterministic content-addressed CSV + provenance
-> existing JSON-backed bronze/silver/gold + quality
-> existing local Spark/Iceberg publish
```

Reproduce K1 and then the bridge:

```bash
./scripts/verify_kafka_k1.sh
./scripts/verify_kafka_k1_5.sh
```

The adapter requires one explicit `business_date`, includes `event_id` and Kafka
coordinates in the canonical source identity, and rejects empty, inconsistent, or
multi-partition input before the lakehouse current state can advance. Re-running the
same accepted set reuses the adapter version and returns the existing lakehouse run as
`status="skipped"`.

This is a bounded local bridge. It is not a continuous streaming pipeline, a direct
Kafka-to-Iceberg sink, end-to-end exactly-once, column-level lineage, or production
Kafka/Spark operation.

## Test

```bash
pytest
```

Tests use `mongomock`, so they do not need a running MongoDB instance.

## Phase 1 Done Checklist

- [x] 샘플 CSV ingest path test-covered with `mongomock`
- [x] `datasets`·`dataset_versions` document creation test-covered
- [x] `source_hash`·`schema_hash`·`row_count`·`null_counts` stored in the manifest/catalog model
- [x] `GET /datasets/{id}` test-covered
- [x] README에 실행 명령 + 설계 결정 3개 설명
- [ ] `docker compose up`으로 real Mongo runtime 실행
- [ ] real Mongo runtime에서 샘플 ingest/API 확인
