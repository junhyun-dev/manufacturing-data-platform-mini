# ROADMAP

> 한글판: [`ROADMAP.ko.md`](ROADMAP.ko.md) (번역본 — source of truth는 이 영어 문서).

## Phase 1 v0 — MongoDB catalog gate

This scope is implemented and test-covered, but still has a runtime verification gap. It closes the closest metadata-catalog gap first: **NoSQL/MongoDB + metadata catalog**.

- [x] **MongoDB metadata catalog** — ingest a synthetic manufacturing-style CSV and register `schema · row/null stats · source · ingested_at`.
- [x] **Version manifest** — `dataset_id · version · source_hash · schema_hash · ingested_at · row_count`.
- [x] **FastAPI catalog endpoints** — `GET /datasets`, `GET /datasets/{id}`.
- [x] **README design rationale** — architecture, tradeoffs, run commands, and Done checklist.
- [ ] **Runtime Mongo verification** — `docker compose up` + real Mongo ingest. Blocked in this environment because Docker Desktop engine is unavailable.

End of v0 for this environment = the catalog loop is implemented and test-covered with `mongomock`. The stronger runtime claim should wait until real MongoDB verification succeeds on a machine with Docker available.

## Phase 1 v0.5 — DaaS extract

- [ ] `GET /datasets/{id}/extract?version=&columns=` — conditional extract for the conditional extract keyword.

## Phase 1 v1 — orchestration polish

- [ ] Self-hosted Airflow orchestration in Docker Compose: ingest → catalog → version → serve.

This is useful later, but not part of the current MongoDB/catalog gate.

## Phase 2 — Mini Lakehouse

### Slice 1 (implemented)
- [x] **Slice 1 CLI** — synthetic manufacturing CSV -> bronze -> silver -> gold -> quality -> Mongo catalog/lineage.
- [x] **Airflow wrapper** — `dags/manufacturing_lakehouse_daily.py` triggers the CLI as an operational wrapper.

### Slice 1 hardening — NOW (implemented this pass)
Goal: make the claims (data quality, schema drift, idempotency, transform/IO separation) actually true in code.
- [x] **transform/IO separation** — pure `transform_silver` / `transform_gold`; `write_*` does IO only (sets up the Spark engine swap).
- [x] **Quality suite** — dbt-style checks (`not_null`, `unique`, `accepted_values`, range, freshness) + reconciliation that distinguishes filtering/dedup from real row loss. Tautology check removed.
- [x] **Schema drift** — `schema_hash` from the actual CSV header (added/removed columns detected), compared to the previous successful run; `schema_drift` check, policy = `warn`; stored on run/lineage doc.
- [x] **Idempotency** — skip re-run on `dataset_id + business_date + source_hash` with a prior success (safe retry/backfill).
- [x] **BENCHMARKS.md** — reference patterns, JD mapping, anti-benchmark (deliberate exclusions).

### EAV mini slice — CORE (implemented this pass)
Goal: data modeling + multi-format intake, reusing the Slice 1 spine (no fork).
- [x] **3 synthetic wide formats** — different columns/units (Korean/English headers, °F/bar), fully synthetic (`sample_eav.py`).
- [x] **Config-driven mapping** — `config/eav_mappings/*.json` → standard fields + deterministic unit conversions (`f_to_c`, `bar_to_kpa`). New format = add one config (tested).
- [x] **wide → EAV (long) → gold pivot/aggregate** — pure `transform_to_eav` / `transform_eav_to_gold`.
- [x] **EAV quality suite** — `mapping_coverage`, `unmapped_source_columns` (warn), `not_null_value`, `accepted_values_attribute`, `value_type_valid`, `numeric_range`, `eav_to_gold_conservation`, `freshness` + shared `schema_drift`.
- [x] **Catalog/lineage + idempotency reuse** — same `lakehouse_runs`/`lineage_events`, `file_id` (file hash) idempotency.

### Spark/Iceberg walking skeleton — CORE-lite (implemented)
Goal: prove the storage/table contract for a corrected `business_date` without doing a full Spark rewrite.
- [x] **Optional Spark dependency pin** — `requirements-spark.txt` pins `pyspark==3.5.8`.
- [x] **Local Iceberg catalog** — Spark hadoop catalog + local warehouse.
- [x] **Single gold table** — `local.db.gold_daily_metrics`, partitioned by `business_date`.
- [x] **Partition overwrite** — corrected rows use `DataFrameWriterV2.overwritePartitions()`.
- [x] **Safety assertion** — target date is replaced without duplicates while another date partition remains unchanged.
- [x] **Snapshot evidence** — `run_id -> snapshot_id` evidence JSON; same `source_hash` rerun creates no new snapshot.
- [ ] **Full medallion Spark rewrite** — intentionally not implemented.
- [x] **Airflow-triggered Spark runtime (local `dags test` + standalone)** — local Airflow triggers the Spark/Iceberg skeleton through both `dags test` and a development `standalone` scheduler/LocalExecutor run.

### Airflow runtime wrapper — CORE-lite (implemented)
Goal: prove Airflow can import the DAG and trigger the same lakehouse CLI task locally, without moving business logic into the DAG.
- [x] **Optional Airflow dependency pin** — `requirements-airflow.txt` pins `apache-airflow==3.3.0` and `apache-airflow-providers-standard==1.15.0` with official Python 3.10 constraints.
- [x] **DAG import** — `airflow dags list` loads `manufacturing_lakehouse_daily`.
- [x] **Task discovery** — `airflow tasks list manufacturing_lakehouse_daily` shows `run_pipeline_task`.
- [x] **Local runtime trigger** — `airflow dags test` runs the BashOperator and the JSON catalog CLI succeeds.
- [x] **Retry/idempotency boundary** — running the same `dags test` again returns pipeline `status="skipped"`.
- [x] **Runtime conf** — `dag_run.conf` passes `business_date`, `raw_path`, `output_dir`, and `catalog_backend`.
- [ ] **Scheduler/worker/webserver deployment** — intentionally not implemented.

### Airflow-triggered Spark/Iceberg skeleton — CORE-lite (implemented)
Goal: prove local Airflow can trigger the existing Spark/Iceberg partition-overwrite skeleton without moving Spark logic into the DAG.
- [x] **DAG wrapper** — `dags/manufacturing_iceberg_skeleton.py` calls the Spark/Iceberg CLI.
- [x] **Command contract** — `build_spark_iceberg_cli_command` is test-covered.
- [x] **DAG parse contract** — optional Airflow DagBag tests cover DAG ids, task ids, and BashOperator commands when Airflow is installed.
- [x] **Local runtime trigger** — `airflow dags test manufacturing_iceberg_skeleton` succeeds.
- [x] **Local standalone scheduler trigger** — Airflow 3.3.0 `standalone` starts API server/scheduler/dag-processor/triggerer and a manual `airflow dags trigger` run succeeds through LocalExecutor.
- [x] **Standalone verification runbook** — `scripts/verify_airflow_standalone.sh` reproduces startup, trigger, state polling, evidence assertions, and cleanup.
- [x] **Worker dependency packaging** — the standalone worker venv must include `requirements-airflow.txt`, `requirements.txt`, and `requirements-spark.txt`.
- [x] **Iceberg evidence** — generated `run_snapshot_map.json`, `current_gold.json`, and `snapshot_comparison.json`.
- [x] **Partition overwrite assertions** — `snapshot_increment=1`, `same_source_created_snapshot=false`, target date replaced, other date preserved.
- [ ] **Production Airflow scheduler/worker deployment** — intentionally not implemented.
- [ ] **Cluster Spark / full Spark medallion pipeline** — intentionally not implemented.

### Lakehouse gold -> Iceberg publish DAG — CORE-lite (implemented)
Goal: connect the implemented JSON-backed lakehouse pipeline to a local Iceberg current table without doing a full Spark rewrite.
- [x] **Publish CLI** — `publish_gold_to_iceberg` reads the latest successful JSON catalog state for a `business_date`.
- [x] **Gold CSV publish** — the selected run's gold CSV is written to `local.db.gold_daily_metrics`.
- [x] **Partition overwrite** — publish uses `DataFrameWriterV2.overwritePartitions()`.
- [x] **Publish idempotency** — re-publishing the same `pipeline_run_id + source_hash` is skipped without creating a new snapshot.
- [x] **Airflow DAG** — `manufacturing_lakehouse_to_iceberg_daily` chains `run_lakehouse_task -> publish_gold_to_iceberg_task`.
- [x] **Command contract + DAG parse tests** — orchestration and optional Airflow DagBag tests cover the new DAG.
- [x] **Local runtime trigger** — `airflow dags test manufacturing_lakehouse_to_iceberg_daily` succeeds.
- [ ] **Mongo-backed publish lookup** — intentionally not implemented until runtime Mongo is verified.
- [ ] **Full Spark medallion pipeline / Spark quality suite** — intentionally not implemented.

### Kafka raw ingestion — K1 (implemented and local broker-verified)
Goal: prove bounded log-based raw ingestion before considering Spark Structured Streaming.
- [x] **Kafka Test 0 runtime pin** — Apache Kafka 4.3.1 KRaft binary + SHA-512 verification.
- [x] **Python client pin** — isolated `confluent-kafka==2.15.0` environment.
- [x] **Broker/client round-trip** — one local broker, one topic, one partition, one event, manual offset commit.
- [x] **Reproducible runbook** — `scripts/verify_kafka_test0.sh` starts, verifies, and stops the broker.
- [x] **K1 event/source contract** — strict JSON v1, `event_id`, `machine_id` key, Kafka coordinate evidence.
- [x] **K1 immutable raw landing** — bounded consumer writes payload + Kafka coordinates through fsync + atomic rename.
- [x] **K1 recovery evidence** — crash after durable landing/before commit, redelivery reuse, offset recovery, bounded replay.
- [x] **K1 quarantine evidence** — invalid event is durably quarantined and does not block the single partition.
- [x] **K1.5 landing -> batch bridge** — deterministic provenance-preserving CSV reuses the existing quality/gold/Iceberg path; same input is skipped.
- [ ] **Spark Structured Streaming** — backlog until a real window/watermark/latency pressure exists.

### Spark machine-event batch — S7 (implemented and local runtime-verified)
Goal: re-express the existing Python silver/gold on one landed `business_date` with Spark, without a full medallion rewrite or streaming.
- [x] **Adapter-input contract** — reuses the K1.5 canonical CSV + `source_hash`; Spark does not re-parse raw JSONL.
- [x] **Engine parity** — Spark DataFrame built-ins reproduce `transform_silver`/`transform_gold` grain and totals (verified equal, incl. a `format_number`-based round matching Python `round` at boundary values like `802.675` and coordinate-ordered natural-key dedup).
- [x] **Spark quality gate** — the existing quality suite runs on the Spark result; a failing result blocks the Iceberg write and the success pointer.
- [x] **Partition overwrite + idempotency** — `overwritePartitions()`, same-source skip (no new snapshot), changed-source correction (exactly one new snapshot), other-date preserved.
- [x] **Shuffle-plan evidence** — gold `groupBy` executed plan + `Exchange` observation recorded as learning evidence, not a performance claim.
- [x] **Thin Airflow wrapper** — single-task DAG calls one validated CLI; `max_active_runs=1`; no transform logic in the DAG body.
- [ ] **Cluster/distributed Spark, performance/throughput claims** — intentionally not implemented.

## Scope: CORE vs OPTIONAL

- **CORE** (the thesis): medallion pipeline · EAV mini · quality checks · catalog/lineage · local Spark/Iceberg · bounded Kafka K1/K1.5.
- **OPTIONAL** (only if a specific interview makes it relevant — e.g. Labrador-style): AI Dataset QA · RAG/vectorDB/LLM-preprocessing.

## Design Strategy

Workspace-level principle: [`../../DATA_PLATFORM_DESIGN_PRINCIPLES.md`](../../DATA_PLATFORM_DESIGN_PRINCIPLES.md).

The recurring JD gap is not only missing tool exposure. It is the ability to design a data platform as an operating system for data:

- batch/streaming intake
- identity, schema, version, partition, and freshness metadata
- quality and reconciliation gates
- lineage and catalog records
- mart/gold modeling grain
- retry, backfill, late data, duplicate input, and failure recovery
- operator visibility through logs, metrics, alerts, and run records

Future work should therefore follow **deep design + small executable slice**:

```text
real-service scenario -> state changes -> metadata contract -> table/file/API design
-> minimal CLI/API implementation -> tests -> README/DESIGN/ROADMAP claim check
```

RAG, Kafka, Spark, dbt, and monitoring are valid design topics when the scenario calls for them. Pull them into implementation only when the system problem is clear and the slice can prove a narrow path.

### BACKLOG (frozen — do not pull forward)
CORE-backlog:
- [x] **Deep design: Kafka K1 raw ingestion** — scenario, question bank, identity/offset/replay/failure boundaries, and first-slice scope reviewed before implementation.
- [ ] **Full Spark/Iceberg translation** — optional future work: swap the full `transform_*` engine to Spark and store more layers as Iceberg/Delta. The current evidence is only a single-gold-table walking skeleton.
- [ ] **Runtime Mongo verification** — blocked here (no Docker engine). Mongo path covered by `mongomock`.
- [x] **Runtime Airflow trigger verification** — local Airflow 3.3.0 `dags test` verified the CLI wrapper; local `standalone` scheduler/LocalExecutor verified the Spark/Iceberg wrapper.
- [ ] **Production Airflow scheduler/worker/webserver deployment** — not implemented.
- [ ] **Task split** — `bronze_task -> silver_task -> gold_task -> quality_task -> catalog_task` after the one-task wrapper is stable.
- [ ] **Graceful null/bad-row quarantine** — manufacturing `transform_silver` strict cast still fails fast (EAV already handles this gracefully).

OPTIONAL-backlog (do NOT implement until an interview requires it):
- [ ] **AI Dataset QA slice** — text/sample dataset ingest -> duplicate/empty/null/PII-mock checks -> label distribution -> train/validation split manifest -> dataset version manifest -> quality report -> catalog/lineage.
- [ ] **RAG / vectorDB / LLM-preprocessing** — framed as dataset quality/version/PII/distribution discipline before training/RAG, not building a vector store.

The optional slices reuse the same `ingest → quality → catalog/lineage` spine. The project is explainable as Lakehouse/Data Mart/modeling/quality for SK/CJ/KakaoBank-style roles, and (via the optional slice) as AI training-data quality/governance for Labrador Labs-style roles.

## Phase 3 — industrial scenarios (scenario-led)

Phase 3 is organized by **operator scenario and failure pressure**, not by a technology list. Implemented facts stay in the Phase 2 sections above; this section only separates what is proven, what is proposed, and what is deliberately distant.

### Implemented foundation (already proven above)

- [x] **Bounded Kafka raw landing (K1) and landing-to-batch bridge (K1.5)** — see `### Kafka raw ingestion — K1`.
- [x] **Spark machine-event batch (S7)** with Python parity and quality-gated Iceberg publish — see `### Spark machine-event batch — S7`.
- [x] **Edge/cloud disconnection and recovery (S8)** — immutable sealed edge spool, replay through the existing local Kafka/K1 landing, downstream batch blocked until the sealed sequence range is fully recovered. Synthetic, local, bounded, single machine/session/partition simulation. Slice: [`learn/system-design/slices/08-edge-cloud-recovery.ko.md`](learn/system-design/slices/08-edge-cloud-recovery.ko.md).

### Proposed next scenarios (not implemented)

Derived from operator scenarios and checked against official industrial-platform documentation (see `BENCHMARKS.md` §6). Each stays `Proposed` until a bounded slice is designed and verified.

- [ ] **Sensor/tag/unit/schema replacement** — reuses the EAV mapping config and schema-drift check.
- [ ] **Suspicious quality metric traced back to source/telemetry** — extends the operator evidence report.
- [ ] **Late/out-of-order telemetry and sequence gap** — only if a real late-data/window pressure is named.
- [ ] **Asset/time-series/document contextualization** — cross-source identity resolution, reduced to this project's scale.

### Backlog / Unknown (distant — do not pull forward)

- [ ] Simulated **ROS2 bag / MCAP-ish** ingest.
- [ ] Real PLC/sensor/robot source; OPC UA / MQTT / ROS 2 / DDS integration.
- [ ] Edge gateway or disconnected durable buffer as a product-grade component.
- [ ] Continuous/event-time streaming, watermarks, Flink or Spark Structured Streaming.
- [ ] Asset hierarchy / Unified Namespace / digital twin.
- [ ] Anomaly model, predictive maintenance, closed-loop control.
- [ ] Production / HA / cluster operation.

---
*Principle: each phase ships something explainable. Don't start the next phase until the active phase's Done criteria are checked.*
