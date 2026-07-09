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

## Scope: CORE vs OPTIONAL

- **CORE** (the thesis): medallion pipeline · EAV mini · quality checks · catalog/lineage · Spark/Iceberg.
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
- [ ] **Deep design: streaming + batch platform** — design source events/files, Kafka topic shape, bronze/silver/gold boundary, idempotency keys, late data/backfill, quality gates, mart grain, monitoring, and recovery before implementing a Kafka slice.
- [ ] **Spark/Iceberg translation** — swap the `transform_*` engine to Spark; store gold as Iceberg/Delta.
- [ ] **Runtime Mongo verification** — blocked here (no Docker engine). Mongo path covered by `mongomock`.
- [ ] **Runtime Airflow trigger verification** — Airflow not installed in this env.
- [ ] **Task split** — `bronze_task -> silver_task -> gold_task -> quality_task -> catalog_task` after the one-task wrapper is stable.
- [ ] **Graceful null/bad-row quarantine** — manufacturing `transform_silver` strict cast still fails fast (EAV already handles this gracefully).

OPTIONAL-backlog (do NOT implement until an interview requires it):
- [ ] **AI Dataset QA slice** — text/sample dataset ingest -> duplicate/empty/null/PII-mock checks -> label distribution -> train/validation split manifest -> dataset version manifest -> quality report -> catalog/lineage.
- [ ] **RAG / vectorDB / LLM-preprocessing** — framed as dataset quality/version/PII/distribution discipline before training/RAG, not building a vector store.

The optional slices reuse the same `ingest → quality → catalog/lineage` spine. The project is explainable as Lakehouse/Data Mart/modeling/quality for SK/CJ/KakaoBank-style roles, and (via the optional slice) as AI training-data quality/governance for Labrador Labs-style roles.

## Phase 3 — domain / streaming

- [ ] Simulated **ROS2 bag / MCAP-ish** ingest.
- [ ] **Kafka** streaming ingest path.

---
*Principle: each phase ships something explainable. Don't start the next phase until the active phase's Done criteria are checked.*
