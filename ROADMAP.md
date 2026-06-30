# ROADMAP

## Phase 1 v0 — MongoDB catalog gate

This is the active scope. It closes the closest Robotis gap first: **NoSQL/MongoDB + metadata catalog**.

- [x] **MongoDB metadata catalog** — ingest a sensor-ish CSV and register `schema · row/null stats · source · ingested_at`.
- [x] **Version manifest** — `dataset_id · version · source_hash · schema_hash · ingested_at · row_count`.
- [x] **FastAPI catalog endpoints** — `GET /datasets`, `GET /datasets/{id}`.
- [x] **README design rationale** — architecture, tradeoffs, run commands, and Done checklist.
- [ ] **Runtime Mongo verification** — `docker compose up` + real Mongo ingest. Blocked in this environment because Docker Desktop engine is unavailable.

End of v0 = the catalog loop is implemented and test-covered. The cover-letter claim can move from "implementation started" to "catalog/version manifest implemented" after runtime Mongo is verified on a machine with Docker available.

## Phase 1 v0.5 — DaaS extract

- [ ] `GET /datasets/{id}/extract?version=&columns=` — conditional extract for the Robotis DaaS keyword.

## Phase 1 v1 — orchestration polish

- [ ] Self-hosted Airflow orchestration in Docker Compose: ingest → catalog → version → serve.

This is useful later, but not part of the current MongoDB/catalog gate.

## Phase 2 — Mini Lakehouse

### Slice 1 (implemented)
- [x] **Slice 1 CLI** — synthetic manufacturing CSV -> bronze -> silver -> gold -> quality -> Mongo catalog/lineage.
- [x] **Airflow wrapper** — `dags/robot_lakehouse_daily.py` triggers the CLI as an operational wrapper.

### Slice 1 hardening — NOW (implemented this pass)
Goal: make the claims (data quality, schema drift, idempotency, transform/IO separation) actually true in code.
- [x] **transform/IO separation** — pure `transform_silver` / `transform_gold`; `write_*` does IO only (sets up the Spark engine swap).
- [x] **Quality suite** — dbt-style checks (`not_null`, `unique`, `accepted_values`, range, freshness) + reconciliation that distinguishes filtering/dedup from real row loss. Tautology check removed.
- [x] **Schema drift** — `schema_hash` from the actual CSV header (added/removed columns detected), compared to the previous successful run; `schema_drift` check, policy = `warn`; stored on run/lineage doc.
- [x] **Idempotency** — skip re-run on `dataset_id + business_date + source_hash` with a prior success (safe retry/backfill).
- [x] **BENCHMARKS.md** — reference patterns, JD mapping, anti-benchmark (deliberate exclusions).

### BACKLOG (frozen — do not pull into Slice 1)
- [ ] **Runtime Mongo verification** — blocked here (no Docker engine). Mongo path covered by `mongomock`.
- [ ] **Runtime Airflow trigger verification** — Airflow not installed in this env.
- [ ] **Task split** — `bronze_task -> silver_task -> gold_task -> quality_task -> catalog_task` after the one-task wrapper is stable.
- [ ] **Spark/Iceberg translation** — swap the `transform_*` engine to Spark; store gold as Iceberg/Delta.
- [ ] **Graceful null/bad-row quarantine** — currently strict numeric cast fails fast at transform time.
- [ ] **AI Dataset QA slice** — text/sample dataset ingest -> duplicate/empty/null/PII-mock checks -> label distribution -> train/validation split manifest -> dataset version manifest -> quality report -> catalog/lineage.

The AI Dataset QA slice is a later Phase 2 extension, not a Slice 1 scope increase. The same project should be explainable as Lakehouse/Data Mart/quality for SK/CJ/KakaoBank-style roles and as AI training-data quality/governance for Labrador Labs-style roles.

## Phase 3 — domain / streaming

- [ ] Simulated **ROS2 bag / MCAP-ish** ingest.
- [ ] **Kafka** streaming ingest path.

---
*Principle: each phase ships something explainable. Don't start the next phase until the active phase's Done criteria are checked.*
