# BENCHMARKS.md — what this project references, and what it deliberately leaves out

This project is a **small, honest miniature** of a data platform, not a clone of one.
The point of this file is to make the references explicit so a reviewer can check that
the structure follows recognized patterns — and, just as importantly, to record what was
**intentionally excluded** so the scope stays a slice instead of a "plausible toy."

Principle: **official docs are rules · OSS is structural sense · private/company code is
operational pattern only.** Mix them; never copy. Public-repo code is newly written and
uses fully synthetic manufacturing data.

---

## 1. Official patterns (the rules we follow)

| Reference | What we take | Where it lives in this repo |
|---|---|---|
| **Databricks Medallion architecture** (bronze/silver/gold) | raw → cleaned/typed → aggregated mart layering | `pipeline/lakehouse.py` (`write_bronze`, `transform_silver`, `transform_gold`) |
| **Apache Airflow Best Practices** | task = retryable transaction; derive the window from `business_date`/`data_interval`, never from `now()`; no heavy top-level code; logic outside the DAG | `dags/manufacturing_lakehouse_daily.py` (thin wrapper only), `business_date` passed via `dag_run.conf` / `ds` |
| **Apache Iceberg** (partitioning, schema evolution, snapshots) | partition by `business_date`; treat schema change as evolution, not breakage; idempotent overwrite of a partition | partition layout in `build_paths`; `schema_drift` check (policy = `warn`); idempotency gate. *Engine itself = backlog.* |
| **OpenLineage** (run / job / dataset model) | a run record with input → output datasets and parent layers | `lineage_events` + `lakehouse_runs` docs (`layers[].parents`). *Vocabulary borrowed; full facet spec + backend = backlog.* |
| **dbt generic tests** (`not_null`, `unique`, `accepted_values`, relationships) | the **names and shape** of the quality assertions | `build_quality_checks` — `not_null_required_columns`, `unique_natural_key`, `accepted_values_operation`, plus reconciliation/range/freshness |
| **Great Expectations / Soda** (expectation = expected vs actual + status) | each check is `{name, status, expected, actual, detail}` | `make_check`, `quality_report.json` |

We implement the **model** these tools define; we do not pull them in as dependencies
(see §5). That keeps the slice readable and the data-quality logic inspectable.

---

## 2. OSS structure patterns (the structural sense we borrow)

| Reference | What we take | What we did NOT take |
|---|---|---|
| **OpenMetadata / DataHub** | catalog entity model: a dataset identity vs its versions/runs; lineage as parent links | the graph DB, the UI, ownership/tags/glossary, event streams |
| **DVC / lakeFS** | dataset versioning by content hash; a manifest as the reproducibility unit | branching, atomic commits, data-as-git, a dedicated versioned store |
| **dbt** | model → tests → docs discipline; transforms expressed as testable units | the full DAG compiler, Jinja models, adapters |
| **Kedro / Dagster** | separating pipeline **nodes** (pure transforms) from IO/orchestration | the asset graph runtime, the web UI, the type system |

The single most load-bearing OSS borrow is **node separation** (Kedro/Dagster) +
**model/test split** (dbt): `transform_silver` / `transform_gold` are pure functions, and
`write_*` does IO only. That is what makes the future Spark port an engine swap, not a
rewrite.

---

## 3. JD mapping — keyword → evidence → status

> Honest status labels: **implemented** (code + test) · **partial** (started/coarse) ·
> **backlog** (planned, not built). Showing partial/backlog is intentional — it is the
> difference between a slice and an overclaim.

### SK / CJ / KakaoBank-style (lakehouse / mart / quality / governance)

| JD keyword | Evidence in this repo | Status |
|---|---|---|
| Lakehouse / medallion | `bronze → silver → gold` in `pipeline/lakehouse.py` | implemented |
| Data Mart | `gold/daily_line_metrics.csv` + EAV `entity_daily_metrics.csv` | implemented |
| Data modeling (EAV) / multi-format intake | `pipeline/eav.py`: 3 wide formats → mapping config → EAV long → gold pivot | implemented |
| Schema mapping / harmonization | `config/eav_mappings/*.json` + unit conversions (`f_to_c`, `bar_to_kpa`); new format = add one config | implemented |
| ETL / ELT | CSV ingest → typed/normalized silver → aggregated gold | implemented |
| Data quality | dbt-style suites on both slices + cross-layer reconciliation/conservation | implemented |
| Schema drift / evolution | `schema_drift` check vs previous successful run (policy = warn) | implemented |
| Idempotency / backfill | skip re-run on `dataset_id + business_date + source_hash` | implemented (Airflow backfill = partial) |
| Lineage | `lakehouse_runs` / `lineage_events` with `layers[].parents` | partial (path-level, not column-level) |
| Catalog | Mongo `datasets` / `dataset_versions` (Phase 1) + run/lineage docs | implemented |
| Orchestration | Airflow DAGs as local wrappers around the lakehouse CLI and Spark/Iceberg skeleton CLI | partial (local `dags test` runtime verified; Spark/Iceberg wrapper also verified through development `standalone` scheduler/LocalExecutor; production deployment not claimed) |
| Spark / Iceberg | single gold Iceberg table, `business_date` partition overwrite, snapshot evidence, Airflow-triggered local skeleton | partial (not a full Spark medallion pipeline) |

### Labrador Labs-style (AI training-data QA / governance / LLM preprocessing) — OPTIONAL

> These are **optional-backlog**: pursued only if a Labrador-style interview actually
> happens. Core (medallion · EAV · quality · catalog/lineage · Spark/Iceberg) comes first.

| JD keyword | Planned evidence | Status |
|---|---|---|
| AI dataset QA | reuse the same `ingest → quality → catalog/lineage` spine with a text/label dataset profile | backlog (optional) |
| Labeling / label quality | label distribution + train/validation split manifest | backlog (optional) |
| Sensitive data | PII **mock** detection (rule-based on synthetic data, clearly labelled mock) | backlog (optional) |
| Dataset versioning | dataset version manifest (same hash/manifest discipline as here) | backlog (optional) |
| Spark / Flink / Kafka / Iceberg | streaming + engine items | backlog (Spark/Iceberg = core; streaming = Phase 3) |
| RAG / vectorDB / LLM preprocessing | framed as "manage dataset quality/version/PII/distribution **before** it enters training/RAG", not as building a vector store | backlog (optional, scope-limited by design) |

The thesis: **one project, multiple job languages.** The reusable spine is
`ingest → version manifest → quality report → catalog/lineage`. The EAV slice (core) and the
AI-QA slice (optional) change only the dataset profile and the check pack — not the platform —
so they sharpen the thesis instead of forking a second system. EAV also speaks directly to a
**data-modeling** gap, which is why it is core rather than optional.

### EAV reference (data modeling)

EAV (entity–attribute–value) is a standard database-modeling pattern for storing
heterogeneous, sparse attributes under one schema. Here it is implemented clean-room on
synthetic data to harmonize multiple wide formats: each source declares a JSON mapping
(columns → standard fields + unit conversions), wide rows melt to EAV long, then pivot
back to a gold mart. The pattern (config-driven mapping, melt/pivot, conservation check)
is the borrow; the **EAV claim is "operated/improved professionally, implemented
personally"** — see DESIGN §Phase 2 EAV.

---

## 4. Private-code-safe patterns (learned, not copied)

Company Airflow/pipeline code was used **only to learn operational patterns**. No code,
DAG ids, customer identifiers, business names, variable names, file names, or
business-specific logic are copied. The abstracted patterns:

- **Thin DAG, logic outside.** The DAG file assembles tasks and passes config; business
  logic lives in importable functions (`pipeline/` here).
- **Config as a list with enable flags.** Multiple sources/customers expressed as a
  configured list with per-item options — abstracted here to CLI/`dag_run.conf` params.
- **Window from the run, not the clock.** Scheduled runs derive the window from the
  Airflow context (`ds`); manual runs override via trigger config — never `now()` in core
  logic.
- **Retry / timeout / failure handling at the task level.** `retries`, `retry_delay`,
  `execution_timeout`, `catchup`, `max_active_runs` are DAG/task operations.
- **Where to split tasks.** `bronze → silver → gold → quality → catalog` is the natural
  split boundary (kept as one wrapper task for now; split is backlog).

---

## 5. Anti-benchmark — deliberately excluded (and why)

Real platforms have these; this slice does not. Listing them is the scope discipline.

| Excluded | Why it is out |
|---|---|
| Graph lineage **UI** | lineage is stored as run/parent records; a browsable graph is presentation, not the loop. (DataHub/Marquez territory.) |
| Full **governance UI** (ownership, tags, glossary, RBAC) | governance metadata model is acknowledged but a console is a separate product. |
| **Streaming** (Kafka / Flink) | the slice is a daily batch; streaming is Phase 3, not Slice 1. |
| **Branching / atomic commits** (lakeFS) | idempotency here is "skip re-run of the same content"; data-as-git is a heavier model than the slice needs. |
| **Full OpenLineage backend** (Marquez) | we borrow the run/job/dataset *vocabulary*; running a lineage server is backlog. |
| **Distributed compute** (Spark cluster, Iceberg engine) | partitioning/schema-evolution/idempotency are in place so the swap is mechanical; the engine itself is Slice 2. |
| Quality libs as **dependencies** (Great Expectations, Soda) | we implement the expectation *model* so the checks stay inspectable in ~40 lines; adopting the lib is backlog if the suite grows. |
| Generic **mapping DSL / rules engine** (EAV) | mapping is plain JSON (columns → standard + named conversions); a DSL/UI is over-engineering for a mini. |
| **AI Dataset QA / RAG / vectorDB** | OPTIONAL — documented, intentionally **not** implemented; pursued only if a Labrador-style interview happens. |

---

## CORE vs OPTIONAL · NOW vs BACKLOG (current freeze)

**CORE** = medallion · EAV mini · quality · catalog/lineage · Spark/Iceberg.
**OPTIONAL** = AI Dataset QA · RAG/vectorDB (only if an interview makes it relevant).

**NOW (implemented):** Slice 1 medallion + hardening (transform/IO split · dbt-style quality
+ reconciliation · schema-drift warn · idempotent re-run) · **EAV mini (multi-format →
mapping config → EAV → gold pivot + EAV quality suite + file_id idempotency)** · this
BENCHMARKS.md.

**BACKLOG — core (frozen):** Spark engine swap · Iceberg/Delta gold · Airflow task split +
runtime trigger verification · runtime Mongo verification (Docker) · graceful null quarantine
(manufacturing slice) · full OpenLineage/Marquez.

**BACKLOG — optional (do NOT implement until an interview requires it):** AI Dataset QA
slice · RAG/vectorDB/LLM-preprocessing · streaming (Kafka/Flink).

---

*References are public docs/OSS as of 2026-06. This is a personal learning project; not
affiliated with any company. Data is fully synthetic.*
