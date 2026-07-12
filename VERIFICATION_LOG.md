# Verification Log

## 2026-06-30 — Phase 2 Slice 1 Initial Implementation

Scope:

- Synthetic manufacturing CSV -> bronze -> silver -> gold -> quality -> catalog/lineage.
- Airflow wrapper added as a single CLI-triggering task.

Commands:

```bash
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-lakehouse-json-final
docker compose up -d
python -c "import airflow; print(airflow.__version__)"
```

Results:

```text
pytest: 8 passed
json CLI: passed, quality_passed=true
docker compose: failed because Docker Desktop engine was unavailable
airflow import: failed because Airflow was not installed
```

Verified:

- [x] Tests passed.
- [x] JSON CLI path checked.
- [x] Docs updated.
- [x] Runtime Mongo/Airflow blockers documented.

## 2026-06-30 — Phase 2 Slice 1 Hardening

Scope:

- Quality suite hardening.
- Schema drift check.
- Transform/IO separation.
- Idempotency for same dataset/date/source hash.
- BENCHMARKS.md.

Commands:

```bash
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-review-cli
```

Results:

```text
pytest: 19 passed
json CLI: passed, quality_passed=true, status=processed
```

Verified:

- [x] Tests passed.
- [x] JSON CLI path checked.
- [x] Docs updated.
- [x] Runtime Mongo/Airflow blockers remain documented.

Notes:

- Mongo path is covered by mongomock tests.
- Runtime Mongo remains unverified until Docker is available.
- Runtime Airflow remains unverified until Airflow is installed.

## 2026-06-30 — Phase 2 Slice 1 Hardening — pre-Codex self-audit

Scope:

- Self-audit only (no new feature, no scope expansion). Verify the hardening claims hold before Codex review.
- Found + fixed one real claim-code gap: schema-drift detection.

Findings:

- **schema_hash was computed from fixed `REQUIRED_COLUMNS`, not the actual CSV header.** Empirically reproduced: adding a column left `schema_hash` unchanged (`f46898d60edf` == `f46898d60edf`), so an added/removed column was invisible to drift detection — while DESIGN §3 / README claimed "columns or inferred types change" is detected. Fix: `read_rows` now returns the actual header; `schema_hash = hash_schema(infer_schema(columns, rows))`. After fix, the added-column case changes the hash (`f46898d60edf` -> `1c48ef0705ee`) and surfaces as `schema_drift = warn`.
- Idempotency Mongo vs JSON backends are semantically equivalent (both gate on `dataset_id + business_date + source_hash + quality.passed`); JSON `latest_successful_run.json` (drift baseline) and `business_date=*.json` (idempotency) are written only on success and do not interfere (reuse only bumps the per-date file).
- Quality `row_count_source_to_silver` couples to the natural-key *contract* (`NATURAL_KEY_COLUMNS`), not to `transform_silver`'s implementation: `expected` is recomputed independently from source, so it catches real row loss while treating date-filtering/dedup as expected.

Commands:

```bash
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-claude-self-audit
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-claude-self-audit  # second run
```

Results:

```text
pytest: 23 passed (was 19; +4: added-column drift, changed-source-same-date new run, not_null fail, freshness/parseable fail)
json CLI run 1: status=processed, quality_passed=true, schema_hash=f46898d60edf
json CLI run 2: status=skipped   (idempotent reuse confirmed)
```

Verified:

- [x] Tests passed (23).
- [x] JSON CLI path checked; second run is `status="skipped"`.
- [x] Docs updated (README/ROADMAP/DESIGN precision on actual-header schema_hash).
- [x] Runtime Mongo/Airflow blockers remain documented (unchanged).

Notes:

- Closed gap: schema-drift detection for added/removed columns (code raised to meet the existing doc claim; docs not lowered).
- Remaining (BACKLOG, unchanged): graceful null/bad-row quarantine (strict numeric cast still fails fast at transform); runtime Mongo (no Docker) / runtime Airflow (not installed); Spark/Iceberg; AI Dataset QA.
- Structural note for Codex: `freshness_business_date`'s off-partition branch is unreachable by construction (silver is filtered to the active date), so it acts as a transform regression guard + ISO-validity check — the parseable branch is the only real-input failure path (tested).

## 2026-06-30 — Phase 2 EAV mini slice (core)

Scope:

- New CORE slice: multiple wide formats → JSON mapping config → EAV (long) → gold pivot/aggregate → quality → catalog/lineage. Reuses the Slice 1 spine (idempotency, schema-drift, catalog, dbt-style quality); no fork, no new project.
- Restated core vs optional: CORE = medallion · EAV · quality · catalog/lineage · Spark/Iceberg; OPTIONAL = AI Dataset QA · RAG/vectorDB (deferred until an interview needs it; NOT implemented).

Constraints honored:

- Clean-room: declined to read company `collector/excel`; built from the public-safe abstracted plan (Slice 6 notes) + standard EAV pattern. No company code/data/names/schemas/file names used. Fully synthetic 3-format data + JSON mapping configs.
- EAV claim framed as "operated/improved professionally, implemented personally"; file_id idempotency = own re-design. Airflow DAG untouched (still a wrapper). AI QA/RAG not implemented.

New files:

- `src/manufacturing_data_platform/pipeline/eav.py`, `run_eav.py`, `sample_eav.py`
- `config/eav_mappings/{plant_a,plant_b,line_c}.json`
- `tests/test_eav_pipeline.py`
- `.gitignore` (+`data/lakehouse_eav/`, `data/raw/eav/`)

Commands:

```bash
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run_eav --catalog-backend json --output-dir /tmp/manufacturing-mini-eav-demo
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run_eav --catalog-backend json --output-dir /tmp/manufacturing-mini-eav-demo  # second run
```

Results:

```text
pytest: 33 passed (was 23; +10 EAV: transforms, unit conversion, run/json, conservation, config-driven new format, idempotency, mapping_coverage/value_type/unmapped-warn failures)
EAV CLI run 1: status=processed, quality_passed=true (9 checks pass)
EAV CLI run 2: status=skipped   (idempotent reuse confirmed)
gold: 3 formats unified; MC-B1 temp 55.0C (from 131F), pressure 100.0kPa (from 1.0 bar); EQP-A1 units 220 (sum of 2 readings), temp 56.0 (avg)
conservation: EAV units 540 == gold units 540
```

Verified:

- [x] Tests passed (33).
- [x] EAV JSON CLI path checked; second run is `status="skipped"`.
- [x] Docs updated (README/ROADMAP/DESIGN/BENCHMARKS + this log + personal phase2-plan).
- [x] Runtime Mongo/Airflow blockers remain documented (unchanged).

Notes:

- Closed: data-modeling (EAV) + multi-format intake gap is now implemented evidence, not just a plan.
- EAV uses graceful value handling (unparseable → `value=None` + `value_type_valid` fail), unlike the manufacturing slice's strict fail-fast — documented as an intentional difference.
- For Codex: confirm (1) clean-room boundary (no company artifacts leaked), (2) EAV claim wording in README/DESIGN is "operated/improved vs implemented", (3) `eav.py` reuse of lakehouse helpers is correct for a different `dataset_id`, (4) scope held — AI QA/RAG remain unimplemented optional-backlog.

## 2026-07-08 — Publication readiness check

Scope:

- Public-readiness verification for the current mini portfolio repo.
- No new feature scope. Confirm what is implemented/test-covered vs what remains backlog or runtime-unverified.

Commands:

```bash
rg -n -i "(api[_-]?key|access[_-]?key|secret|token|password|passwd|private[_-]?key|mongodb\\+srv|Bearer |AKIA|BEGIN RSA|BEGIN OPENSSH|client_secret|refresh_token)" --glob '!**/.venv/**' --glob '!**/__pycache__/**' --glob '!**/.pytest_cache/**' --glob '!PUBLICATION_CHECKLIST.md' .
path/privacy wording scan from `PUBLICATION_CHECKLIST.md`
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-publication-cli
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run_eav --catalog-backend json --output-dir /tmp/manufacturing-mini-publication-eav-cli
```

Results:

```text
secret scan: no matches after excluding PUBLICATION_CHECKLIST.md self-match
private/company/customer path scan: no matches after excluding PUBLICATION_CHECKLIST.md self-match
pytest: 33 passed
lakehouse JSON CLI: passed, status=processed, quality_passed=true
EAV JSON CLI: passed, status=processed, quality_passed=true
```

Verified:

- [x] Tests passed (33).
- [x] Lakehouse JSON CLI path checked.
- [x] EAV JSON CLI path checked.
- [x] README/ROADMAP keep Spark/Iceberg/Kafka as backlog, not implemented.
- [x] Runtime Mongo/Airflow gaps remain documented.

Notes:

- Current public-safe claim: synthetic CSV/EAV mini data platform with catalog/version metadata, medallion pipeline, dbt-style quality checks, path-level lineage records, schema drift warning, and source-hash/file-hash idempotency.
- Do not claim: production lakehouse, Spark/Iceberg implementation, Kafka streaming, real Mongo runtime verification, or real Airflow runtime verification.
- The repo still has separate uncommitted learning-note renames/edits under `learn/system-design/`; review those before final public push.

## 2026-07-09 — Process/docs alignment after Claude charter audit

Scope:

- Accept Claude audit items that do not require new runtime features.
- Re-number system-design docs into a stable reading order.
- Add missing gold-grain and operator-debugging/RCA design evidence.
- Clarify that current public evidence is manufacturing-style/tabular synthetic data, not a machine/session source slice.
- Clarify that `freshness_business_date` is date/partition validity, not age-based source freshness.

Commands:

```bash
rg -n "00-system-scenario|01-source-contract|02-source-contract|01-slice2-question-map|03-slice2-spark-iceberg-shift|04-iceberg-spark-mini-primer" learn README.md <local blog drafts README> <local publication ledger>
pytest
```

Results:

```text
old filename/cross-reference scan: no matches
pytest: 33 passed
```

Verified:

- [x] `learn/system-design/README.md` now starts from service purpose charter, then scenario seed, question map, decision, test, implementation.
- [x] System-design docs use stable names. Current layout keeps root docs thin and moves scenario/source/slice details under `scenarios/`, `source-contracts/`, and `slices/`.
- [x] Added `learn/reference-decisions/gold-grain.md`.
- [x] Added `learn/system-design/scenarios/02-operator-debugging-wrong-gold.md`.
- [x] Blog ledger moved Iceberg post to B5; B4 is now operator debugging / RCA.
- [x] README claim boundary says current external description should be manufacturing-style/tabular until a machine/session source slice exists.

Claim/log consistency:

- B1 evidence remains valid: idempotent rerun tests (`test_rerun_same_source_and_date_is_skipped_mongo`, `test_rerun_same_source_and_date_is_skipped_json`) plus 2026-07-08 CLI check.
- B2 evidence remains valid: schema drift self-audit on 2026-06-30 and schema drift tests.
- B3 evidence remains valid: EAV tests and 2026-07-08 EAV CLI check.

Notes:

- No Spark/Iceberg implementation was added.
- No machine/session-specific ROS2/MCAP/session/sensor slice was added.
- Next recommended implementation slice: read-only operator evidence report for suspicious gold metrics.

## 2026-07-09 — Public rename to manufacturing-data-platform-mini

Scope:

- Rename public project identity to `manufacturing-data-platform-mini`.
- Rename Python package/import path to `manufacturing_data_platform`.
- Rename Airflow wrapper to `manufacturing_lakehouse_daily`.
- Rename synthetic source path to `data/raw/manufacturing_events.csv`.
- Rename source contract equipment column to `machine_id`.
- Update workspace/blog/ledger references so future sessions start from the new name.

Commands:

```bash
legacy-name/package/source scan across repo, blog drafts, ledger, bootstrap, and playbook
legacy domain wording scan across repo, blog drafts, ledger, bootstrap, and playbook
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-rename-cli
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run_eav --catalog-backend json --output-dir /tmp/manufacturing-mini-rename-eav-cli
```

Results:

```text
legacy name/package/source scan: no matches
legacy domain wording scan: no matches
pytest: 33 passed
lakehouse JSON CLI: passed, status=processed, quality_passed=true, dataset_id=manufacturing_daily_metrics
EAV JSON CLI: passed, status=processed, quality_passed=true, dataset_id=manufacturing_wide_eav
```

Verified:

- [x] README, docs, blog drafts, ledger, and session bootstrap now use `manufacturing-data-platform-mini`.
- [x] Runtime commands now use `manufacturing_data_platform`.
- [x] Synthetic source schema now uses `machine_id`.
- [x] Current public-safe name matches current evidence: manufacturing-style/tabular synthetic data platform mini.

Notes:

- This was a naming/claim-boundary correction, not a new feature.
- Existing DEV.to B1 draft URL was created before this rename; update or recreate the external draft before publishing.

## 2026-07-10 — Operator evidence report slice

Scope:

- Implement a read-only operator report over the JSON catalog state.
- Exercise the existing catalog/lineage/quality evidence for a suspicious gold metric scenario.
- Keep the claim boundary explicit: path-level lineage only, not column-level lineage or OpenLineage integration.

Commands:

```bash
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-operator-report-cli
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.operator_report --output-dir /tmp/manufacturing-mini-operator-report-cli --business-date 2026-06-29
```

Results:

```text
pytest: 35 passed
lakehouse JSON CLI: passed, status=processed, quality_passed=true
operator evidence report CLI: passed
```

Verified:

- [x] Added `src/manufacturing_data_platform/pipeline/operator_report.py`.
- [x] Added `tests/test_operator_report.py`.
- [x] Report returns gold grain, run identity, source/schema hashes, stats, quality summary, and lineage trace.
- [x] Report includes explicit claim boundary: no column-level lineage, no OpenLineage backend, no production incident workflow.
- [x] README and publication checklist include the operator report command.

Notes:

- This closes the first operator-debugging/RCA walkthrough without adding Spark/Iceberg scope.
- Next portfolio artifact can be B4: gold 숫자가 이상할 때 source_hash, quality, lineage로 원인 좁히기.

## 2026-07-10 — B2 schema drift publication evidence check

Scope:

- Verify the code/test evidence behind the B2 schema drift blog draft.
- Confirm the public claim boundary: actual-header `schema_hash`, `schema_drift=warn`, required-column fast failure, no Iceberg/Delta schema evolution implementation.

Commands:

```bash
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-b2-schema-drift-check
```

Results:

```text
pytest: 35 passed
lakehouse JSON CLI: passed, status=processed, quality_passed=true
```

Verified:

- [x] `test_schema_drift_helper_states` covers baseline/stable/warn helper states.
- [x] `test_schema_drift_warns_against_previous_successful_run` covers warn policy without failing the run.
- [x] `test_schema_stable_when_schema_unchanged_across_dates` covers stable schema across different source content/date.
- [x] `test_schema_drift_warns_on_added_column` covers the actual-header bug fix for added columns such as `operator_id`.
- [x] B2 blog wording keeps Iceberg/Delta schema evolution as design-only / future direction.

Notes:

- This is a publication evidence check, not a new feature slice.
- Current missing required-column behavior is `ValueError` fast failure, not a structured quality report.

## 2026-07-10 — B3 EAV publication evidence check

Scope:

- Verify the code/test evidence behind the B3 wide CSV -> EAV -> gold blog draft.
- Confirm the public claim boundary: clean-room synthetic data, config-driven mapping, no company/customer schemas, no Spark/Iceberg/Kafka implementation in this slice.

Commands:

```bash
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run_eav --catalog-backend json --output-dir /tmp/manufacturing-mini-b3-eav-check
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run_eav --catalog-backend json --output-dir /tmp/manufacturing-mini-b3-eav-check
```

Results:

```text
pytest: 35 passed
EAV JSON CLI run 1: status=processed, quality_passed=true, dataset_id=manufacturing_wide_eav
EAV JSON CLI run 2: status=skipped, quality_passed=true, dataset_id=manufacturing_wide_eav
gold_rows=4, gold_units_total=540, gold_defects_total=12
lineage_layers=bronze -> silver_eav -> gold
```

Verified:

- [x] `test_transform_to_eav_maps_and_converts_units` covers deterministic unit conversion.
- [x] `test_transform_to_eav_captures_type_errors_gracefully` covers bad-value capture as quality evidence, not transform crash.
- [x] `test_transform_eav_to_gold_aggregates_sum_and_avg` covers sum/average rollups.
- [x] `test_eav_run_passes_and_unifies_three_formats` covers the 3-format synthetic sample and conservation.
- [x] `test_new_format_is_onboarded_by_adding_one_config` covers adding `vendor_d.csv` + `vendor_d.json` without pipeline code change.
- [x] `test_eav_idempotent_rerun_is_skipped` covers repeat-run idempotency.

Notes:

- `source_file_id` is each file-content hash; run-level idempotency uses the combined `source_hash` across source files.
- This is publication evidence for B3, not a new feature slice.

## 2026-07-11 — Airflow wrapper command contract

Scope:

- Make the Airflow wrapper command testable without requiring Airflow to be installed.
- Keep business logic in the lakehouse CLI/pipeline module, not inside the DAG body.
- Verify the same concrete CLI command still runs with the JSON catalog backend.

Commands:

```bash
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --business-date 2026-06-29 --raw-path data/raw/manufacturing_events.csv --output-dir /tmp/manufacturing-mini-airflow-contract-check --catalog-backend json
```

Results:

```text
pytest: 38 passed
lakehouse JSON CLI: passed, status=processed, quality_passed=true
```

Verified:

- [x] Added `manufacturing_data_platform.orchestration.build_lakehouse_cli_command`.
- [x] Airflow DAG uses the shared command builder instead of owning business logic.
- [x] Command builder supports Airflow Jinja runtime parameters for `business_date`, `raw_path`, `output_dir`, and `catalog_backend`.
- [x] Command builder rejects invalid concrete catalog backends.

Notes:

- This verifies the Airflow wrapper command contract, not Airflow runtime execution.
- Airflow is not installed in this environment; DAG import/trigger under a real Airflow runtime remains pending.

## 2026-07-12 — Airflow local runtime wrapper verification

Scope:

- Verify the existing one-task Airflow DAG in a real local Airflow runtime.
- Keep Airflow in an isolated `/tmp` virtualenv so the project `.venv` remains lightweight.
- Prove local DAG import, task discovery, command rendering, and `dags test` execution.
- Keep production scheduler/worker/webserver deployment out of scope.

Commands:

```bash
python -m venv /tmp/manufacturing-mini-airflow-venv
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install --upgrade pip
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install "apache-airflow==3.3.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.10.txt"

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow db migrate

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags list

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow tasks list manufacturing_lakehouse_daily

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow tasks render manufacturing_lakehouse_daily run_pipeline_task 2026-06-29

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags test manufacturing_lakehouse_daily 2026-06-29 \
  -c '{"business_date":"2026-06-29","raw_path":"data/raw/manufacturing_events.csv","output_dir":"/tmp/manufacturing-mini-airflow-runtime","catalog_backend":"json"}'

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags test manufacturing_lakehouse_daily 2026-06-29 \
  -c '{"business_date":"2026-06-29","raw_path":"data/raw/manufacturing_events.csv","output_dir":"/tmp/manufacturing-mini-airflow-runtime","catalog_backend":"json"}'

python -m pytest -q
```

Results:

```text
airflow version: 3.3.0
airflow db migrate: passed
airflow dags list: manufacturing_lakehouse_daily loaded
airflow tasks list: run_pipeline_task
airflow tasks render: renders the same lakehouse CLI command
airflow dags test run 1: DagRun success; BashOperator command exited with return code 0
pipeline result run 1: status=processed, quality_passed=true, catalog_backend=json
airflow dags test run 2 with same conf/output state: DagRun success; BashOperator command exited with return code 0
pipeline result run 2: status=skipped, quality_passed=true, catalog_backend=json
pytest: 40 passed
```

Verified:

- [x] Airflow runtime can import the DAG.
- [x] Airflow runtime sees the expected task.
- [x] `dag_run.conf` passes `business_date`, `raw_path`, `output_dir`, and `catalog_backend`.
- [x] The BashOperator executes the same `manufacturing_data_platform.pipeline.run` CLI entrypoint.
- [x] Re-running the same Airflow DAG test uses the pipeline idempotency gate and returns `status="skipped"`.
- [x] Business logic remains outside the DAG body.

Notes:

- Airflow was installed with the official 3.3.0 Python 3.10 constraints.
- `requirements-airflow.txt` records the optional runtime verification dependency.
- This verifies local Airflow wrapper execution, not production scheduler/worker/webserver deployment.
- The BashOperator uses the worker shell's `python`; production packaging/worker image dependency management is not verified.
- At this point, Airflow-triggered Spark/Iceberg runtime was still unverified; it is closed by the next section.

## 2026-07-12 — Airflow-triggered Spark/Iceberg skeleton

Scope:

- Add a local Airflow DAG that triggers the existing Spark/Iceberg skeleton CLI.
- Keep SparkSession/Iceberg write logic outside the DAG body.
- Verify local Airflow runtime execution with `airflow dags test`.
- Prove the same Iceberg evidence still holds when launched through Airflow.
- Keep production scheduler/worker/webserver deployment and cluster Spark out of scope.

New files:

- `dags/manufacturing_iceberg_skeleton.py`
- `learn/system-design/slices/03-airflow-spark-iceberg-runtime.ko.md`

Changed files:

- `src/manufacturing_data_platform/orchestration.py`
- `tests/test_orchestration.py`
- `src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py`

Commands:

```bash
python -m pytest tests/test_orchestration.py -q
python -m pytest -q

PYTHONPATH=src /tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements-airflow.txt
PYTHONPATH=src /tmp/manufacturing-mini-airflow-venv/bin/python -m pip install pytest
PYTHONPATH=src /tmp/manufacturing-mini-airflow-venv/bin/python -m pytest tests/test_airflow_dags.py -q

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags list

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow tasks list manufacturing_iceberg_skeleton

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow tasks render manufacturing_iceberg_skeleton run_spark_iceberg_skeleton_task 2026-06-29

rm -rf /tmp/manufacturing-mini-airflow-iceberg-warehouse /tmp/manufacturing-mini-airflow-iceberg-evidence

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags test manufacturing_iceberg_skeleton 2026-06-29 \
  -c '{"warehouse":"/tmp/manufacturing-mini-airflow-iceberg-warehouse","output_dir":"/tmp/manufacturing-mini-airflow-iceberg-evidence"}'
```

Results:

```text
orchestration tests: 5 passed
pytest: 42 passed, 3 skipped without Airflow in the base environment
optional Airflow DagBag tests: 3 passed
airflow tasks render: renders Spark/Iceberg skeleton CLI command
airflow dags list: manufacturing_iceberg_skeleton loaded
airflow tasks list: run_spark_iceberg_skeleton_task
airflow dags test: DagRun success; BashOperator command exited with return code 0
Iceberg runtime coordinate: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
run statuses inside evidence: processed -> skipped -> processed
snapshot_increment: 1
same_source_created_snapshot: false
evidence files: run_snapshot_map.json, current_gold.json, snapshot_comparison.json
```

Verified:

- [x] Airflow runtime can import the Spark/Iceberg DAG.
- [x] Airflow runtime sees `run_spark_iceberg_skeleton_task`.
- [x] Optional Airflow DagBag tests cover both project DAGs when Airflow is installed.
- [x] The BashOperator executes `manufacturing_data_platform.pipeline.spark_iceberg_skeleton`.
- [x] Spark/Iceberg creates a local Iceberg table through the Airflow-triggered task.
- [x] Same-source retry still creates no new snapshot.
- [x] Corrected same-`business_date` input creates a new snapshot and overwrites only the target partition.
- [x] Other `business_date` partition remains unchanged.

Notes:

- This verifies local Airflow `dags test` orchestration of the Spark/Iceberg skeleton.
- `airflow dags test` verifies local DAG import/task wiring/command execution, not scheduler, queue, executor, worker, or webserver behavior.
- It does not verify production scheduler/worker/webserver deployment.
- It does not verify cluster Spark or a full Spark medallion pipeline.
- The BashOperator still uses the worker shell's `python`; production dependency packaging remains out of scope.

## 2026-07-12 — Airflow standalone scheduler run for Spark/Iceberg skeleton

Scope:

- Verify that Airflow can be started locally as `airflow standalone`.
- Move beyond `dags test` by triggering `manufacturing_iceberg_skeleton` through the scheduler/LocalExecutor path.
- Keep the claim bounded to development-only local standalone, not production deployment.

Commands:

```bash
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements-airflow.txt
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements.txt -r requirements-spark.txt

rm -rf /tmp/manufacturing-mini-airflow-standalone-home \
  /tmp/manufacturing-mini-airflow-standalone-iceberg-warehouse \
  /tmp/manufacturing-mini-airflow-standalone-iceberg-evidence

export AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-standalone-home
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH=src
export PATH="/tmp/manufacturing-mini-airflow-venv/bin:$PATH"

airflow standalone

curl -s -o /tmp/manufacturing-mini-airflow-http-check.txt \
  -w "%{http_code}\n" http://127.0.0.1:8080/

airflow dags list
airflow dags list-import-errors
airflow dags unpause manufacturing_iceberg_skeleton
airflow dags trigger manufacturing_iceberg_skeleton \
  --run-id standalone_iceberg_20260712_0818 \
  --conf '{"warehouse":"/tmp/manufacturing-mini-airflow-standalone-iceberg-warehouse","output_dir":"/tmp/manufacturing-mini-airflow-standalone-iceberg-evidence","clean":true}'

scripts/verify_airflow_standalone.sh
```

Results:

```text
airflow standalone: started api-server on 0.0.0.0:8080
curl 127.0.0.1:8080: 200
standalone components observed: api-server, scheduler, dag-processor, triggerer, LocalExecutor workers
dag parse: manufacturing_lakehouse_daily and manufacturing_iceberg_skeleton loaded, no import errors
manual scheduler run: standalone_iceberg_20260712_0818
dag_run state: success
task state: success
scripted state transition: queued/None -> running/queued -> running/running -> success/success
executor: LocalExecutor
task command exit code: 0
evidence files: run_snapshot_map.json, current_gold.json, snapshot_comparison.json
snapshot_increment: 1
same_source_created_snapshot: false
target business_date row count: 1
current gold rows: 2026-06-29 corrected row + 2026-06-30 preserved row
```

Verified:

- [x] Airflow 3.3.0 local `standalone` can start the API server, scheduler, dag processor, triggerer, and LocalExecutor workers.
- [x] The project DAGs are parsed by the standalone dag processor.
- [x] `manufacturing_iceberg_skeleton` can be manually triggered through the scheduler path.
- [x] The LocalExecutor worker runs the BashOperator command to completion.
- [x] The Spark/Iceberg skeleton still produces the same partition-overwrite evidence under scheduler execution.
- [x] `scripts/verify_airflow_standalone.sh` reproduces startup, trigger, state polling, evidence assertions, and process cleanup.

Notes:

- First standalone attempt failed because the venv `bin` directory was not on `PATH`; `standalone` subprocesses invoke `airflow` by name.
- First scheduler-triggered task failed with `ModuleNotFoundError: No module named 'pymongo'` until the Airflow worker venv also installed `requirements.txt`.
- Scheduler-triggered Spark also requires `requirements-spark.txt` in the same worker venv.
- This is a development-only local standalone check. It does not verify production Airflow deployment, HA scheduling, a distributed executor, queue/worker fleet behavior, auth hardening, or cluster Spark.

## 2026-07-11 — Spark/Iceberg single-gold-table walking skeleton

Scope:

- Implement the smallest Spark/Iceberg slice for the correction-rerun scenario.
- Keep scope to one local Iceberg gold table: `local.db.gold_daily_metrics`.
- Prove `business_date` partition overwrite, D2 partition preservation, snapshot evidence, and same-`source_hash` rerun without a new snapshot.

New files:

- `requirements-spark.txt`
- `src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py`
- `tests/test_spark_iceberg_skeleton.py`
- `learn/system-design/slices/spark-iceberg-partition-overwrite/05-version-pin.md`

Commands:

```bash
python -m pip install -r requirements-spark.txt
pytest tests/test_spark_iceberg_skeleton.py -q
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_iceberg_skeleton \
  --warehouse /tmp/manufacturing-mini-iceberg-warehouse \
  --output-dir /tmp/manufacturing-mini-iceberg-evidence \
  --clean
```

Results:

```text
pip install requirements-spark.txt: passed (pyspark==3.5.8)
spark skeleton tests: 2 passed
pytest: 40 passed
spark CLI: passed
Iceberg runtime coordinate: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
target partition row count: 1
snapshot count: 1 -> 2 after correction
same source rerun created snapshot: false
other business_date partition preserved: true
```

Verified:

- [x] Local SparkSession starts with Iceberg extensions.
- [x] Local hadoop catalog creates and reads an Iceberg table.
- [x] Corrected `business_date` partition is overwritten without duplicates.
- [x] Another `business_date` partition remains unchanged.
- [x] `run_id` and numeric `snapshot_id` are recorded separately.
- [x] Same `source_hash` rerun does not create a new snapshot.

Claim boundary:

- Allowed: local Spark/Iceberg single-gold-table walking skeleton with `business_date` partition overwrite and snapshot evidence.
- Not allowed: full Spark medallion pipeline, production lakehouse, Iceberg rollback system, concurrent writer handling, production Airflow-triggered Spark runtime.
