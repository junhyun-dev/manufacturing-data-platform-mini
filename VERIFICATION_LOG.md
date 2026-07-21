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

## 2026-07-12 — Lakehouse gold -> Iceberg publish DAG

Scope:

- Connect the implemented JSON-backed lakehouse pipeline to local Iceberg without doing a full Spark rewrite.
- Add a publish CLI that reads the latest successful JSON catalog state for a `business_date`, loads that run's gold CSV, and publishes it to `local.db.gold_daily_metrics`.
- Add a two-task Airflow DAG: `run_lakehouse_task -> publish_gold_to_iceberg_task`.
- Keep Mongo-backed publish lookup, Spark quality checks, production Airflow, and cluster Spark out of scope.

New files:

- `src/manufacturing_data_platform/pipeline/publish_gold_to_iceberg.py`
- `dags/manufacturing_lakehouse_to_iceberg_daily.py`
- `tests/test_publish_gold_to_iceberg.py`
- `learn/system-design/slices/04-lakehouse-to-iceberg-publish.ko.md`

Commands:

```bash
python -m pytest tests/test_orchestration.py tests/test_publish_gold_to_iceberg.py -q
python -m pytest -q

PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run \
  --business-date 2026-06-29 \
  --raw-path data/raw/manufacturing_events.csv \
  --output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg-cli/lakehouse \
  --catalog-backend json

PYTHONPATH=src python -m manufacturing_data_platform.pipeline.publish_gold_to_iceberg \
  --lakehouse-output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg-cli/lakehouse \
  --business-date 2026-06-29 \
  --warehouse /tmp/manufacturing-mini-lakehouse-to-iceberg-cli/warehouse \
  --output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg-cli/evidence \
  --clean

PYTHONPATH=src python -m manufacturing_data_platform.pipeline.publish_gold_to_iceberg \
  --lakehouse-output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg-cli/lakehouse \
  --business-date 2026-06-29 \
  --warehouse /tmp/manufacturing-mini-lakehouse-to-iceberg-cli/warehouse \
  --output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg-cli/evidence

PYTHONPATH=src /tmp/manufacturing-mini-airflow-venv/bin/python -m pytest tests/test_airflow_dags.py -q

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-lakehouse-to-iceberg-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
PATH="/tmp/manufacturing-mini-airflow-venv/bin:$PATH" \
/tmp/manufacturing-mini-airflow-venv/bin/airflow db migrate

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-lakehouse-to-iceberg-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
PATH="/tmp/manufacturing-mini-airflow-venv/bin:$PATH" \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags test manufacturing_lakehouse_to_iceberg_daily 2026-06-29 \
  -c '{"business_date":"2026-06-29","raw_path":"data/raw/manufacturing_events.csv","lakehouse_output_dir":"/tmp/manufacturing-mini-airflow-lakehouse-to-iceberg-run/lakehouse","warehouse":"/tmp/manufacturing-mini-airflow-lakehouse-to-iceberg-run/warehouse","iceberg_output_dir":"/tmp/manufacturing-mini-airflow-lakehouse-to-iceberg-run/evidence"}'
```

Results:

```text
orchestration + publish tests: 11 passed
pytest: 48 passed, 4 skipped
lakehouse JSON CLI: passed, status=processed, quality_passed=true
publish CLI run 1: status=published, snapshot_count=1, operation=overwrite
publish CLI run 2: status=skipped, same gold_snapshot_id, skipped_reason=same lakehouse run already published
optional Airflow DagBag tests: 4 passed
airflow db migrate: passed for fresh local AIRFLOW_HOME
airflow dags test manufacturing_lakehouse_to_iceberg_daily: DagRun success
Airflow task order: run_lakehouse_task -> publish_gold_to_iceberg_task
Airflow publish output: status=published, snapshot_count=1
```

Verified:

- [x] JSON catalog state is the publish source; raw CSV is not re-read by the publish step.
- [x] Only a successful lakehouse run is publishable.
- [x] Gold CSV rows are written to a local Iceberg table with `overwritePartitions()`.
- [x] Same lakehouse run publish retry is skipped without creating a new snapshot.
- [x] Spark optional test covers corrected target partition replacement and other partition preservation.
- [x] Airflow can parse the new two-task DAG when Airflow is installed.
- [x] Local `airflow dags test` runs the lakehouse CLI task and then the Iceberg publish CLI task to success.

Claim boundary:

- Allowed: local JSON-backed lakehouse gold -> local Iceberg publish, two-task local Airflow DAG, `pipeline_run_id -> snapshot_id` evidence, retry publish skip.
- Not allowed: Mongo-backed publish lookup, full Spark/Iceberg medallion pipeline, Spark-based quality suite, production Airflow deployment, cluster Spark, concurrent writer handling, exactly-once catalog/table transaction.

## 2026-07-12 — Spark/Iceberg local setup recheck

Scope:

- Reconfirm today's local Spark/Iceberg setup from the public repo commands.
- Keep the claim bounded to local SparkSession + local Iceberg hadoop catalog.

Commands:

```bash
python -m pip install -r requirements-spark.txt
python -c "import pyspark; print(pyspark.__version__)"

rm -rf /tmp/manufacturing-mini-today-iceberg-warehouse \
  /tmp/manufacturing-mini-today-iceberg-evidence

PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_iceberg_skeleton \
  --warehouse /tmp/manufacturing-mini-today-iceberg-warehouse \
  --output-dir /tmp/manufacturing-mini-today-iceberg-evidence \
  --clean
```

Results:

```text
pip install requirements-spark.txt: already satisfied
pyspark: 3.5.8
Iceberg runtime coordinate: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
spark skeleton CLI: passed
target_partition_row_count: 1
corrected_row_count: 1
snapshot_count: 1 -> 2 after correction
snapshot_increment: 1
same_source_created_snapshot: false
other business_date partition preserved: true
```

Verified:

- [x] PySpark is installed in the current Python environment.
- [x] Spark can resolve the Iceberg runtime jar from the configured coordinate.
- [x] Local Iceberg table creation works.
- [x] Business-date partition overwrite works.
- [x] Same-source retry does not create a new snapshot.

Claim boundary:

- Allowed: local Spark/Iceberg setup and single-gold-table partition overwrite.
- Not allowed: cluster Spark, production lakehouse, full Spark medallion rewrite, Spark-based quality suite.

## 2026-07-13 — Airflow two-task lakehouse -> Iceberg DAG recheck

Scope:

- Recheck the implemented Airflow DAG after adding the Kafka design-only documents.
- Verify the existing path, not Kafka: JSON lakehouse task -> local Spark/Iceberg publish task.
- Use a fresh local `AIRFLOW_HOME` and isolated `/tmp` output paths.

Commands:

```bash
.venv/bin/python -m pytest -q

PYTHONPATH=src /tmp/manufacturing-mini-airflow-venv/bin/python -m pytest \
  tests/test_airflow_dags.py \
  tests/test_publish_gold_to_iceberg.py \
  tests/test_orchestration.py -q

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-kafka-design-check-20260713/airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
PATH="/tmp/manufacturing-mini-airflow-venv/bin:$PATH" \
/tmp/manufacturing-mini-airflow-venv/bin/airflow db migrate

AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-kafka-design-check-20260713/airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
PATH="/tmp/manufacturing-mini-airflow-venv/bin:$PATH" \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags test \
  manufacturing_lakehouse_to_iceberg_daily 2026-06-29 \
  -c '{"business_date":"2026-06-29","raw_path":"data/raw/manufacturing_events.csv","lakehouse_output_dir":"/tmp/manufacturing-mini-airflow-kafka-design-check-20260713/lakehouse","warehouse":"/tmp/manufacturing-mini-airflow-kafka-design-check-20260713/warehouse","iceberg_output_dir":"/tmp/manufacturing-mini-airflow-kafka-design-check-20260713/evidence"}'
```

Results:

```text
base pytest: 45 passed, 7 skipped
Airflow/Spark optional test set: 15 passed
Airflow metadata migration: passed with fresh SQLite metadata DB
DAG: manufacturing_lakehouse_to_iceberg_daily
task order: run_lakehouse_task -> publish_gold_to_iceberg_task
lakehouse task: quality_passed=true
publish task: status=published
Iceberg table: local.db.gold_daily_metrics
Iceberg runtime: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
published_row_count: 2
target_partition_row_count: 2
snapshot_count: 1
snapshot operation: overwrite
DagRun state: success
```

Verified:

- [x] Airflow parses all three project DAGs in the optional runtime environment.
- [x] The first task creates a quality-passed JSON-backed lakehouse run.
- [x] The second task reads that successful run and publishes its gold CSV through local Spark/Iceberg.
- [x] The Iceberg publish evidence records `pipeline_run_id`, `source_hash`, `schema_hash`, and `gold_snapshot_id`.
- [x] The Kafka documents remain design-only; this run does not add Kafka implementation evidence.

Claim boundary:

- Allowed: local Airflow `dags test` for the two-task JSON lakehouse -> local Spark/Iceberg publish path.
- Not allowed: production Airflow deployment, scheduler/HA proof for this two-task DAG, cluster Spark, full Spark medallion rewrite, Kafka runtime, or end-to-end streaming.

## 2026-07-14 — Kafka design audit and pre-implementation gate

Scope:

- Review the Kafka scenario, question bank, and K1 slice after external-reference supplement.
- Recheck version-sensitive claims against Apache Kafka and librdkafka documentation.
- Keep Kafka code, broker installation, and runtime claims out of this step.

Commands:

```bash
.venv/bin/python -m pytest -q

PYTHONPATH=src /tmp/manufacturing-mini-airflow-venv/bin/python -m pytest \
  tests/test_airflow_dags.py \
  tests/test_publish_gold_to_iceberg.py \
  tests/test_orchestration.py -q

java -version
command -v kafka-server-start.sh
command -v kafka-storage.sh
python -c "from importlib.metadata import version; print(version('confluent-kafka')); print(version('kafka-python'))"
```

Results:

```text
base project environment: 45 passed, 7 skipped
Airflow/Spark optional test set: 15 passed
Java: OpenJDK 17 available
Kafka binary: not installed
usable Docker runtime: unavailable in this WSL session
globally visible but not repo-pinned clients: confluent-kafka 2.3.0, kafka-python 2.0.2
Kafka produce/consume round-trip: not run
```

Design review decisions:

- [x] Keep K1 bounded to one local broker, one topic, one partition, and raw landing.
- [x] Keep Spark Structured Streaming, direct Iceberg sink, and continuous Airflow ownership out of K1.
- [x] Demote K1.5 from current Core to a candidate after K1 proves the landing contract.
- [x] Make the `durable landing -> offset commit` crash window an explicit failure-injection test.
- [x] Distinguish Java producer idempotence defaults from librdkafka defaults.
- [x] Record the official KRaft standalone storage-format sequence.

Claim boundary:

- Allowed: Kafka raw-ingestion scenario/question/slice design reviewed against official references.
- Not allowed: Kafka broker/client runtime verified, Kafka ingestion implemented, continuous streaming pipeline, multi-broker/HA, or end-to-end exactly-once.

## 2026-07-14 — Kafka Test 0 local KRaft round-trip

Scope:

- Close the Kafka broker/client environment gate before implementing K1 raw landing.
- Pin the runtime and client rather than relying on globally installed packages.
- Verify only one local broker/topic/partition/event and a manual consumer offset commit.

Command:

```bash
./scripts/verify_kafka_test0.sh
```

Results:

```text
Java: OpenJDK 17
Kafka archive: kafka_2.13-4.3.1.tgz
archive SHA-512: verified
broker mode: single-node KRaft, standalone storage format
broker address: 127.0.0.1:19092
client: confluent-kafka 2.15.0 (librdkafka 2.15.0)
topic: manufacturing.machine-events.v1.test0
partition count: 1
produced coordinate: partition=0, offset=0
consumed coordinate: partition=0, offset=0
consumer group committed next offset: 1
producer config: enable.idempotence=true, acks=all
broker shutdown: clean; no Kafka process left running
```

Verified:

- [x] Kafka 4.3.1 can be downloaded, checksum-verified, formatted, and started without Docker.
- [x] The pinned CPython 3.10 `confluent-kafka` wheel installs in an isolated venv.
- [x] One keyed JSON event is acknowledged by the broker and read back unchanged.
- [x] Produced and consumed Kafka coordinates match.
- [x] The consumer commits the next offset manually after reading the event.
- [x] The runbook writes machine-readable evidence and stops the broker.

Claim boundary:

- Allowed: pinned local Kafka 4.3.1 KRaft broker/client Test 0; one-topic/one-partition produce-consume-manual-commit proof.
- Not allowed: K1 raw landing implemented, restart/replay or crash-window dedup verified, continuous streaming pipeline, multi-broker/HA, secure production Kafka, or end-to-end exactly-once.

## 2026-07-14 — Kafka K1 bounded raw ingestion

Scope:

- Implement the reviewed K1 event identity, message key, landing, offset-commit, recovery, replay, and quarantine contracts.
- Keep Kafka optional: pure contract/landing tests run without a broker or Kafka client dependency.
- Verify the runtime path with one local KRaft broker, one topic, and one partition.
- Keep Spark Structured Streaming, direct Iceberg sink, Airflow continuous ownership, and production Kafka outside K1.

Commands:

```bash
.venv/bin/python -m pytest -q
./scripts/verify_kafka_test0.sh
./scripts/verify_kafka_k1.sh
```

Initial runtime finding:

```text
first K1 run failed before broker use:
  kafka event contract imported pipeline.lakehouse.ACCEPTED_OPERATIONS
  pipeline.lakehouse imported pymongo
  isolated Kafka venv intentionally did not contain pymongo

fix:
  moved ACCEPTED_OPERATIONS to dependency-free manufacturing_data_platform.domain
  CSV lakehouse and Kafka contract now share the domain constant without runtime coupling
```

Results:

```text
base pytest: 56 passed, 7 skipped
Kafka Test 0 after shared-runner refactor: passed
Kafka K1 broker verification: passed
Kafka: 4.3.1 single-node KRaft
client: confluent-kafka 2.15.0
topic: manufacturing.machine-events.v1
partition count: 1

initial valid records: offsets 0..2
initial landing accepted_count: 3
initial committed next offset: 3

failure injection record: offset 3
failure point: after immutable landing rename, before offset commit
same-group retry coordinate: offset 3
retry landing status: reused
retry accepted_count: 0
retry reused_coordinate_count: 1
retry committed next offset: 4

bounded replay: offsets 0..3
replay reused_coordinate_count: 4
replay mutated normal group offset: false

invalid record: offset 4
quarantine_count: 1
committed next offset after quarantine: 5

reconciliation:
  produced_record_count: 5
  persisted_coordinate_count: 5
  accepted_event_count: 4
  quarantined_event_count: 1
  immutable_batch_count: 3
```

Verified:

- [x] Strict JSON event v1 validates required fields, types, time/date, operation, and metric ranges.
- [x] `event_id`, Kafka coordinate, consumer-group progress, and `machine_id` message key are separate contracts.
- [x] Accepted JSONL stores the normalized payload plus topic/partition/offset/key/timestamp evidence.
- [x] JSONL and manifest files are fsynced in staging and atomically renamed as an immutable batch.
- [x] Offset commit occurs only after `land_records` returns from durable landing.
- [x] Crash after landing/before commit causes redelivery without accepted-set duplication.
- [x] Same coordinate with changed key/payload is rejected as a consistency violation.
- [x] Same `event_id` at a new coordinate is recorded as duplicate evidence by pure tests.
- [x] Bounded replay reuses persisted coordinates and does not commit replay progress.
- [x] Invalid event is quarantined and does not block the partition.
- [x] The shared local-Kafka runner still reproduces Test 0 and stops the broker.

Evidence:

```text
source contract: learn/system-design/source-contracts/02-kafka-machine-event-v1.md
decision notes:
  learn/reference-decisions/kafka-event-identity-and-key.md
  learn/reference-decisions/kafka-offset-and-landing-commit.md
code: src/manufacturing_data_platform/kafka_ingestion/
unit tests: tests/test_kafka_ingestion.py
runtime runbook: scripts/verify_kafka_k1.sh
runtime evidence: /tmp/manufacturing-mini-kafka-k1-evidence/kafka_k1_verification.json
```

Claim boundary:

- Allowed: bounded local one-broker/one-partition Kafka raw ingestion; strict synthetic event contract; payload+coordinate immutable JSONL evidence; manual landing-before-commit offset handling; injected crash recovery; bounded replay; invalid-event quarantine.
- Not allowed: continuous streaming service, production Kafka operation, multi-partition ordering/rebalance, multi-broker HA, end-to-end exactly-once, Schema Registry, TLS/SASL/ACL, Spark Structured Streaming, direct Iceberg streaming sink, or Airflow-owned continuous consumer.

## 2026-07-16 — Kafka K1 offset-gap contract review

Scope:

- Review the external-audit finding that `max(offset) + 1` could silently skip a gapped batch.
- Reconcile the landing contract with Apache Kafka 4.3.1 and confluent-kafka 2.15.0 semantics.
- Keep the change inside the existing one-topic/one-partition bounded K1 scope.

Commands:

```bash
.venv/bin/python -m pytest -q tests/test_kafka_ingestion.py
.venv/bin/python -m pytest -q
python -m pytest -q
./scripts/verify_kafka_k1.sh
git diff --check
```

Results:

```text
Kafka unit tests: 13 passed
base environment: 58 passed, 7 skipped
Spark-visible environment: 61 passed, 4 skipped
Kafka 4.3.1 local broker verification: passed
git diff --check: passed
```

Verified:

- [x] Kafka offsets are not required to be consecutive; a batch with offsets `0, 2` commits next offset `3`.
- [x] K1 landing rejects input that does not preserve strictly increasing consumer poll order.
- [x] The runtime passes every collected single-partition poll record to landing before synchronous commit.
- [x] Existing landing-before-commit recovery, coordinate reuse, bounded replay, and quarantine behavior still pass against a real local broker.
- [x] Filesystem wording is limited to the local Linux `fsync` + same-filesystem atomic rename path.
- [x] Injected failure remains an in-process logical recovery test, not SIGKILL or power-loss crash-consistency proof.

Decision:

```text
reject contiguous-offset enforcement
accept strictly increasing poll-order contract
commit last durably handled record offset + 1
keep arbitrary filtered subsets outside the land_records caller contract
```

Notes:

- `CLAUDE_IMPLEMENTATION_PACKAGE.md` and `EXTERNAL-AUDIT-PACKAGE.md` are temporary coordination artifacts, not public implementation evidence or commit candidates.
- K1.5 landed-JSONL-to-batch integration remains a separate next Slice.

## 2026-07-16 — Kafka K1.5 landing-to-batch bridge

Scope:

- Adapt one explicit `business_date` from immutable K1 accepted JSONL into a deterministic, content-addressed CSV.
- Preserve `event_id`, Kafka coordinate, key, timestamp, and source-record fingerprint in source/bronze identity evidence.
- Reuse the existing JSON-backed quality/gold pipeline and local Spark/Iceberg publisher.
- Keep Spark Structured Streaming, direct Kafka-to-Iceberg writes, Airflow changes, multi-partition input, and production claims outside K1.5.

Commands:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_kafka_batch_adapter.py -q
PYTHONPATH=src .venv/bin/python -m pytest -q
PYTHONPATH=src python -m pytest -q
./scripts/verify_kafka_k1.sh
./scripts/verify_kafka_k1_5.sh
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.publish_gold_to_iceberg \
  --lakehouse-output-dir /tmp/manufacturing-mini-kafka-k1-5-evidence/lakehouse \
  --business-date 2026-06-29 \
  --warehouse /tmp/manufacturing-mini-kafka-k1-5-evidence/warehouse \
  --output-dir /tmp/manufacturing-mini-kafka-k1-5-evidence/iceberg --clean
# repeat the publisher without --clean
git diff --check
```

Results:

```text
focused adapter tests: 22 passed
base environment: 80 passed, 7 skipped
Spark-visible environment: 83 passed, 4 skipped
Kafka 4.3.1 K1 broker verification: passed
K1.5 runtime checks: 11 passed

selected accepted events: 4
adapter first/second status: created -> reused
lakehouse first/second status: processed -> skipped
adapter source_hash == lakehouse source_hash: true
lakehouse run directories for the date: 1
gold rows: 1
gold totals: units_produced=100, defect_count=6

Iceberg first publish: published
Iceberg retry: skipped
snapshot count: 1 -> 1
snapshot id unchanged: 2896841135077514634
target partition rows: 1
```

Verified:

- [x] Only accepted envelopes whose coordinate/status/event identity/key/timestamp agree with the sibling manifest enter the adapter.
- [x] Manifest `accepted_count`, accepted entries, and accepted JSONL row count must agree.
- [x] Empty-date, malformed/tampered, and multi-partition input fails before lakehouse current state can advance.
- [x] Fixed columns, `\n`, and `(topic, partition, offset)` ordering make canonical CSV bytes deterministic.
- [x] Batch grouping changes neither canonical CSV bytes nor source hash.
- [x] Existing content-addressed versions reuse only when both CSV and persisted provenance match.
- [x] Different physical manifest grouping with the same CSV identity raises a provenance consistency error instead of returning stale provenance.
- [x] Same accepted set reuses the adapter version and existing lakehouse run without doubling gold.
- [x] The K1 landing from a real local broker flows through adapter, quality-passed gold, and the existing local Iceberg publish.
- [x] Re-publishing the same lakehouse run creates no new Iceberg snapshot.

Review disposition:

```text
ADR status: Implemented
wall-clock omitted from provenance: accepted for reproducibility
manifest/JSONL count cross-check: accepted as input-contract validation, not scope expansion
multi-partition input: explicitly rejected until that scope is designed and verified
```

Known pre-existing follow-up:

- A cold Spark/Iceberg publish writes Ivy resolution lines to process stdout before the final JSON, so redirecting stdout to a JSON parser fails. The persisted publish evidence JSON is valid. This CLI stream-cleanliness issue is outside K1.5 and remains a separate fix.

Claim boundary:

- Allowed: bounded local Kafka landing-to-batch bridge for one date; deterministic source identity with Kafka provenance; existing quality/gold rerun contract reuse; downstream local Iceberg publish and publish retry evidence.
- Not allowed: continuous streaming pipeline, Spark Structured Streaming, direct Kafka-to-Iceberg sink, multi-partition/rebalance, end-to-end exactly-once, column-level lineage, cryptographic payload-integrity chain, concurrent writer correctness, or production Kafka/Spark/Airflow operation.

## 2026-07-16 — Kafka K1/K1.5 portfolio promotion evidence

Scope:

- Re-run the representative Kafka K1 and K1.5 scenario from the public reproduction commands.
- Re-publish the resulting quality-passed gold to a clean local Iceberg table and verify publish retry behavior.
- Package a public-safe evidence summary, one architecture/runtime overview, one failure/recovery screen, and one batch/Iceberg rerun screen.
- Link the package from the external-reader README without changing implementation claims.

Commands:

```bash
.venv/bin/python -m pytest -q
PYTHONPATH=src .venv/bin/python -m manufacturing_data_platform.pipeline.run \
  --catalog-backend json --output-dir /tmp/manufacturing-mini-promotion-cli
PYTHONPATH=src .venv/bin/python -m manufacturing_data_platform.pipeline.operator_report \
  --output-dir /tmp/manufacturing-mini-promotion-cli --business-date 2026-06-29
PYTHONPATH=src .venv/bin/python -m manufacturing_data_platform.pipeline.run_eav \
  --catalog-backend json --output-dir /tmp/manufacturing-mini-promotion-eav
./scripts/verify_kafka_k1.sh
./scripts/verify_kafka_k1_5.sh
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.publish_gold_to_iceberg \
  --lakehouse-output-dir /tmp/manufacturing-mini-kafka-k1-5-evidence/lakehouse \
  --business-date 2026-06-29 \
  --warehouse /tmp/manufacturing-mini-kafka-k1-5-evidence/warehouse \
  --output-dir /tmp/manufacturing-mini-kafka-k1-5-evidence/iceberg --clean
# repeat the publisher without --clean
jq empty docs/portfolio/kafka-k1-k1-5/evidence/runtime-evidence.json
npx playwright screenshot ...
```

Results:

```text
pytest: 80 passed, 7 skipped
JSON lakehouse CLI + operator report: passed
EAV JSON CLI: passed
publication secret scan: no findings
publication private-path placeholder scan: no findings

Kafka 4.3.1 K1 broker verification: passed
K1 reconciliation: produced=5, persisted=5, accepted=4, quarantined=1
landing-before-commit failure observed: true
recovery: redelivered=1, landing status=reused, accepted total=4
bounded replay: reused=4, normal group commit=false

K1.5 checks: 11 passed
adapter: created -> reused
lakehouse: processed -> skipped
quality: 8 checks passed
gold: 1 row, units_produced=100, defect_count=6
source_hash: 9efd6173efd21cd6563c9ab88d4d63cde7cb4599287faa6ea1576a68b589ed53

Iceberg: published -> skipped
snapshot count: 1 -> 1
snapshot id unchanged: 3544754184027092485

portfolio screens: 3 PNG files, each 1440x900
desktop visual inspection: passed
mobile report layout visual inspection: passed
```

The K1.5 bridge entry above and this promotion recheck are separate point-in-time runs. A fresh Kafka production run assigns new record timestamps/fingerprints, so the canonical adapter `source_hash` can change; a clean Iceberg warehouse publish also receives a new `snapshot_id`. Determinism and retry-skip assertions apply within the same immutable landing/publish state, while each entry preserves the values observed in that run.

Artifacts:

- `docs/portfolio/kafka-k1-k1-5/README.md`
- `docs/portfolio/kafka-k1-k1-5/README.ko.md`
- `docs/portfolio/kafka-k1-k1-5/evidence/runtime-evidence.json`
- `docs/portfolio/kafka-k1-k1-5/report.html`
- `docs/portfolio/kafka-k1-k1-5/assets/*.png`

Claim boundary:

- Allowed: the same K1/K1.5 local bounded claims recorded above, now packaged as an external-reader walkthrough with actual runtime-derived screens.
- Not allowed: dashboard/production observability operation, continuous streaming, multi-partition or multi-broker correctness, production Kafka/Spark/Iceberg operation, or any claim not already supported by K1/K1.5 verification.

## 2026-07-19 — S7 Spark machine-event batch (Codex reviewed / accepted)

Scope:

- Re-express the existing Python silver/gold on one landed `business_date` with Spark DataFrame built-ins, reusing the K1.5 canonical CSV + `source_hash` as the input contract.
- Gate the Iceberg publish on the existing quality suite applied to the Spark result; publish quality-passed corrections with `overwritePartitions()`.
- Keep Spark Structured Streaming, direct Kafka-to-Iceberg sink, full medallion rewrite, cluster/distributed Spark, and performance claims out of scope.

Commands:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_spark_machine_event_batch.py
PYTHONPATH=src .venv/bin/python -m pytest -q
PYTHONPATH=src python -m pytest -q
python -m pytest -q tests/test_spark_machine_event_batch.py   # Spark-visible (pyspark 3.5.8)
PYTHON_BIN=python bash scripts/verify_spark_machine_event_batch.sh
python -m py_compile dags/manufacturing_spark_machine_event_batch.py
git diff --check
```

Results:

```text
S7 pure tests (.venv): passed; Spark integration tests skipped without pyspark
base environment (.venv): 88 passed, 12 skipped
Spark-visible environment (system python): 95 passed, 5 skipped
S7 Spark integration (system python): 10 passed (engine parity, dedup, quality gate, publish/rerun/correction/other-date)
S7 runtime verification (scripts/verify_spark_machine_event_batch.sh): 8/8 checks passed
DAG py_compile: valid
git diff --check: passed

runtime state transitions (real local Iceberg table local.db.gold_daily_metrics):
  other-date D2 publish -> published (baseline partition)
  source A publish -> published, snapshot_count 1 -> 2
  same source A retry -> skipped, snapshot_id unchanged (5635830946204702022), snapshot_count 2
  correction source B -> published, snapshot_count 2 -> 3, target D1 partition replaced (units_produced=200)
  other-date D2 rows preserved through the correction
  gold groupBy executed plan -> Exchange observed
  Spark batch source_hash == adapter source_hash
```

Verified:

- [x] Spark silver/gold match Python `transform_silver`/`transform_gold` on the same canonical rows (grain, totals, `defect_rate`, `avg_cycle_time_ms`), using a `format_number`-based round that matches Python `round` at boundary doubles like `802.675` (see revision below) and Kafka-coordinate-ordered natural-key dedup.
- [x] The existing quality suite runs on the Spark-materialized result; a numeric-range/conservation violation blocks the Iceberg write and writes no success-state pointer.
- [x] Same `table + business_date + source_hash` success is skipped with no new snapshot; a changed source creates exactly one new snapshot and replaces only the target `business_date` partition.
- [x] `source_hash` (input), `run_id` (execution), and `snapshot_id` (table commit) are recorded as separate fields.
- [x] The gold aggregation produces a shuffle `Exchange`, recorded as local execution-plan learning evidence, not a performance claim.
- [x] The single-task Airflow DAG only assembles one validated CLI command; no transform/quality/Iceberg logic in the DAG body. `max_active_runs=1`.

Airflow evidence (from Codex independent review after the revision, isolated Airflow 3.3.0 runtime):

```text
isolated DagBag suite: 5 passed
airflow dags test manufacturing_spark_machine_event_batch: DagRun success, task exit 0,
  local Iceberg publish status=published, snapshot_count=1
This is a DagBag/dags-test wiring proof only, NOT scheduler/executor/production Airflow evidence.
In Claude's own environment airflow is not installed, so the Airflow tests skip here.
```

Claim boundary:

- Allowed: local bounded Spark batch reusing a provenance-checked Kafka landing adapter, preserving the existing gold grain and reconciliation contract, publishing only quality-passed corrections to one Iceberg gold table; verified same-source no-op, changed-source partition replacement, other-date preservation, shuffle-plan evidence, and a thin local Airflow DagBag/dags-test wrapper contract.
- Not allowed: production or cluster Spark, large-scale performance/throughput improvement, full Spark/Iceberg medallion pipeline, continuous Kafka/Spark streaming, end-to-end exactly-once, concurrent writer correctness, distributed Spark-native quality evaluation (quality is collected to the driver), or production Airflow operation.

Revision (2026-07-19) — Codex review H1/H2/M1/M2 addressed:

```text
H1 rounding parity: bround/round/decimal-cast diverge from Python round at valid boundary doubles
  (probe: bround 204, round 779, decimal 779 mismatches / 40,400 integer-ratio cases at scale 2;
   32107/40 -> Python 802.67 vs bround 802.68). Switched gold rounding to format_number + comma
   strip + double cast (0 mismatches / 40,400). Added boundary golden test
   test_gold_rounding_matches_python_at_half_boundary (avg 802.675 -> 802.67, Spark == Python). No UDF.
H2 stale-state skip: skip now requires the recorded snapshot to still exist in the current table's
  snapshot history, so an emptied/recreated warehouse with persisted state rewrites instead of a
  false skip. Added test_stale_success_state_on_recreated_warehouse_rewrites.
M1 quality-fail exit code: main() now non-zero exits on status=quality_failed so a BashOperator
  task fails. Added pure test_main_exits_nonzero_on_quality_failure.
M2 bridge provenance persistence: run_bridge_spark_batch threads adapter identity via extra_evidence
  so the persisted spark_machine_event_batch.json contains the same adapter/source_hash as the return
  value. Added pure test_bridge_persists_adapter_identity + persisted==returned assertion in the
  publish integration test.
Doc corrections: rounding wording fixed across ADR/slice/scenario/roadmap/traceability; claim
  boundary now states quality is driver-collected (not distributed Spark-native); ADR Status set to
  Proposed pending Codex re-verification; this entry re-dated 2026-07-19.

Re-run results (2026-07-19):
  base (.venv): 90 passed, 14 skipped
  Spark-visible (system python): 99 passed, 5 skipped
  S7 Spark integration (system): 14 passed (incl. H1 boundary parity, H2 recreated-warehouse recovery)
  S7 runtime verification: 8/8 checks passed
  git diff --check: passed
```

## 2026-07-21 — S8 edge/cloud recovery (returned-unreviewed / Codex review required)

Scope:

- Simulate one bounded disconnected edge session with an immutable sealed local spool, replay it through the existing local Kafka/K1 landing after reconnect, and allow the existing K1.5 batch/gold path only after the sealed sequence range is fully represented in the central accepted set.
- Reuse K1/K1.5 through public APIs only. No changes to `contracts.py`, `landing.py`, `runtime.py`, `batch_adapter.py`, their tests, or the shared Kafka runbook.
- Keep OPC UA/MQTT/ROS 2/DDS, Structured Streaming/Flink, new dependencies, multi machine/session/partition, and production claims out of scope.

Commands:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_edge_recovery.py
PYTHONPATH=src .venv/bin/python -m pytest -q
./scripts/verify_edge_recovery.sh
./scripts/verify_kafka_k1.sh
./scripts/verify_kafka_k1_5.sh
git diff --check
```

Results:

```text
S8 targeted tests: 11 passed
default suite (.venv): 101 passed, 14 skipped
S8 runtime verification (scripts/verify_edge_recovery.sh): passed (broker phase 7/7, promote phase 7/7)
K1 regression (scripts/verify_kafka_k1.sh): passed
K1.5 regression (scripts/verify_kafka_k1_5.sh): passed
git diff --check: passed

runtime state transitions (real local Kafka broker, Apache Kafka 4.3.1 KRaft):
  phase spool   -> broker process absent; edge events 1..3 appended and sealed (expected_last_sequence=3)
                   central accepted total = 0; missing = [1, 2, 3]
  phase partial -> replayed edge sequences [1, 2] at Kafka offsets [0, 1]
                   accepted this batch = 2; central accepted total = 2; missing = [3]; recovery_complete = false
  phase complete-> replayed [1, 2, 3] at NEW Kafka offsets [2, 3, 4]
                   accepted this batch = 1; duplicate event_ids = 2; central accepted total = 3; missing = []
  phase repeat  -> replayed [1, 2, 3] at NEW Kafka offsets [5, 6, 7]
                   accepted this batch = 0; duplicate event_ids = 3; central accepted total = 3
  accepted_total transition: 0 -> 2 -> 3 -> 3

K1.5 promotion gate (project .venv):
  first  run -> bridge lakehouse status = processed, quality_passed = true
  second run -> bridge lakehouse status = skipped
  adapter source_hash unchanged: 75d98a387601f6b532b756f640a7c2281813e9cd0b33d7a622c01b70ef22381a
  lakehouse run_id unchanged:    2026-06-29-20260721T042445Z-8ddc9ebf
  trusted gold identical across reruns

identity separation observed in evidence:
  edge sequence  [1, 2, 3]
  Kafka offsets  [0, 1, 4]      <- different space; event 3 landed at offset 4
  event_id       evt-20260629-000001 / -000002 / -000003
  source_hash and run_id recorded as separate fields
```

Verified:

- [x] An edge entry is only considered buffered after canonical bytes are fsynced and atomically renamed on the same local filesystem; the immutable entry set is the progress record (no separate mutable cursor).
- [x] Same coordinate + same canonical bytes is an idempotent reuse; same coordinate + different bytes, a duplicate `event_id` at another sequence, an unsafe identifier, a seal with a missing sequence, an append after sealing, and a changed seal are all rejected.
- [x] Completeness is decided by sealed `event_id` membership in the central accepted set, never by Kafka offset continuity; a contiguous-looking landing that misses one edge event is still reported incomplete with the exact missing sequence.
- [x] Incomplete recovery raises before `run_bridge` is called and creates no adapter output, no lakehouse run, and no trusted-state pointer.
- [x] Complete recovery permits the existing K1.5 path and produces a quality-passed gold result.
- [x] Repeated producer replay at new Kafka coordinates adds duplicate transport evidence only: the accepted business-event set, the canonical `source_hash`, the `run_id`, and the trusted gold rows are unchanged and the bridge rerun is `skipped`.
- [x] Edge sequence, `event_id`, Kafka coordinate, `source_hash`, and `run_id` remain distinguishable in the persisted evidence.
- [x] The spool/coverage path imports neither pyspark nor pymongo at module level, so the shared Kafka runbook venv can run the broker phase; the K1.5 import is lazy.

Failures encountered and resolved:

```text
1 failure during implementation: seal_edge_session validated the sequence range before checking an
existing seal, so re-sealing with a different expected_last_sequence reported "spool also holds
sequences [3]" instead of the correct "already sealed" conflict. Fixed by deciding reuse-vs-conflict
on the existing seal first. Re-ran targeted tests: 11 passed.
```

Runtime/operation gate:

```text
runtime artifacts only under /tmp (spool, landing, adapter, lakehouse, evidence JSON)
local broker started and stopped through the existing shared runbook; no broker process left running
no persistent service deployment or migration
no credential or private-path content in tracked files
```

Review disposition:

```text
returned-unreviewed / Codex review required
scenario 05 moved from Proposed to implemented / local bounded recovery verified
ADR status intentionally left Proposed pending Codex independent verification
```

Claim boundary:

- Allowed: a bounded local edge-recovery **simulation** with an immutable sealed spool, replay of synthetic machine events through a real local Kafka broker into the existing K1 landing, downstream batch/gold blocked while recovery was incomplete, and complete plus repeated replay verified without accepted-set or trusted-result duplication. Required qualifiers: synthetic, local, bounded, simulation, single machine/session/partition.
- Not allowed: industrial IoT / autonomous factory platform, real edge gateway or product-grade offline buffer, OPC UA / MQTT / ROS 2 / DDS integration, continuous or large-scale real-time streaming, power-loss-safe or distributed durability, multi-partition ordering/rebalance correctness, production Kafka/Spark/Airflow operation, end-to-end exactly-once, digital twin, anomaly detection, predictive maintenance, or machine control.

### Revision (2026-07-21) — Codex review H1/H2/M1/M2 addressed

```text
H1 bounded session scope enforced:
  seal now derives and persists machine_id and business_date; a fresh seal with more than one
  machine_id or business_date is rejected (EdgeSessionScopeError). promote_recovered_session
  refuses a requested business_date that differs from the sealed session date BEFORE any
  adapter/lakehouse/evidence output. Codex's counterexample (mixed dates sealing successfully and
  promoting 1 of 2 sealed events) no longer reproduces.
  New tests: test_seal_rejects_mixed_machine_id, test_seal_rejects_mixed_business_date,
             test_seal_persists_session_scope,
             test_promotion_rejects_business_date_mismatch_without_side_effects.

H2 partial-recovery gate now exercised at runtime (was unit-only):
  phase_broker calls the real promote_recovered_session while sequence 3 is missing, asserts
  RecoveryIncompleteError, asserts adapter/lakehouse/promotion-evidence paths do not exist, and
  persists partial_promotion_blocked=true in the phase evidence. The lazy run_bridge import lets
  the incomplete branch fail inside the Kafka runbook venv without the batch stack.
  Runtime result: partial_promotion_blocked=true, no_downstream_output=true,
  "RecoveryIncompleteError: recovery incomplete: missing edge sequences [3] of 1..3".

M1 full seal re-validation on load and reuse:
  _validate_seal checks format_version, edge_source_id, boot_session_id, sealed_event_count, the
  exact declared sequence set 1..N, per-entry fingerprint/event_id, and session membership; every
  entry path segment must agree with its envelope. Missing AND extra spool entries are rejected
  instead of silently filtered, and existing-seal reuse runs the same validation.
  New tests: test_entry_added_after_sealing_is_rejected_on_load,
             test_tampered_seal_manifest_is_rejected (9 tamper mutations).

M2 assumption and boundary recorded:
  event_id is documented in the source contract and ADR as a globally unique, immutable v1
  business-event identity; the same event_id with a different payload is a producer contract
  violation, not a correction. No payload-equivalence checking is claimed because K1 performs none.
  Scenario 05's quality-failure variant no longer says "Iceberg write": S8 invokes the K1.5
  JSON-backed batch/gold path and does not invoke or test S7 Iceberg publish, nor a
  quality-failure runtime case.
```

Re-run results (2026-07-21, revision):

```text
focused: PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_edge_recovery.py -> 17 passed
normal suite: PYTHONPATH=src .venv/bin/python -m pytest -q                       -> 107 passed, 14 skipped
./scripts/verify_edge_recovery.sh  -> passed (broker phase 9/9 incl. partial promotion gate, promote phase 7/7)
./scripts/verify_kafka_k1.sh       -> passed (regression)
./scripts/verify_kafka_k1_5.sh     -> passed (regression)
git diff --check                   -> clean

runtime state transitions (unchanged shape, re-verified):
  accepted_total 0 -> 2 -> 3 -> 3
  partial phase: promotion BLOCKED at runtime, no downstream output created
  K1.5 processed -> skipped; source_hash and run_id unchanged across reruns
```

Status after revision: `revision-pending / Codex re-review required`. ADR, scenario 05, slice 08,
and source contract 03 are intentionally NOT promoted to `Implemented`.

Note: `.venv/bin/pytest` carries a stale shebang pointing at an old `robot-data-platform-mini`
path, so all runs above used `.venv/bin/python -m pytest`.

## 2026-07-21 — S8 Codex independent re-review (accepted-closed)

Review disposition:

```text
accepted-closed
ADR status promoted: Implemented
scenario 05 / slice 08 / source contract 03 promoted: Implemented
```

Codex independently inspected the revised code, tests, runtime script, design contract, and
claim boundary. The H1 mixed-machine/date counterexample is now blocked at seal time, promotion
date mismatch fails before downstream output, sealed manifests are fully revalidated on load and
reuse, and the partial recovery gate is exercised by the real runtime path.

Independent commands:

```text
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_edge_recovery.py
  17 passed in 1.01s

PYTHONPATH=src .venv/bin/python -m pytest -q
  107 passed, 14 skipped in 2.62s

./scripts/verify_edge_recovery.sh
  passed
  broker phase: 9/9 checks
  promote phase: 7/7 checks
  partial_promotion_blocked=true
  no_downstream_output=true
  accepted_total: 0 -> 2 -> 3 -> 3
  K1.5: processed -> skipped
  source_hash and run_id unchanged within the repeated promotion

./scripts/verify_kafka_k1.sh
  passed

./scripts/verify_kafka_k1_5.sh
  passed (11 checks)

git diff --check
  clean
```

Accepted design judgments:

- session scope is derived from immutable spool content and persisted in the seal;
- an entry added after sealing invalidates the sealed session and fails loudly;
- wall-clock values remain outside canonical identity;
- `event_id` membership is valid only under the documented globally unique, immutable v1
  business-identity assumption.

Residual boundaries remain unchanged: no power-loss/SIGKILL guarantee, NFS/object-store guarantee,
concurrent-writer guarantee, or atomic transaction spanning landing commit and spool seal.
