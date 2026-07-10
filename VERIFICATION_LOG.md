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
rg -n -i "(personal path|private email|private company name|customer name|internal path)" --glob '!**/.venv/**' --glob '!**/__pycache__/**' --glob '!**/.pytest_cache/**' --glob '!PUBLICATION_CHECKLIST.md' .
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
- [x] System-design docs use stable names: `00-service-purpose-charter`, `00a-plain-project-map`, `01-scenario-seed`, `02-slice2-question-map`, `03-source-contract`, `04-slice2-spark-iceberg-shift`, `05-iceberg-spark-mini-primer`.
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
