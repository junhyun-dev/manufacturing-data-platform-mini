# Verification Log

## 2026-06-30 — Phase 2 Slice 1 Initial Implementation

Scope:

- Synthetic manufacturing CSV -> bronze -> silver -> gold -> quality -> catalog/lineage.
- Airflow wrapper added as a single CLI-triggering task.

Commands:

```bash
pytest
PYTHONPATH=src python -m robot_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/robot-mini-lakehouse-json-final
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
PYTHONPATH=src python -m robot_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/robot-mini-review-cli
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
PYTHONPATH=src python -m robot_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/robot-mini-claude-self-audit
PYTHONPATH=src python -m robot_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/robot-mini-claude-self-audit  # second run
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

