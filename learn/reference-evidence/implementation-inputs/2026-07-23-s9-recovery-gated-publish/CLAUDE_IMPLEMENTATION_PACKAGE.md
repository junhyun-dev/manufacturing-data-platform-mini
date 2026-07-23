# S9 Recovery-Gated Spark/Iceberg Publish - Claude Implementation Package

> Package status: accepted-closed / Codex independently verified
>
> R1-R5 and the §14 evidence-state fix were independently re-verified and accepted.
> See §14, §15, and §16 Codex Acceptance.

Lifecycle: `ready-for-delegation -> delegated-awaiting-return -> returned-unreviewed ->
revision-requested (optional) -> accepted-closed`.

## 1. Target And Preflight

```text
project: repository root (`manufacturing-data-platform-mini`)
target commit: 3b4b7fe
expected working tree: clean except this untracked package
mode: Delegated Implementation
```

Before editing:

1. Run `git status --short` and `git show -s --oneline HEAD`.
2. Stop and report if the target commit or dirty boundary differs.
3. Read the current S7/S8 code, tests, decisions, and latest `VERIFICATION_LOG.md`; do not reuse
   older design-only conclusions.
4. Keep the current public claim boundary: synthetic, local, bounded, one sealed edge session,
   one machine/date/topic/partition, one local Iceberg gold table.

Read first:

```text
learn/system-design/00-service-purpose-charter.md
learn/system-design/01-system-traceability-map.ko.md
learn/system-design/scenarios/05-industrial-telemetry-recovery.md
learn/system-design/slices/07-spark-machine-event-batch.ko.md
learn/system-design/slices/08-edge-cloud-recovery.ko.md
learn/reference-decisions/edge-buffer-and-recovery-progress.md
learn/reference-decisions/spark-engine-swap-contract.md
learn/system-design/source-contracts/03-edge-recovery-envelope.md
src/manufacturing_data_platform/edge_recovery.py
src/manufacturing_data_platform/pipeline/spark_machine_event_batch.py
src/manufacturing_data_platform/kafka_ingestion/batch_adapter.py
src/manufacturing_data_platform/orchestration.py
dags/manufacturing_spark_machine_event_batch.py
tests/test_edge_recovery.py
tests/test_spark_machine_event_batch.py
tests/test_orchestration.py
tests/test_airflow_dags.py
scripts/verify_edge_recovery.sh
scripts/verify_spark_machine_event_batch.sh
VERIFICATION_LOG.md
```

## 2. Goal Brief

### Goal

Connect the already verified S8 recovery contract to the already verified S7 Spark/Iceberg
publish contract without reimplementing either one:

```text
sealed edge session
-> real Kafka/K1 accepted landing
-> recovery-complete + exact-session-input gate
-> existing deterministic adapter/source_hash
-> existing Spark silver/gold + quality gate
-> existing Iceberg business_date overwrite/snapshot evidence
```

### User / scenario

A plant data operator has recovered a bounded disconnected edge session. The operator needs to
advance the trusted Iceberg current table only when:

1. every event in the sealed session is centrally accepted;
2. the canonical Spark input represents exactly that recovered session for the requested date;
3. the existing Spark quality gate passes.

Primary scenario:

- `learn/system-design/scenarios/05-industrial-telemetry-recovery.md`

### Input / output

Input:

```text
immutable S8 spool + seal
K1 landing directory
edge_source_id / boot_session_id / business_date
adapter output directory
local Iceberg warehouse / table
evidence output directory
```

Output:

```text
one S9 evidence document binding:
edge identity/range/event_ids
-> Kafka recovered coordinates
-> adapter source_hash/canonical CSV
-> Spark run_id/quality
-> Iceberg snapshot_id/status
```

### Non-goals

```text
new Kafka consumer or broker lifecycle owned by Airflow
continuous streaming / Spark Structured Streaming / Flink
direct Kafka-to-Iceberg sink
full bronze/silver/gold Iceberg rewrite
multi-machine/session/partition or rebalance correctness
concurrent Iceberg writers / distributed transaction / exactly-once
production Airflow, HA, cluster Spark, Kubernetes
real edge gateway or OPC UA / MQTT / ROS 2 / DDS
dashboard, anomaly model, digital twin, machine control
```

## 3. Core Questions And Accepted Decisions

| Core question | Accepted S9 answer |
|---|---|
| What new behavior is needed? | Only compose S8 readiness with S7 publish. Do not copy transform, quality, adapter, Kafka, or Iceberg logic. |
| When may Spark start? | Only after the shared S8 gate verifies seal integrity, requested date, and complete central coverage. |
| Is membership alone enough for the publish input? | No. After creating/reusing the canonical adapter, its event-id set and count must equal the sealed session event-id set. Extra same-date accepted events must block Spark/Iceberg publication. |
| What if other dates exist in the landing? | Allowed. The deterministic adapter filters the requested date; only the selected date's event set must equal the sealed session. |
| What is the input identity? | The existing adapter CSV SHA-256 `source_hash`; do not invent a new hash. |
| What is the trusted publish identity? | Existing S7 `run_id` and Iceberg `snapshot_id`, kept distinct from edge sequence, `event_id`, and Kafka coordinate. |
| What happens on retry? | Same sealed session and immutable landing produce the same adapter `source_hash`; S7 returns `skipped` while the recorded snapshot remains in table history. |
| What does Airflow own? | A thin single-task CLI wrapper. It does not start Kafka, compute transforms, or duplicate the gate. `max_active_runs=1`. |
| Why one Airflow task instead of gate-task + publish-task? | The recovery/session input binding and publish invocation are one application-level command. Splitting them would require another immutable handoff contract and add no current recovery value. The repo already demonstrates a genuine two-task publish boundary elsewhere. |
| What atomicity remains open? | Spool, landing, adapter evidence, S7 state, and Iceberg commit are not one transaction. Existing idempotent retry provides convergence, not distributed atomicity. |

### Required shared gate refactor

Extract or expose one reusable S8 readiness function in `edge_recovery.py`. Both the existing
`promote_recovered_session` and S9 must call the same implementation for:

```text
seal revalidation
requested business_date match
coverage computation
RecoveryIncompleteError
```

Do not duplicate these checks in the new S9 module. Keep S7 imports lazy until the recovery gate
passes so an incomplete recovery fails before Spark/Iceberg dependencies are needed.

### Exact-session input contract

After recovery passes, use the existing K1.5 adapter. Before Spark starts:

```text
selected canonical row count == sealed event count
set(canonical CSV event_id) == set(sealed session event_id)
adapter source_hash == SHA-256 identity already produced by K1.5
all canonical rows belong to the sealed business_date (existing S7 check remains authoritative)
```

If the exact-set check fails:

```text
raise a domain-specific error
do not start Spark
do not create or update Iceberg table/snapshot/success state
adapter staging output may exist and must not be described as trusted output
```

The existing documented assumption remains: machine-event v1 `event_id` is globally unique and
immutable; K1/S9 do not claim payload-equivalence checking for a reused `event_id`.

## 4. Approved Implementation Shape

Suggested new module:

```text
src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py
```

It should provide:

```text
run_recovered_telemetry_publish(...)
parse_args(...)
main(...)
```

Composition order:

```text
shared S8 require-recovery-complete gate
-> existing adapt_landing_to_batch
-> exact sealed-event-set vs canonical-adapter check
-> existing run_spark_machine_event_batch
-> persist one S9 evidence document
```

The S9 evidence must preserve, without conflating:

```text
edge_source_id
boot_session_id
edge sequence range
event_ids
Kafka coordinates
adapter source_hash
Spark run_id
quality result
Iceberg snapshot_id
publish status (published / skipped / quality_failed)
claim boundary
```

CLI behavior:

- incomplete/date-mismatch/exact-set mismatch: non-zero, no Spark/Iceberg publish;
- S7 `quality_failed`: non-zero, no snapshot/success-state advance;
- `published` or same-source `skipped`: zero.

Suggested Airflow wrapper:

```text
dags/manufacturing_recovered_telemetry_publish.py
dag_id: manufacturing_recovered_telemetry_publish
task_id: recovered_telemetry_publish_task
schedule=None / catchup=False / max_active_runs=1
```

It calls one validated S9 CLI command assembled in `orchestration.py`. No transform, coverage,
quality, or Iceberg logic may appear in the DAG body.

## 5. Test Contract

### Pure / base-environment tests

Required:

1. Existing S8 promotion still uses the shared gate and preserves all S8 behavior.
2. Incomplete recovery stops before adapter/Spark/Iceberg/evidence output.
3. Requested date mismatch stops before downstream output.
4. Complete recovery with exact canonical event set calls S7 with the adapter's existing
   `csv_path` and `source_hash`.
5. An extra accepted event for the same business date causes exact-set mismatch and blocks S7.
6. Other-date accepted events do not poison the selected session.
7. Returned and persisted S9 evidence contain the same edge/Kafka/source/run/snapshot identity
   chain.
8. `quality_failed` makes the CLI exit non-zero.
9. Same-source `skipped` exits zero.
10. Command builder quotes literal and Jinja parameters correctly.
11. Optional DagBag test checks DAG id, one task, command entrypoint, and `max_active_runs=1`.

Do not mock away the Core ordering. At least one composition test must prove the S7 callable is
not invoked when recovery or exact-set validation fails.

### Spark integration

Using local Spark/Iceberg:

```text
complete recovered session -> published + quality passed + snapshot id
same immutable session/landing retry -> skipped + same snapshot id
incomplete session -> no Spark/Iceberg state
```

Do not duplicate S7's correction/other-date/rounding matrix; those remain inherited S7 evidence.

### Runtime scenario

Build one reproducible runbook, preferably:

```text
scripts/verify_recovered_telemetry_publish.sh
```

It must reuse the existing pinned Kafka and Spark assets rather than copy broker/session setup.
Required state evidence:

```text
broker absent while S8 spool is created and sealed
real local Kafka partial replay -> S8 recovery/publish gate blocks
complete replay -> recovery_complete=true
S9 direct CLI -> Iceberg status=published, quality passed, snapshot id present
same S9 direct CLI retry -> status=skipped, same source_hash and snapshot id
edge event ids == selected adapter event ids
identity chain is present in persisted S9 evidence
no broker process remains after the runbook
```

Airflow runtime:

```text
airflow dags test manufacturing_recovered_telemetry_publish <logical-date> --conf ...
```

Use the completed S8 spool/landing and a separate clean local warehouse/evidence directory.
Record DAG import, rendered command, task exit, publish/skip status, and snapshot evidence.
`dags test` is sufficient for S9 because standalone/LocalExecutor has already been verified for
the same local Spark worker dependency boundary. Do not claim scheduler/executor operation from
this S9 run.

If the isolated Airflow environment is unavailable, install from the pinned
`requirements-airflow.txt`, `requirements.txt`, and `requirements-spark.txt`. Do not change
dependency pins unless a direct incompatibility is reproduced and reported first.

## 6. Allowed Changes

Implementation:

```text
src/manufacturing_data_platform/edge_recovery.py
src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py     new
src/manufacturing_data_platform/orchestration.py
dags/manufacturing_recovered_telemetry_publish.py                          new
```

Tests/runtime:

```text
tests/test_edge_recovery.py
tests/test_recovered_telemetry_publish.py                                  new
tests/test_orchestration.py
tests/test_airflow_dags.py
scripts/verify_recovered_telemetry_publish.sh                              new
scripts/recovered_telemetry_publish_verification.py                        new if needed
```

Design/evidence after implementation and runtime pass:

```text
learn/reference-decisions/recovery-gated-publish-boundary.md               new
learn/reference-decisions/README.md
learn/system-design/slices/09-recovery-gated-spark-iceberg.ko.md           new
learn/system-design/README.md
learn/system-design/00-service-purpose-charter.md
learn/system-design/01-system-traceability-map.ko.md
README.md
README.ko.md
ROADMAP.md
ROADMAP.ko.md
DESIGN.md
DESIGN.ko.md
PROJECT_PROGRESS_MAP.md
PROJECT_PROGRESS_MAP.ko.md
VERIFICATION_LOG.md
```

This implementation package itself may be updated only for lifecycle status and return summary.

## 7. Forbidden Changes

```text
src/manufacturing_data_platform/kafka_ingestion/*
src/manufacturing_data_platform/pipeline/spark_machine_event_batch.py
src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py
existing K1/K1.5/S7/S8 runtime scripts except the explicitly allowed S8 test/refactor file
requirements*.txt
existing DAG behavior
full task split (bronze/silver/gold/quality/catalog)
new Kafka/Spark/Airflow/Iceberg dependencies
unrelated refactors or file reorganization
blog drafts, publishing registry, resume files
commit / push / publication
```

Do not expand the slice because a production platform would normally use more services.

## 8. Verification Commands

At minimum:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_edge_recovery.py \
  tests/test_recovered_telemetry_publish.py tests/test_orchestration.py
PYTHONPATH=src .venv/bin/python -m pytest -q
PYTHONPATH=src python -m pytest -q tests/test_recovered_telemetry_publish.py \
  tests/test_spark_machine_event_batch.py
PYTHON_BIN=python ./scripts/verify_recovered_telemetry_publish.sh
./scripts/verify_edge_recovery.sh
./scripts/verify_kafka_k1.sh
./scripts/verify_kafka_k1_5.sh
PYTHON_BIN=python ./scripts/verify_spark_machine_event_batch.sh
python -m py_compile dags/manufacturing_recovered_telemetry_publish.py
git diff --check
```

Airflow:

```text
run the optional DagBag suite in the pinned Airflow environment
run one local `airflow dags test` with the completed S8 spool/landing
```

Report skip reasons and exact interpreter/environment. Do not combine test counts from different
interpreters as if they were one suite.

## 9. Claim Boundary

Allowed only after Codex independent review:

```text
Implemented a synthetic, local, bounded recovery-gated batch path that binds one sealed edge
session and its real local Kafka landing evidence to the existing deterministic adapter,
Spark quality gate, and one Iceberg gold-table snapshot. Incomplete recovery and same-date
event-set mismatch block Spark/Iceberg publication; same immutable input retry is a no-op.
The integrated CLI is callable through a thin local Airflow dags-test wrapper.
```

Required qualifiers:

```text
synthetic / local / bounded / one session-machine-date-topic-partition /
single local Iceberg gold table / Airflow dags test
```

Forbidden:

```text
production industrial IoT / autonomous factory platform
continuous or large-scale streaming
direct Kafka-to-Iceberg streaming sink
full Spark/Iceberg medallion platform
production/cluster Spark or performance improvement claim
production/HA/distributed Airflow operation
multi-partition/rebalance/concurrent-writer correctness
end-to-end exactly-once or distributed atomicity
real edge hardware/protocol integration
```

## 10. Required Return To Codex

Change package status to `returned-unreviewed / Codex review required` and append:

```text
preflight result
changed files and forbidden-file check
implementation decisions and deviations
targeted/base/Spark/Airflow/runtime verification
exact incomplete -> published -> skipped state evidence
event-set equality and full identity-chain evidence
failures encountered and fixes
remaining Unknowns/Backlog
risky judgments for Codex
git diff summary
```

Do not describe the result as approved. Do not commit, push, publish, or start another slice.

## 11. Return Summary (Claude -> Codex, 2026-07-23)

Status: `returned-unreviewed / Codex review required`. Not approved. No commit, push, or
publication was performed. No other slice was started.

### 11.1 Preflight result

```text
git show -s --oneline HEAD  ->  3b4b7fe Implement bounded edge recovery slice   (matches target)
git status --short          ->  clean except this untracked package directory   (matches expected)
```

Read before editing: charter, traceability map, scenario 05, slices 07/08, the S8 and S7 decision
notes, source contract 03, `edge_recovery.py`, `spark_machine_event_batch.py`, `batch_adapter.py`,
`orchestration.py`, the existing runbooks, and the latest `VERIFICATION_LOG.md` entries. No older
design-only conclusion was reused.

### 11.2 Changed files and forbidden-file check

Implementation:

```text
M  src/manufacturing_data_platform/edge_recovery.py                        (shared gate extracted)
A  src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py (283 lines)
M  src/manufacturing_data_platform/orchestration.py                        (+1 command builder)
A  dags/manufacturing_recovered_telemetry_publish.py                       (52 lines)
```

Tests/runtime:

```text
A  tests/test_recovered_telemetry_publish.py                    (386 lines, 14 tests)
M  tests/test_orchestration.py                                  (+2 tests)
M  tests/test_airflow_dags.py                                   (+1 test, +1 dag id assertion)
A  scripts/verify_recovered_telemetry_publish.sh                (41 lines)
A  scripts/recovered_telemetry_publish_verification.py          (300 lines)
```

Design/evidence:

```text
A  learn/reference-decisions/recovery-gated-publish-boundary.md
M  learn/reference-decisions/README.md
A  learn/system-design/slices/09-recovery-gated-spark-iceberg.ko.md
M  learn/system-design/README.md
M  learn/system-design/00-service-purpose-charter.md
M  learn/system-design/01-system-traceability-map.ko.md
M  README.md  README.ko.md  ROADMAP.md  ROADMAP.ko.md  DESIGN.md  DESIGN.ko.md
M  PROJECT_PROGRESS_MAP.md  PROJECT_PROGRESS_MAP.ko.md  VERIFICATION_LOG.md
M  this package (status + this return summary only)
```

Forbidden-file check: every path above is in §6. Nothing under `kafka_ingestion/`,
`spark_machine_event_batch.py`, `spark_iceberg_skeleton.py`, `requirements*.txt`, existing DAGs,
blog drafts, or the publishing registry was touched. Confirmed with `git status --short` and
`git diff --stat`.

### 11.3 Implementation decisions and deviations

1. **Shared gate extracted, not copied.** `require_recovery_ready(...)` now lives in
   `edge_recovery.py` and performs load -> `business_date` scope check -> coverage ->
   `recovery_complete` refusal. `promote_recovered_session` was rewritten to call it, so S8 and S9
   cannot drift apart. S8's external behaviour (exception types, messages, refusal point) is
   unchanged and the existing S8 suite passes untouched.
2. **Gate before Spark exists.** The gate runs before `batch_adapter` or any pyspark import. A
   refused session therefore creates no adapter directory and no warehouse — asserted in unit tests
   and re-proved at runtime with the real broker.
3. **Exact-session-input check is separate from completeness.** `assert_exact_session_input(...)`
   is a pure function requiring both count and set equality, and reports `extra_event_ids` /
   `missing_event_ids`. Rationale: the adapter selects every accepted event for the date, so
   membership alone would let a same-date outsider event ride into a "complete" publish.
4. **S7 is invoked, never re-expressed.** No transform, quality, or `overwritePartitions()` code
   exists in the new module. `test_complete_recovery_calls_s7_with_adapter_identity` and a
   `_SparkSpy` that raises if invoked prove both the call and the ordering.
5. **Lazy imports kept.** `edge_recovery.py` still imports no pyspark/pymongo at module level, so
   the Kafka runbook interpreter can evaluate the gate. S9 imports the adapter and Spark only after
   the gate passes.
6. **Runbook reuses pinned assets.** `verify_recovered_telemetry_publish.sh` delegates the broker
   to the existing `run_with_local_kafka.sh` and Spark/Iceberg to the S7 module. No broker or
   session setup was copied. Phase 1 refuses to run if a broker is already up, so "disconnected"
   is enforced rather than assumed.
7. **CLI failure surface.** Gate refusal, set mismatch, and `quality_failed` all exit 1 with a
   readable one-line reason instead of a raw traceback.

Deviations from the package: none in scope or contract. One judgment call is recorded in §11.8.

### 11.4 Verification

Interpreters are reported separately and counts are never summed across them.

```text
.venv/bin/python  3.12.3  (no pyspark, no airflow)
system python     3.12.3  (pyspark available)
/tmp/manufacturing-mini-airflow-venv  3.10.12, airflow 3.3.0 + pyspark
```

```text
targeted (.venv)   tests/test_edge_recovery.py tests/test_recovered_telemetry_publish.py
                   tests/test_orchestration.py        -> 40 passed, 2 skipped
                   (skips = the 2 S9 Spark integration tests; pyspark absent in .venv)
base suite (.venv) PYTHONPATH=src .venv/bin/python -m pytest -q  -> 121 passed, 17 skipped
Spark (system py)  tests/test_recovered_telemetry_publish.py
                   tests/test_spark_machine_event_batch.py       -> 28 passed in 33.07s
runbook            PYTHON_BIN=python ./scripts/verify_recovered_telemetry_publish.sh -> passed
regression         verify_edge_recovery.sh / verify_kafka_k1.sh / verify_kafka_k1_5.sh /
                   verify_spark_machine_event_batch.sh           -> all passed
dag compile        python -m py_compile dags/manufacturing_recovered_telemetry_publish.py -> valid
git diff --check                                                 -> clean
no Kafka broker process remained after the runbook
```

Airflow (isolated pinned environment, `AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags`, fresh SQLite
metadata DB created with `airflow db migrate`; no dependency pin was changed):

```text
DagBag suite: 6 passed in 1.89s
airflow dags test manufacturing_recovered_telemetry_publish 2026-06-29 -c '{...}'
  input : the completed sealed session from the runbook
  output: a separate clean warehouse (/tmp/manufacturing-mini-s9-airflow)
  DagRun state=success, BashOperator return code 0
  recovery_complete=true, sealed 3 == adapter selected 3
  status=published, quality_passed=true, snapshot_count=1, snapshot 9093910960272427618
```

`dags test` wiring proof only — no scheduler, executor, worker, HA, or production claim.

### 11.5 Exact incomplete -> published -> skipped evidence

From `/tmp/manufacturing-mini-s9-verification/s9_verification.json` (one sealed session,
`mc-101`, `2026-06-29`, 3 events, `expected_last_sequence=3`):

```text
phase 1  broker_absent_during_spool = true, sealed_event_count = 3

phase 2  partial replay : accepted 2, missing edge sequences [3], offsets [0, 1]
         S9 publish     : RecoveryIncompleteError
                          partial_publish_blocked   = true
                          no_warehouse_created      = true
                          no_adapter_created        = true
         complete replay: accepted 3, missing [], offsets [2, 3, 4]

phase 3  first : status = published, quality 7/7 pass, gold rows 1
                 source_hash e08a1cfbbbc93b9299aab774e132ec860c4be4dddd4aaa302a61f42c73993d7e
                 snapshot_id 3887281284647789193
         retry : status = skipped, same source_hash, same snapshot_id
         9/9 publish-phase checks pass
```

### 11.6 Event-set equality and identity chain

```text
edge event ids (3) == adapter selected event ids (3)   -> pass

edge sequence      [1, 2, 3]
event_id           evt-20260629-000001 / -000002 / -000003
Kafka coordinate   offsets [0, 1, 4] on manufacturing.machine-events.v1 / partition 0
source_hash        e08a1cfbbbc93b92...
spark run_id       2026-06-29-20260723T035242Z-a53450c2
iceberg snapshot   3887281284647789193
```

Offsets `[0, 1, 4]` against edge sequence `[1, 2, 3]` are direct runtime evidence that completeness
cannot be decided by offset continuity. The chain is present in both the returned document and the
persisted evidence file, and `identity_spaces_distinct` asserts the five spaces are not equal to
each other.

Counterexample coverage in the unit suite (14 tests):

```text
extra same-date accepted event        -> SessionInputMismatchError with extra_event_ids
other-date accepted events            -> do NOT poison the session (still publishes)
requested date != sealed session date -> EdgeSessionScopeError before any output
incomplete recovery                   -> no adapter, no Spark, no Iceberg
S7 callable invoked                   -> only after both gates pass (spy raises otherwise)
```

### 11.7 Failures encountered and fixes

1. **Two self-inflicted bugs in the new module, found and fixed before any verification run.**
   (a) a nonsense evidence expression `coverage["expected_last_sequence"] and spark["business_date"]`
   was replaced with `spark["business_date"]`; (b) the CLI surfaced raw tracebacks on gate refusal
   and now exits 1 with a readable reason.
2. **Runbook check `identity_chain_persisted` failed on the first full run.** Diagnosis: a
   verification-script bug, not an implementation bug — both publishes wrote to the same default
   evidence path, so the persisted document belonged to the retry run while being compared against
   the first run's chain. They legitimately differ because S7 mints a fresh `run_id` on a skipped
   rerun. Fix: each run now writes its own evidence file (`s9_publish_first.json` /
   `s9_publish_retry.json`) and is compared against its own persisted document; an
   `identity_spaces_distinct` check was added at the same time. The re-run passed 9/9.

### 11.8 Risky judgments for Codex

1. **Inherited S7 rerun behaviour (highest-value item to review).** A `skipped` rerun returns a
   **newly minted `spark run_id`** while `source_hash` and `gold_snapshot_id` stay identical. S7 is
   a forbidden file, so this was recorded rather than changed. Consequence: "the identity chain is
   stable across reruns" is **false** and is not claimed anywhere; only `source_hash` and
   `gold_snapshot_id` are asserted invariant. If Codex wants run-identity stability on no-op
   reruns, that is an S7 change and needs its own package.
2. **Charter "Now / implemented" list was already stale.** It stopped at K1.5 — S7 and S8 were
   never added. I added only the S9 line rather than backfilling other slices' entries, to stay
   inside this package's scope. The S7/S8 gap is left for Codex to dispose of.
3. **Two indexes outside §6 are missing S8 and now S9**:
   `learn/system-design/slices/README.ko.md` (lists only up to 07) and the S8 section of
   `learn/system-design/01-system-traceability-map.ko.md` (S9 was added there; S8 still has no
   section). Not edited because `slices/README.ko.md` is not in the allowed list and the S8 section
   is S8's documentation debt.
4. **Airflow snapshot id differs from the runbook's** (`9093910960272427618` vs
   `3887281284647789193`) because the DAG run used its own clean warehouse per §5. This is a
   different table instance, not a second commit to the same table — stated explicitly so it is not
   misread as non-idempotency.
5. **The gate is not atomic with the publish.** If Spark fails after the gate passes, adapter
   output can remain on disk. Bounded and disclosed in the ADR boundary section, not fixed here.

### 11.9 Remaining Unknowns / Backlog

```text
late/out-of-order telemetry and sequence gaps                  (unchanged from S8)
multiple sessions publishing into one business_date            (no real pressure named yet)
adapter residue cleanup after a post-gate Spark failure         (failure-state slice candidate)
whether S7's fresh run_id on a skipped rerun should be fixed as a contract or changed
scheduler/executor-level Airflow evidence for this DAG          (dags test only today)
```

### 11.10 git diff summary

```text
17 files changed, 455 insertions(+), 8 deletions(-)   (tracked files)
+ 6 new untracked files (S9 module, tests, 2 scripts, DAG, 2 design docs)
git diff --check: clean
working tree left dirty and uncommitted, as required
```

## 12. Codex Review - Revision Requested (2026-07-23)

Review basis:

```text
current diff and package reviewed directly
focused base tests rerun: 40 passed, 2 skipped
full base suite rerun: 121 passed, 17 skipped
Spark-visible tests rerun: 28 passed
S9 Kafka -> Spark/Iceberg runbook rerun: passed
Airflow rerun unavailable in Codex environment because the reported /tmp venv no longer exists
```

The composition contract and runtime state transition are accepted in principle. Partial recovery
blocks before adapter/Spark state, complete recovery publishes, and the same source creates no new
Iceberg snapshot. The following changes are required on the current candidate diff.

### R1 - distinguish a Spark attempt from the run that created the snapshot (must fix)

On a skipped retry, current evidence pairs a newly minted `spark_run_id` with the existing
`iceberg_snapshot_id` inside `identity_chain`. The new attempt did not create that snapshot, so a
reader can misread the pair as a causal `run -> snapshot` relationship.

Revise S9 evidence without changing S7:

```text
spark.run_id may remain for compatibility, but define it as the current attempt id
identity_chain.spark_run_id -> identity_chain.spark_attempt_run_id
add iceberg.snapshot_relation:
  created_by_current_attempt  when status=published
  reused_from_prior_attempt   when status=skipped
add snapshot_created_by_current_attempt: true/false
do not claim the producer run_id is known on skip; S7 does not expose it
```

Update tests and runtime checks to prove:

```text
first and retry attempt run_ids differ
first snapshot_created_by_current_attempt=true
retry snapshot_created_by_current_attempt=false
source_hash and snapshot_id remain equal
persisted and returned evidence agree for each attempt
```

Update ADR/slice/README/design/traceability/verification wording so "identity chain" does not imply
that the skipped attempt created the reused snapshot.

### R2 - remove the false-pass identity check (must fix)

`identity_spaces_distinct` currently uses:

```text
edge_sequence != kafka_offsets OR source_hash != str(snapshot_id)
```

The second predicate is effectively always true, so the check can pass even if the intended runtime
counterexample disappears. Replace it with a narrowly named assertion that proves the observed
fact:

```text
edge_sequence_not_kafka_offsets:
  [1,2,3] != [0,1,4]
```

Identity-space separation is a schema/semantics claim, not something proven by arbitrary value
inequality. Keep the separate fields and document their meanings; do not claim a generic
"five values are unequal" test.

### R3 - tighten no-op and failure-output claims (must fix)

S7 still starts Spark, computes transforms, and evaluates quality before deciding `skipped`.
Therefore "same immutable session and landing retry is a no-op" is too broad. Replace S9-specific
uses with:

```text
same source retry creates no new Iceberg snapshot and performs no partition overwrite
```

Also revise the ADR row saying gate refusal, set mismatch, and quality failure all leave
"산출물 없음". Quality failure may leave adapter/S7 failure evidence; the correct boundary is:

```text
gate/date/set refusal: no Spark/Iceberg publish state; adapter may exist after set mismatch
quality failure: failure evidence may remain, but no snapshot/success-state advance
```

### R4 - verification-script cleanup (required)

In `recovered_telemetry_publish_verification.py`:

```text
remove the duplicate "phases": state dictionary key
fix the phase-publish docstring: it uses the system Spark interpreter selected by PYTHON_BIN,
not the project .venv in the verified command
```

Correct the Return Summary new-file count (`module + tests + two scripts + DAG + two design docs`
is seven new candidate files, excluding this pre-existing package).

### R5 - stale index cleanup (allowed in this revision)

Backfill only the already-implemented entries needed to remove the drift identified in §11.8:

```text
00-service-purpose-charter.md Now/implemented: add S7 and S8 before S9
learn/system-design/slices/README.ko.md: add S8 and S9
01-system-traceability-map.ko.md: add the missing S8 traceability section or explicitly link its
existing scenario/slice/ADR/code/test evidence
```

This revision explicitly allows `learn/system-design/slices/README.ko.md`.

### Required rerun

```text
focused base tests
full base suite
Spark-visible S7/S9 tests
S9 runtime runbook
S8/K1/K1.5/S7 regressions
DagBag + S9 airflow dags test in the isolated pinned environment
git diff --check
```

Return the package to:

```text
returned-unreviewed / Codex review required
```

Record exact changed files, evidence-schema example for both `published` and `skipped`, test
results per interpreter, and remaining unknowns. Do not commit, push, or publish.

## 13. Revision Return Summary (Claude -> Codex, 2026-07-23)

Status: `returned-unreviewed / Codex review required`. Not approved. R1-R5 are applied on the same
candidate diff — no rewrite, no scope change, no commit/push/publication, no new slice.

### 13.1 R1 - attempt vs. the run that created the snapshot (applied)

`build_evidence` now derives `created_here = spark["status"] == "published"` and emits:

```text
spark.attempt_run_id                            current attempt (spark.run_id kept as an alias)
identity_chain.spark_attempt_run_id             renamed from spark_run_id
identity_chain.snapshot_created_by_current_attempt
iceberg.snapshot_relation                       created_by_current_attempt | reused_from_prior_attempt
iceberg.snapshot_created_by_current_attempt     true | false
iceberg.producer_attempt_run_id                 the attempt id when published, null when skipped
```

`producer_attempt_run_id` is null on a skip on purpose: S7 does not expose the run that committed
the snapshot, so it is recorded as unknown rather than filled in with the current attempt. The
`identity_chain.note` now says explicitly that no `run -> snapshot` causal relation may be read
from the pair when `snapshot_created_by_current_attempt` is false.

Proven by (unit) `test_skipped_attempt_does_not_claim_it_created_the_snapshot`, the extended
identity-chain test, and (Spark) `test_spark_publish_then_retry_creates_no_new_snapshot`; and at
runtime by four new runbook checks (§13.6).

Wording updated in: S9 ADR, slice 09, `README.md`, `README.ko.md`, `DESIGN.md`, `DESIGN.ko.md`,
traceability map, both progress maps, `VERIFICATION_LOG.md`.

### 13.2 R2 - false-pass identity check removed (applied)

```text
removed: identity_spaces_distinct
         edge_sequence != kafka_offsets OR source_hash != str(snapshot_id)
         (the right predicate is effectively always true, so the check could survive the
          disappearance of the counterexample it was supposed to protect)

added:   edge_sequence_not_kafka_offsets
         [1, 2, 3] != [0, 1, 4]   — the observed fact, and only that
```

The docs now state that identity-space separation is a schema/semantics contract (each space has
its own field with a fixed meaning), not something proven by value inequality. The unit assertion
on the chain carries a comment saying the same thing.

### 13.3 R3 - no-op and failure-output claims tightened (applied)

```text
before: "same immutable session and landing retry is a no-op"
after : "same source retry creates no new Iceberg snapshot and performs no partition overwrite"
        + an explicit note that S7 still starts Spark, transforms, and evaluates quality
          before deciding to skip, so compute cost is NOT saved
```

Changed in the module's `CLAIM_BOUNDARY`, the ADR (decision table, failure table, boundaries,
claim boundary), slice 09, both READMEs, both DESIGN docs, both ROADMAPs, both progress maps.

The ADR failure table no longer collapses every refusal into "산출물 없음":

```text
gate / date refusal      no Spark, no Iceberg publish state, no adapter output
set mismatch             adapter staging may exist; it is not trusted output
quality failure          S7 failure evidence may remain; no snapshot and no success-state advance
```

The Spark integration test was renamed `test_spark_publish_then_retry_creates_no_new_snapshot` so
the test name states the actual invariant.

### 13.4 R4 - verification-script cleanup and count correction (applied)

```text
docstring: "--phase publish  project .venv" was wrong. It now says the Spark-capable interpreter
           selected by PYTHON_BIN in verify_recovered_telemetry_publish.sh, and notes that the
           verified command uses the system python because .venv has no pyspark.

new-file count: seven, not six — S9 module, S9 tests, two scripts, DAG, two design docs
                (verified with `git ls-files --others --exclude-standard`, excluding this package)
```

On the duplicate `"phases"` key: an AST scan of the script found **no literal duplicate dict key**,
so this was read as the real redundancy at that line — `"phases": state` persisted the whole
cross-process scratch dictionary a second time, and its children were already named `phase_*`.
Applied fix: the final document flattens the keys (`spool` / `broker` / `publish`) and
`phase_state.json` is deleted once `s9_verification.json` is written, so the phase results exist in
exactly one place. **If a different duplicate was meant, please point at the line and it will be
corrected in one edit.**

### 13.5 R5 - stale index backfill (applied)

```text
00-service-purpose-charter.md  Now/implemented: S7 and S8 entries added before S9
slices/README.ko.md            entries 8 (S8) and 9 (S9) added
01-system-traceability-map.ko.md   the missing "S8. 단절 구간을 봉인해 모으고..." section added,
                                   with scenario 05 / slice 08 / ADR / edge_recovery.py /
                                   test_edge_recovery.py / verify_edge_recovery.sh linked,
                                   plus an S8 row in the scenario/question-bank table
```

All links in the touched documents were checked programmatically; none are broken.

### 13.6 Rerun results

Per interpreter, never summed:

```text
.venv/bin/python 3.12.3 (no pyspark/airflow)   system python 3.12.3 (pyspark)
/tmp/manufacturing-mini-airflow-venv 3.10.12 (airflow 3.3.0 + pyspark 3.5.8)

focused (.venv)    tests/test_edge_recovery.py tests/test_recovered_telemetry_publish.py
                   tests/test_orchestration.py          -> 41 passed, 2 skipped
base suite (.venv) PYTHONPATH=src .venv/bin/python -m pytest -q  -> 122 passed, 17 skipped
Spark (system py)  tests/test_recovered_telemetry_publish.py
                   tests/test_spark_machine_event_batch.py       -> 29 passed in 38.34s
S9 runbook         PYTHON_BIN=python ./scripts/verify_recovered_telemetry_publish.sh -> passed
regressions        verify_edge_recovery.sh / verify_kafka_k1.sh / verify_kafka_k1_5.sh /
                   verify_spark_machine_event_batch.sh          -> all passed
git diff --check                                                -> clean
no Kafka broker process remained after the runbook
```

Publish phase now runs 13 checks, all pass:

```text
first_published                                first_quality_passed
snapshot_id_present                            retry_skipped
retry_same_source_hash                         retry_same_snapshot_id
retry_creates_no_new_snapshot            <- R3 (snapshot_count unchanged)
attempt_run_ids_differ                   <- R1
first_snapshot_created_by_current_attempt      <- R1
retry_snapshot_not_created_by_current_attempt  <- R1 (producer_attempt_run_id = null)
edge_event_ids_equal_adapter_event_ids
identity_chain_persisted                       (each attempt vs its own file)
edge_sequence_not_kafka_offsets          <- R2 ([1,2,3] != [0,1,4])
```

State transition re-observed: partial replay blocked with `no_warehouse_created=true` and
`no_adapter_created=true`; complete replay published with quality 7/7 and
`gold_snapshot_id=8860719031591076067`; retry skipped with the same `source_hash` and snapshot.

### 13.7 Evidence schema, both statuses (from this run)

```json
published:
  "spark":   { "attempt_run_id": "2026-06-29-20260723T093032Z-e6d89e47", "run_id": "<same>",
               "source_hash": "98a421e9...", "quality_passed": true }
  "iceberg": { "status": "published", "gold_snapshot_id": 8860719031591076067,
               "snapshot_count": 1, "target_partition_row_count": 1,
               "snapshot_relation": "created_by_current_attempt",
               "snapshot_created_by_current_attempt": true,
               "producer_attempt_run_id": "2026-06-29-20260723T093032Z-e6d89e47" }
  "identity_chain": { "edge_sequence": [1,2,3], "event_id": [...3 ids...],
               "kafka_coordinate": offsets [0,1,4], "adapter_source_hash": "98a421e9...",
               "spark_attempt_run_id": "...093032Z-e6d89e47",
               "iceberg_snapshot_id": 8860719031591076067,
               "snapshot_created_by_current_attempt": true }

skipped:
  "spark":   { "attempt_run_id": "2026-06-29-20260723T093044Z-e4beadc3", ... same source_hash }
  "iceberg": { "status": "skipped", "gold_snapshot_id": 8860719031591076067,
               "snapshot_count": 1,
               "snapshot_relation": "reused_from_prior_attempt",
               "snapshot_created_by_current_attempt": false,
               "producer_attempt_run_id": null }
  "identity_chain": { ... same edge/event/kafka/source_hash/snapshot ...,
               "spark_attempt_run_id": "...093044Z-e4beadc3",
               "snapshot_created_by_current_attempt": false }
```

The only fields that differ between the two attempts are the attempt id and the three relation
fields. `source_hash`, `gold_snapshot_id`, and `snapshot_count` are identical.

### 13.8 Airflow environment note (read this before re-verifying)

The pinned `/tmp/manufacturing-mini-airflow-venv` had disappeared from this machine as well — the
same condition Codex reported. It was **rebuilt from the existing pinned requirements with no pin
change**:

```bash
python3 -m venv /tmp/manufacturing-mini-airflow-venv          # Python 3.10.12, matching constraints-3.10.txt
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements-airflow.txt
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements.txt -r requirements-spark.txt
```

Result: Airflow 3.3.0 / Python 3.10.12 / pyspark 3.5.8. No incompatibility was encountered, so no
pin was touched.

```text
DagBag suite: 6 passed
airflow dags test manufacturing_recovered_telemetry_publish 2026-06-29 -c '{...}'
  AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags, fresh SQLite metadata DB via `airflow db migrate`
  input: the completed sealed session from the runbook
  output: its own clean warehouse (/tmp/manufacturing-mini-s9-airflow)
  DagRun state=success, BashOperator return code 0
  recovery_complete=true, sealed 3 == adapter selected 3
  status=published, quality_passed=true, snapshot_count=1
  gold_snapshot_id 1091721367780693312
  snapshot_relation=created_by_current_attempt
  producer_attempt_run_id == attempt_run_id (2026-06-29-20260723T093526Z-bf97a3c8)
```

Because `/tmp` is not durable here, this venv may vanish again; the three commands above rebuild it
exactly. `dags test` remains a wiring proof only — not scheduler/executor/HA/production evidence.

### 13.9 Changed files in this revision

```text
M src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py   R1, R3
M scripts/recovered_telemetry_publish_verification.py                       R1, R2, R4
M tests/test_recovered_telemetry_publish.py                                 R1, R2, R3 (+1 test)
M learn/reference-decisions/recovery-gated-publish-boundary.md              R1, R2, R3
M learn/system-design/slices/09-recovery-gated-spark-iceberg.ko.md          R1, R2, R3
M learn/system-design/00-service-purpose-charter.md                         R5
M learn/system-design/slices/README.ko.md                                   R5 (allowed by §12)
M learn/system-design/01-system-traceability-map.ko.md                      R1, R5
M README.md  README.ko.md  DESIGN.md  DESIGN.ko.md                          R1, R3
M ROADMAP.md  ROADMAP.ko.md  PROJECT_PROGRESS_MAP.md  PROJECT_PROGRESS_MAP.ko.md   R3
M VERIFICATION_LOG.md                                                       revision entry
M this package                                                              status + §13
```

Forbidden-file check unchanged: nothing under `kafka_ingestion/`, `spark_machine_event_batch.py`,
`spark_iceberg_skeleton.py`, `requirements*.txt`, existing DAGs, blog drafts, or the publishing
registry was touched. `requirements*.txt` in particular was read but not modified.

```text
18 tracked files changed, 608 insertions(+), 8 deletions(-)
7 new untracked candidate files (excluding this package directory)
git diff --check: clean; working tree left dirty and uncommitted
```

### 13.10 Two related spots found while sweeping, deliberately NOT changed

```text
scripts/edge_recovery_verification.py:233  (S8, forbidden by §7)
  Also has a check named identity_spaces_distinct. Checked: it does NOT have the R2 false-pass
  defect - it is a single predicate, edge_sequence != kafka_offsets, which is exactly the narrow
  assertion R2 asks for. Only the NAME is broader than what it proves. Left untouched because S8
  runtime scripts are forbidden here; flagged so the naming can be aligned in an S8 package if
  Codex wants it.

DESIGN.md:238 / DESIGN.ko.md:198  (S7 section)
  "Same-source rerun is a no-op" - the same imprecision R3 corrects, but in S7's own section.
  R3 scoped the fix to "S9-specific uses", so S7's wording was left alone rather than silently
  rewritten. Say the word and it is a two-line change.
```

The earlier S9 entry in `VERIFICATION_LOG.md` did contain "same immutable input retry is a no-op"
in its claim boundary. That line is an S9 claim, so it was corrected in place with an inline
pointer to the revision entry rather than left standing.

### 13.11 Remaining Unknowns / risky judgments

```text
R4 duplicate-key interpretation (above) - confirm the intended target if it was not the
  "phases": state redundancy
whether S7's fresh run_id on a skipped attempt should become a fixed contract or be changed
  (still an S7-package decision; S9 only records it honestly now)
adapter staging residue after a post-gate Spark failure - failure-state slice candidate
multiple sessions publishing into one business_date - no real pressure named yet
scheduler/executor-level Airflow evidence - dags test only
/tmp is not durable on this machine; pinned venvs must be rebuilt per §13.8
```

## 14. Codex Final Evidence-State Revision (2026-07-23)

The R1 published/skipped distinction is correct, and the reported test counts were independently
reproduced:

```text
focused base: 41 passed, 2 skipped
full base:    122 passed, 17 skipped
Spark:        29 passed
```

One untested status remains incorrect. `build_evidence` currently derives:

```python
created_here = spark["status"] == "published"
snapshot_relation = "created_by_current_attempt" if created_here else "reused_from_prior_attempt"
```

For an S7 `quality_failed` result, `gold_snapshot_id` is `None`, but S9 records:

```text
status=quality_failed
gold_snapshot_id=null
snapshot_relation=reused_from_prior_attempt
```

No snapshot was reused. This is a false evidence statement.

### Required fix

Make the status-to-snapshot relation exhaustive:

```text
published      -> created_by_current_attempt
                  snapshot_created_by_current_attempt=true
                  producer_attempt_run_id=current attempt

skipped        -> reused_from_prior_attempt
                  snapshot_created_by_current_attempt=false
                  producer_attempt_run_id=null (unknown in S7)

quality_failed -> no_snapshot
                  snapshot_created_by_current_attempt=false
                  producer_attempt_run_id=null
                  gold_snapshot_id=null
```

Reject an unexpected S7 status instead of silently classifying it as reuse.

Add a direct `build_evidence` or composition test that proves the full `quality_failed` evidence
shape, in addition to the existing CLI exit-code test. Persisted and returned evidence must agree.
Update the S9 ADR/slice and `VERIFICATION_LOG.md` only where needed to document `no_snapshot`.

The R4 duplicate-key report was caused by Codex reading two overlapping `sed` ranges that both
printed the boundary line; Claude's AST result was correct. The flattened final evidence and
scratch-file cleanup are acceptable and do not need to be reverted.

Rerun:

```text
focused base tests
full base suite
Spark-visible S7/S9 tests
git diff --check
```

The Kafka/Spark runbook and Airflow `dags test` need not be repeated because this fix only changes
the evidence relation for a non-published quality-failure branch; do not change runtime or DAG
code. Return to `returned-unreviewed / Codex review required`, with no commit/push/publication.

## 15. Final Return Summary (Claude -> Codex, 2026-07-23)

Status: `returned-unreviewed / Codex review required`. Not approved. No commit, push, or
publication. Only the §14 evidence branch was touched — no runtime, DAG, or gate code changed.

### 15.1 The defect, restated

It was mine, and Codex is right that it was a false statement rather than a cosmetic one:

```text
created_here = spark["status"] == "published"
snapshot_relation = "created_by_current_attempt" if created_here else "reused_from_prior_attempt"
```

A binary split over a three-valued status. `quality_failed` fell into the `else`, so the evidence
claimed a snapshot had been reused while `gold_snapshot_id` was null and nothing had been
committed. The CLI already exited 1 on that branch, so the behaviour was right and only the
recorded sentence was wrong — which is exactly the class of error the earlier R1 review was about.

### 15.2 Fix

`src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py`:

```python
SNAPSHOT_RELATION_BY_STATUS = {
    "published": "created_by_current_attempt",
    "skipped": "reused_from_prior_attempt",
    "quality_failed": "no_snapshot",
}

status = spark["status"]
if status not in SNAPSHOT_RELATION_BY_STATUS:
    raise UnexpectedSparkStatusError(...)          # never silently classified as reuse
snapshot_relation = SNAPSHOT_RELATION_BY_STATUS[status]
created_here = snapshot_relation == "created_by_current_attempt"
```

Resulting evidence per status:

```text
published      snapshot_relation=created_by_current_attempt
               snapshot_created_by_current_attempt=true
               producer_attempt_run_id=<current attempt>       gold_snapshot_id=<id>

skipped        snapshot_relation=reused_from_prior_attempt
               snapshot_created_by_current_attempt=false
               producer_attempt_run_id=null (S7 does not expose it)  gold_snapshot_id=<same id>

quality_failed snapshot_relation=no_snapshot
               snapshot_created_by_current_attempt=false
               producer_attempt_run_id=null                    gold_snapshot_id=null
```

`gold_snapshot_id` needed no special handling — it already passes through S7's `None`.

Two additions beyond the literal requirement, both small:

1. `identity_chain.snapshot_relation` is now present too. The chain is the block a reader scans
   for the `run -> snapshot` pairing, and it previously carried only the boolean, which cannot
   distinguish "reused" from "nothing exists".
2. The `identity_chain.note` now instructs the reader to check `snapshot_relation` before pairing
   `spark_attempt_run_id` with `iceberg_snapshot_id`, and states that `no_snapshot` means the id
   is null.

`UnexpectedSparkStatusError` is deliberately **not** added to the CLI's gate-refusal `except`
clause. That clause is documented as "no Spark started, no Iceberg state touched", which is false
for this error — it can only occur after S7 has run. It surfaces as an uncaught exception with a
nonzero exit, which is the honest outcome for a broken internal contract.

### 15.3 Tests added (both requested by §14)

```text
test_quality_failure_evidence_says_no_snapshot_not_reuse
  Runs the full composition through the real S8 gate and real K1 landing with an S7 stub that
  returns quality_failed. Asserts the entire evidence shape: relation=no_snapshot, created=false,
  producer=null, gold_snapshot_id=null in both `iceberg` and `identity_chain`, the attempt id is
  still recorded, and the persisted document equals the returned one for this status.

test_build_evidence_maps_every_status_and_rejects_unknown_ones
  Direct build_evidence test over all three statuses in one table, asserts
  set(SNAPSHOT_RELATION_BY_STATUS) is exactly the three known statuses, and asserts an unknown
  status ("committed_unpublished") raises UnexpectedSparkStatusError instead of defaulting.
```

The existing CLI exit-code test for `quality_failed` is unchanged and still passes; these are
additional, not replacements.

### 15.4 Rerun (§14 scope)

Per interpreter, never summed:

```text
focused (.venv)        tests/test_edge_recovery.py tests/test_recovered_telemetry_publish.py
                       tests/test_orchestration.py        -> 43 passed, 2 skipped
full base (.venv)      PYTHONPATH=src .venv/bin/python -m pytest -q  -> 124 passed, 17 skipped
Spark (system python)  tests/test_recovered_telemetry_publish.py
                       tests/test_spark_machine_event_batch.py       -> 31 passed in 33.89s
git diff --check                                                     -> clean
```

Deltas from the previous return: +2 tests everywhere (41->43 focused, 122->124 base, 29->31 Spark).
The 2 skips in `.venv` are still the two S9 Spark integration tests (no pyspark there); they run in
the system-python column.

Per §14, the Kafka/Spark runbook and the Airflow `dags test` were **not** rerun. Justification as
given: this change only alters the evidence relation on a non-published branch, and no runtime or
DAG code was touched. The runbook and Airflow results recorded in §13.6 and §13.8 remain the
runtime evidence for the published/skipped path.

### 15.5 Changed files

```text
M src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py
    exhaustive mapping, UnexpectedSparkStatusError, identity_chain.snapshot_relation, note
M tests/test_recovered_telemetry_publish.py                     +2 tests
M learn/reference-decisions/recovery-gated-publish-boundary.md  status->relation table, why
                                                                quality_failed is separate,
                                                                failure-state row
M learn/system-design/slices/09-recovery-gated-spark-iceberg.ko.md   core question + status branch
M VERIFICATION_LOG.md                                           §14 entry
M this package                                                  status + §15
```

Nothing else was touched. Forbidden files unchanged: `kafka_ingestion/*`,
`spark_machine_event_batch.py`, `spark_iceberg_skeleton.py`, `requirements*.txt`, existing DAGs,
the S9 runbook scripts, blog drafts, publishing registry.

### 15.6 Note on the R4 duplicate-key item

Acknowledged — §14 confirms it came from two overlapping `sed` ranges printing the boundary line,
and that the flattened final evidence plus scratch-file cleanup are acceptable as-is. Nothing was
reverted. §13.4's open question is now closed and needs no further action.

### 15.7 Remaining Unknowns (unchanged from §13.11 except the closed item)

```text
whether S7's fresh run_id on a skipped attempt should become a fixed contract or be changed
  (S7-package decision; S9 only records it honestly)
adapter staging residue after a post-gate Spark failure - failure-state slice candidate
multiple sessions publishing into one business_date - no real pressure named yet
scheduler/executor-level Airflow evidence - dags test only
/tmp is not durable on this machine; pinned venvs must be rebuilt per §13.8
CLOSED: R4 duplicate-key interpretation (resolved in §14)
```

## 16. Codex Acceptance (2026-07-23)

Status: `accepted-closed`.

Codex reviewed the current diff and direct source rather than relying on the return summary. The
three snapshot relations are exhaustive and evidence-safe:

```text
published      -> created_by_current_attempt
skipped        -> reused_from_prior_attempt
quality_failed -> no_snapshot
unknown        -> UnexpectedSparkStatusError
```

Independent rerun:

```text
focused base: 43 passed, 2 skipped
full base:    124 passed, 17 skipped
Spark-visible S7/S9: 31 passed
git diff --check: clean
```

The Kafka/Spark runbook and Airflow `dags test` were not repeated after §14 because that revision
only changed the non-published evidence relation. Their accepted runtime evidence remains in §13.
No S10 or portfolio-release work is included in this package.
