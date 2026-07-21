# S8 Edge/Cloud Recovery - Claude Implementation Package

> Package status: accepted-closed / Codex review complete
>
> Codex review required before commit/push

Lifecycle: `ready-for-delegation -> delegated-awaiting-return -> returned-unreviewed ->
revision-requested (optional) -> accepted-closed`.

## 1. Target And Preflight

```text
project: repository root (manufacturing-data-platform-mini)
target commit: 5b82038
expected working tree: clean except this untracked package
mode: Delegated Implementation
```

Before editing:

1. Run `git status --short` and `git show -s --oneline HEAD`.
2. Read the current scenario, K1/K1.5 code and tests, and latest `VERIFICATION_LOG.md` entries.
3. Treat current code/runtime evidence as truth. Do not reuse older design-only conclusions.
4. If the target commit or dirty boundary differs, stop and report the mismatch before editing.

Read first:

```text
learn/system-design/scenarios/05-industrial-telemetry-recovery.md
learn/system-design/slices/05-kafka-raw-ingestion.ko.md
learn/system-design/slices/06-kafka-landing-to-batch.ko.md
learn/reference-decisions/kafka-event-identity-and-key.md
learn/reference-decisions/kafka-offset-and-landing-commit.md
learn/system-design/source-contracts/02-kafka-machine-event-v1.md
src/manufacturing_data_platform/kafka_ingestion/contracts.py
src/manufacturing_data_platform/kafka_ingestion/landing.py
src/manufacturing_data_platform/kafka_ingestion/runtime.py
src/manufacturing_data_platform/kafka_ingestion/batch_adapter.py
tests/test_kafka_ingestion.py
tests/test_kafka_batch_adapter.py
scripts/run_with_local_kafka.sh
VERIFICATION_LOG.md (K1, K1.5, S7 entries)
```

## 2. Approved Scope

### Slice thesis

```text
Simulate one bounded disconnected edge session with an immutable local spool,
replay it through the existing local Kafka/K1 landing after reconnect,
and allow the existing K1.5 batch/gold path to run only after the sealed edge
sequence range is completely represented in the central accepted set.
```

This is a failure/recovery slice, not a new streaming platform.

Primary scenario:

- [`learn/system-design/scenarios/05-industrial-telemetry-recovery.md`](../../../system-design/scenarios/05-industrial-telemetry-recovery.md)

### Core question pull and accepted answers

| Question | Accepted S8 answer |
|---|---|
| What identifies the physical/business event? | Existing strict v1 `event_id`; the event payload contract is unchanged. |
| What identifies edge ordering? | `(edge_source_id, boot_session_id, sequence_no)`. It is separate from Kafka coordinates. |
| How can absence be distinguished from loss? | A bounded session is sealed with `expected_last_sequence`; completeness is the full range `1..N`. Without a seal, recovery cannot be declared complete. |
| What is durable edge progress? | Immutable persisted spool entries. Do not add a separately advanced mutable cursor that can disagree with the files. |
| When may an edge event be considered buffered? | Only after canonical envelope bytes and metadata are fsynced and atomically renamed on the same local filesystem. |
| How is replay deduplicated? | Kafka coordinates remain transport evidence; `event_id` prevents a producer replay at new offsets from increasing the accepted business-event set. |
| When may downstream trusted state advance? | Only after every sealed sequence maps to an accepted central `event_id`. Incomplete recovery must block K1.5 invocation. |
| What happens on repeated complete replay? | It may create duplicate transport evidence, but central accepted count, K1.5 canonical `source_hash`, and trusted batch result must not change; the second bridge run is skipped. |
| What is the runtime proof? | Spool while the broker is not running, start the existing local Kafka runtime, perform partial recovery, complete recovery, repeat replay, then run K1.5 processed -> skipped. |
| What is not claimed? | Real edge hardware/protocols, continuous service, power-loss durability, concurrent writers, HA, multi-partition ordering, or production operation. |

### Bounded source/identity contract

Use an edge envelope around the existing strict event; do not add fields to the v1 Kafka payload.

```text
format_version: 1
edge_source_id: path-safe non-empty identifier
boot_session_id: path-safe non-empty identifier
sequence_no: integer >= 1
event: existing strict machine-event v1 payload
```

S8 simplifications:

```text
one edge_source_id
one boot_session_id
one machine_id
one business_date
sequence starts at 1
one sealed recovery range
one Kafka topic / partition / consumer
single writer on a local Linux filesystem
```

The spool must reject:

- the same edge coordinate with different canonical bytes;
- duplicate `event_id` values at different edge sequence numbers;
- unsafe path identifiers;
- a seal whose expected range is not fully present in the local spool;
- append or changed seal after the session is sealed.

Same coordinate + same bytes is an idempotent reuse.

### State transitions to prove

```text
edge session open
-> append events 1..3 durably while no broker process is running
-> seal expected_last_sequence=3
-> reconnect and replay only events 1..2
-> central accepted total = 2; missing edge sequence = [3]
-> K1.5 promotion is blocked and no trusted batch output is created
-> replay events 1..3 at new Kafka offsets
-> events 1..2 are duplicate evidence; event 3 is newly accepted
-> central accepted total = 3; missing = []; recovery complete
-> K1.5 path runs and quality-passed gold is created
-> replay events 1..3 once more at new offsets
-> central accepted total remains 3
-> K1.5 canonical source_hash remains unchanged and bridge status is skipped
```

Do not infer edge completeness from Kafka offset continuity. Kafka offsets and edge sequence are
different spaces, and valid Kafka offset gaps already exist in K1.

## 3. Allowed Changes

Implementation and tests:

```text
src/manufacturing_data_platform/edge_recovery.py                  new
tests/test_edge_recovery.py                                      new
scripts/edge_recovery_verification.py                             new
scripts/verify_edge_recovery.sh                                  new
```

Design/evidence alignment after tests pass:

```text
learn/reference-decisions/edge-buffer-and-recovery-progress.md    new
learn/reference-decisions/README.md                               link/status only
learn/system-design/source-contracts/03-edge-recovery-envelope.md new
learn/system-design/slices/08-edge-cloud-recovery.ko.md           new
learn/system-design/README.md                                     link/status only
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

If a small package export in `src/manufacturing_data_platform/__init__.py` is genuinely needed,
it is allowed. Prefer direct module imports and leave it unchanged otherwise.

## 4. Forbidden Changes

```text
src/manufacturing_data_platform/kafka_ingestion/contracts.py
src/manufacturing_data_platform/kafka_ingestion/landing.py
src/manufacturing_data_platform/kafka_ingestion/runtime.py
src/manufacturing_data_platform/kafka_ingestion/batch_adapter.py
tests/test_kafka_ingestion.py
tests/test_kafka_batch_adapter.py
scripts/run_with_local_kafka.sh
scripts/verify_kafka_k1.sh
scripts/verify_kafka_k1_5.sh
requirements*.txt
dags/
blog drafts or publication registry
```

Also forbidden:

```text
real OPC UA / MQTT / ROS 2 / DDS integration
Spark Structured Streaming, Flink, watermark/window work
new broker, database, queue, framework, or dependency
multiple machines/sessions/partitions or concurrent writers
production/HA/scale claims
unrelated refactors or file reorganization
commit / push / publication
```

The existing K1/K1.5 modules are regression boundaries and must be reused by public APIs only.
If the approved contract cannot be implemented without changing one, stop and report why instead
of widening scope.

## 5. Implementation Tasks

### Task 1 - Immutable edge spool

Implement a small, deterministic API in `edge_recovery.py` for:

- validating/canonicalizing the edge envelope while reusing `validate_machine_event`;
- appending one immutable sequence entry through staging -> file fsync -> atomic rename ->
  parent-directory fsync;
- idempotent reuse and conflict detection;
- sealing one session with `expected_last_sequence`;
- loading and validating the sealed session without trusting filenames alone.

Do not put wall-clock values into content identity. If observational timestamps are emitted, keep
them outside canonical bytes/source identity.

### Task 2 - Coverage and promotion gate

Implement read-only coverage calculation that compares the sealed edge event IDs to
`load_landing_index(landing_dir)["accepted_events"]` and returns structured evidence:

```text
expected_sequence_count
central_accepted_sequence_count
missing_sequences
recovery_complete
edge identities and event IDs
claim boundary
```

Add a bounded promotion function that refuses to call the existing K1.5 `run_bridge` while
recovery is incomplete. A failed guard must not create adapter/lakehouse output. Once complete,
delegate to `run_bridge` without copying its transform/quality logic.

### Task 3 - Local Kafka recovery runtime

Use the existing `produce_events`, `consume_and_land`, and `scripts/run_with_local_kafka.sh`.

The verification wrapper should:

1. prepare and seal the edge spool before starting Kafka;
2. start the existing pinned local broker through the shared runbook;
3. produce/consume partial, complete, and repeated replay phases at new Kafka offsets;
4. assert accepted totals `0 -> 2 -> 3 -> 3` and the expected missing sequence transition;
5. after the broker phase, use the project `.venv` to run the K1.5 promotion gate and prove
   `processed -> skipped` with an unchanged canonical `source_hash`;
6. write one structured evidence JSON under `/tmp`, with a self-declared claim boundary.

It is acceptable for the broker phase and K1.5 phase to be separate subprocesses because their
contract is the persisted spool/landing evidence. Do not copy the shared Kafka runbook.

### Task 4 - Tests

Always-on tests must cover at least:

1. canonical deterministic spool entry and sealed manifest;
2. same coordinate/same bytes reuse;
3. same coordinate/different bytes conflict;
4. unsafe identifier, duplicate event ID, missing sequence at seal, and append-after-seal rejection;
5. partial coverage reports the exact missing sequence;
6. incomplete recovery blocks K1.5 and creates no downstream output;
7. complete recovery permits K1.5 and produces quality-passed gold;
8. repeated producer replay at new Kafka coordinates does not increase the accepted business set;
9. duplicate-only replay leaves K1.5 canonical source hash unchanged and rerun status skipped;
10. edge sequence, `event_id`, Kafka coordinate, `source_hash`, and `run_id` remain distinguishable
    in evidence.

The pure/integration test may construct `KafkaRecord` values directly; broker runtime proof remains
separate. Do not require Kafka or Spark for the default suite.

### Task 5 - Documentation and evidence

Only after implementation verification succeeds:

- change scenario 05 from `Proposed` to `implemented / local bounded recovery verified`;
- add a thin slice map and ADR/source contract;
- update EN/KO public documents consistently without copying volatile test counts into overview docs;
- append the commands, state transitions, failures, and bounded claim to `VERIFICATION_LOG.md`;
- keep industrial benchmark references as design context, not implementation evidence.

## 6. Verification Contract

Run and report all of the following:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_edge_recovery.py
PYTHONPATH=src .venv/bin/python -m pytest -q
./scripts/verify_edge_recovery.sh
./scripts/verify_kafka_k1.sh
./scripts/verify_kafka_k1_5.sh
git diff --check
```

If system Spark tests are unaffected, do not run them just to increase a test count. S8 does not
change Spark/Iceberg code. If implementation unexpectedly imports Spark, treat that as a scope bug.

Scenario acceptance must explicitly show:

```text
broker absent during spool preparation
sealed edge events = 3
partial central accepted = 2; missing sequence = [3]; promotion blocked
complete central accepted = 3; missing = []; K1.5 processed; quality passed
repeated replay central accepted = 3; K1.5 skipped; source_hash unchanged
```

Release/operation gate:

```text
no persistent service deployment or migration
runtime artifacts only under /tmp
runbook cleans up local broker state/processes
no credential or private-path content in tracked files
```

## 7. Claim Boundary

Allowed after Codex independently verifies the result:

```text
Implemented a bounded local edge-recovery simulation with an immutable sealed spool,
replayed synthetic machine events through a real local Kafka broker into the existing K1 landing,
blocked downstream batch/gold while recovery was incomplete, and verified complete and repeated
replay without accepted-set or trusted-result duplication.
```

Required qualifiers: `synthetic`, `local`, `bounded`, `simulation`, `single machine/session/partition`.

Forbidden:

```text
built or operated an industrial IoT / autonomous factory platform
real edge gateway or offline product buffer
OPC UA / MQTT / ROS 2 / DDS integration
continuous or large-scale real-time streaming
power-loss-safe or distributed durability
multi-partition ordering/rebalance correctness
production Kafka/Spark/Airflow operation
exactly-once end to end
digital twin, anomaly detection, predictive maintenance, or machine control
```

## 8. Required Return To Codex

Change this package status to `returned-unreviewed / Codex review required` and append a return
summary containing:

```text
changed files
implementation decisions and any deviation from this contract
targeted/full/runtime verification results
the exact 0 -> 2 -> 3 -> 3 state evidence
K1.5 processed -> skipped evidence and source_hash comparison
failures encountered and how they were resolved
remaining Unknowns and Backlog
risky judgments Codex must re-check
git diff summary
```

Do not describe the result as approved. Do not commit, push, publish, or start the next slice.

---

## 9. Return Summary (returned-unreviewed / Codex review required)

Preflight PASS: HEAD `5b82038` = target, tree clean except this package. K1/K1.5/S7 code + `VERIFICATION_LOG.md` treated as truth. commit/push/publication 없음. 다음 slice 시작 안 함.

### Changed files

```text
new code/tests/scripts:
  src/manufacturing_data_platform/edge_recovery.py
  tests/test_edge_recovery.py
  scripts/edge_recovery_verification.py
  scripts/verify_edge_recovery.sh
new design:
  learn/reference-decisions/edge-buffer-and-recovery-progress.md
  learn/system-design/source-contracts/03-edge-recovery-envelope.md
  learn/system-design/slices/08-edge-cloud-recovery.ko.md
modified docs:
  learn/system-design/scenarios/05-industrial-telemetry-recovery.md (Proposed -> implemented)
  learn/reference-decisions/README.md, learn/system-design/README.md (link/status only)
  README.md, README.ko.md, ROADMAP.md, ROADMAP.ko.md, DESIGN.md, DESIGN.ko.md,
  PROJECT_PROGRESS_MAP.md, PROJECT_PROGRESS_MAP.ko.md, VERIFICATION_LOG.md
UNCHANGED (forbidden): kafka_ingestion/*.py, test_kafka_ingestion.py, test_kafka_batch_adapter.py,
  run_with_local_kafka.sh, verify_kafka_k1.sh, verify_kafka_k1_5.sh, requirements*.txt, dags/,
  blog drafts, publication registry, src/manufacturing_data_platform/__init__.py
```

### Implementation decisions / deviations

- **Lazy K1.5 import (necessary deviation-adjacent choice).** `batch_adapter -> lakehouse` imports `pymongo` at module level, but the shared Kafka runbook venv intentionally lacks it. `run_bridge` is therefore imported **inside** `promote_recovered_session`, so the spool/coverage path stays importable in the broker phase. Verified: `edge_recovery` imports neither pyspark nor pymongo at module level.
- **Progress = immutable entry set.** No separate mutable cursor, per the accepted contract.
- **`entry.json` content IS the canonical envelope** (no wall-clock anywhere in identity). Loading re-canonicalizes and checks the directory name against the envelope, so filenames alone are never trusted.
- **Seal is idempotent-or-conflict**, decided before range re-validation (see Failures).
- **Three-phase runtime** (spool / broker / promote) as separate processes; contract between them is the persisted spool+landing evidence. The shared Kafka runbook is reused, not copied.

### Verification results

```text
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_edge_recovery.py  -> 11 passed
PYTHONPATH=src .venv/bin/python -m pytest -q                              -> 101 passed, 14 skipped
./scripts/verify_edge_recovery.sh                                        -> passed (broker 7/7, promote 7/7)
./scripts/verify_kafka_k1.sh                                             -> passed (regression)
./scripts/verify_kafka_k1_5.sh                                           -> passed (regression)
git diff --check                                                         -> clean
Spark tests intentionally not run: S8 touches no Spark/Iceberg code.
```

### Exact 0 -> 2 -> 3 -> 3 evidence

```text
spool    : broker absent; sealed edge events = 3 (expected_last_sequence=3); accepted total 0; missing [1,2,3]
partial  : replay [1,2] @ offsets [0,1] -> accepted +2 -> total 2; missing [3]; recovery_complete false; promotion BLOCKED
complete : replay [1,2,3] @ NEW offsets [2,3,4] -> accepted +1, duplicate 2 -> total 3; missing []
repeat   : replay [1,2,3] @ NEW offsets [5,6,7] -> accepted +0, duplicate 3 -> total 3
accepted_total transition = 0 -> 2 -> 3 -> 3
```

### K1.5 processed -> skipped and source_hash comparison

```text
first  promotion : lakehouse status = processed, quality_passed = true
second promotion : lakehouse status = skipped
source_hash      : 75d98a387601f6b532b756f640a7c2281813e9cd0b33d7a622c01b70ef22381a (UNCHANGED)
run_id           : 2026-06-29-20260721T042445Z-8ddc9ebf (UNCHANGED)
gold rows        : identical across reruns
identity spaces  : edge sequence [1,2,3] vs Kafka offsets [0,1,4] -> demonstrably different spaces
```

### Failures encountered and resolution

```text
1 test failure during implementation (real logic bug, not a test accommodation):
seal_edge_session validated the 1..N range BEFORE checking an existing seal, so re-sealing with a
different expected_last_sequence surfaced "spool also holds sequences [3]" instead of the correct
"already sealed with a different expected_last_sequence" conflict.
Fix: decide reuse-vs-conflict on the existing seal first, then validate the range for a fresh seal.
Re-ran targeted suite -> 11 passed.
```

### Remaining Unknowns / Backlog

```text
Unknown : power-loss/SIGKILL crash consistency of the spool (only in-process ordering verified);
          NFS/object-store behaviour; concurrent writers.
Unknown : two-system atomicity between the landing commit and the spool seal (not addressed).
Backlog : late/out-of-order telemetry + sequence gap; multi edge_source/session/partition;
          real OT protocols; continuous service; asset/cross-source identity (Cognite lane).
```

### Risky judgments Codex must re-check

```text
1. Lazy `run_bridge` import to keep the broker phase importable in the Kafka venv — confirm this is
   acceptable rather than a packaging change.
2. Completeness measured only by event_id membership. If the same event_id were ever legitimately
   re-emitted for a different physical event, coverage would over-report. Current v1 contract treats
   event_id as unique business identity, so this holds — confirm.
3. Seal omits any wall-clock value (chosen for reproducibility). If an audit trail needs sealed_at,
   it must live outside canonical identity.
4. Scenario 05 flipped to "implemented / local bounded recovery verified" while the ADR stays
   Proposed. Confirm the split is what you want before promotion.
5. The broker phase reuses one stable consumer group so each replay consumes only newly produced
   messages; a different group/reset strategy would change the offsets (not the accepted totals).
```

### git diff summary

```text
See `git status --short` / `git diff --stat` in the return message. No commit, no push, no publication.
```

---

## 10. Codex Review - Revision Requested (2026-07-21)

Decision: **revise**. Do not commit, push, publish, or start another slice.

The core direction, lazy `run_bridge` import, wall-clock-free canonical identity, and
`event_id`-based recovery coverage are acceptable for this bounded slice. Targeted tests pass, but
the candidate does not yet enforce two load-bearing contracts and overstates one runtime check.

### H1 - Enforce the declared one-machine / one-business-date session scope

The package and public claim require one machine and one business date, but the implementation
currently accepts mixed values in one sealed session. Codex reproduced this with two entries:

```text
mixed_dates       = [2026-06-29, 2026-06-30]
mixed_machines    = [mc-101, mc-202]
seal              = success
recovery_complete = true
promotion date    = 2026-06-29
silver rows       = 1 of 2 sealed events
```

This violates the thesis that every event in the sealed range is represented in the promoted
bounded batch.

Required revision:

1. Derive and persist the session `machine_id` and `business_date` in the seal, or otherwise make
   them explicit validated session invariants.
2. Reject a fresh seal containing more than one `machine_id` or more than one `business_date`.
3. Reject `promote_recovered_session(..., business_date=...)` when the requested date differs from
   the sealed session date, before creating adapter/lakehouse/evidence output.
4. Add tests for mixed machine, mixed date, and promotion-date mismatch, including no downstream
   side effects.

Do not broaden S8 to multiple machines or dates; enforce the existing bounded scope.

### H2 - Actually exercise the partial-recovery promotion gate in runtime verification

`phase_broker` currently records partial coverage and immediately proceeds to complete replay. It
does not call `promote_recovered_session` while sequence 3 is missing. Therefore the exact runtime
claim `promotion BLOCKED` is not produced by the runtime script; only the unit test proves it.

Required revision:

1. During the partial phase, invoke the real promotion gate and assert
   `RecoveryIncompleteError`.
2. Assert that adapter, lakehouse, trusted-pointer, and promotion evidence outputs do not exist.
3. Persist an explicit runtime check such as `partial_promotion_blocked=true` in phase evidence.
4. Keep the call before complete replay. The lazy import should let the incomplete branch fail
   before the Kafka runbook needs `pymongo` or the batch stack.
5. Update `VERIFICATION_LOG.md`, the package return summary, and slice wording from the new actual
   runtime result. Do not describe a unit-only assertion as runtime evidence.

### M1 - Revalidate the complete immutable seal contract on load/reuse

`load_sealed_session` currently ignores spool entries above `expected_last_sequence` and does not
fully validate the seal's source/session IDs, declared count, or exact declared sequence set.
Existing-seal reuse returns the JSON without this validation. This is weaker than the documented
"load and re-validate a sealed session" contract.

Required revision:

1. Validate seal `format_version`, `edge_source_id`, `boot_session_id`,
   `sealed_event_count`, and the exact sequence/fingerprint declaration.
2. Reject missing **and extra** spool entries after sealing; do not silently filter extras.
3. Verify every loaded entry belongs to the requested source/session and agrees with its path.
4. Reuse of an existing seal must pass the same validation before returning success.
5. Add focused tamper/extra-entry tests. This remains local single-writer validation, not a
   security or power-loss claim.

### M2 - Make the `event_id` assumption and Iceberg boundary precise

Accepted bounded assumption:

```text
event_id is a globally unique, immutable business-event identity in machine-event v1.
The same event_id with different payload is a producer contract violation, not a correction.
```

Record that assumption in the source contract/ADR and do not claim payload-equivalence checking
that K1 does not perform.

Also revise scenario 05's quality-failure variant. S8 invokes the K1.5 JSON-backed batch/gold path;
it does not invoke or test S7 Iceberg publish. Replace the current `Iceberg write` wording with the
actual inherited K1.5 successful-pointer boundary, and state that S8 does not exercise Iceberg
publication or a quality-failure runtime case.

### Status and verification on return

- Keep ADR/scenario/slice status as revision pending; do not promote to `Implemented` yet.
- Remove the stale return-summary note that the DESIGN numbering and README v0 wording remain
  pre-existing issues; both were already closed at target HEAD `5b82038`.
- Re-run focused tests, the normal suite, `verify_edge_recovery.sh`, K1, and K1.5 regressions.
- Report the exact new tests and runtime evidence. Set the package back to
  `returned-unreviewed / Codex review required` only after all revisions pass.

Codex independent checks performed before requesting revision:

```text
python3 -m pytest -q tests/test_edge_recovery.py -> 11 passed
mixed-machine/date counterexample              -> reproduced (seal and partial-date promotion passed)
.venv/bin/pytest                                -> unusable stale shebang pointing to the old
                                                   robot-data-platform-mini path; use
                                                   python -m pytest or recreate the venv entrypoints
```

---

## 11. Revision Return Summary (2026-07-21) — returned-unreviewed / Codex review required

All four findings addressed on top of the existing diff. No commit/push/publication, no new slice.
Status intentionally left **not promoted**: ADR/scenario 05/slice 08/source contract 03 all read
`revision-pending / Codex re-review required`.

### H1 — bounded session scope is now an enforced invariant

- `seal_edge_session` derives and **persists** `machine_id` and `business_date` in the seal.
- A fresh seal holding more than one `machine_id` or `business_date` raises `EdgeSessionScopeError`.
- `promote_recovered_session` rejects a requested `business_date` that differs from the sealed
  session date **before** adapter/lakehouse/evidence output exists.
- Codex's counterexample no longer reproduces: mixed machine/date can no longer be sealed, and the
  "promote 2026-06-29 with 1 of 2 sealed events" path is blocked.
- New tests: `test_seal_rejects_mixed_machine_id`, `test_seal_rejects_mixed_business_date`,
  `test_seal_persists_session_scope`,
  `test_promotion_rejects_business_date_mismatch_without_side_effects`.

### H2 — partial-recovery gate is now real runtime evidence

- `phase_broker` invokes the actual `promote_recovered_session` while sequence 3 is missing,
  asserts `RecoveryIncompleteError`, asserts adapter/lakehouse/promotion-evidence paths are absent,
  and persists `partial_promotion_blocked=true`. The call stays before complete replay.
- Confirmed the lazy `run_bridge` import lets the incomplete branch fail inside the Kafka runbook
  venv without the batch stack.
- Runtime output: `partial_promotion_blocked=true`, `no_downstream_output=true`,
  `RecoveryIncompleteError: recovery incomplete: missing edge sequences [3] of 1..3`.
- Broker-phase checks grew 7 -> 9. `VERIFICATION_LOG.md`, slice 08, and this summary now describe
  runtime evidence, not a unit-only assertion.

### M1 — complete seal re-validation on load and reuse

- `_validate_seal` checks `format_version`, `edge_source_id`, `boot_session_id`,
  `sealed_event_count`, the exact declared sequence set `1..N`, per-entry fingerprint/`event_id`,
  and session membership; `_load_entry` now verifies **every** path segment against the envelope.
- Missing **and extra** entries are rejected (no silent filtering); existing-seal reuse runs the
  same validation before returning.
- New tests: `test_entry_added_after_sealing_is_rejected_on_load`,
  `test_tampered_seal_manifest_is_rejected` (9 tamper mutations).

### M2 — assumption and Iceberg boundary made precise

- Source contract 03 and the ADR now state: `event_id` is a globally unique, immutable v1
  business-event identity; the same `event_id` with a different payload is a producer contract
  violation, not a correction. **No payload-equivalence checking is claimed**, because K1 performs
  none; a reused `event_id` for a different physical event would over-report coverage and is
  outside S8's detection scope.
- Scenario 05's quality-failure variant no longer says "Iceberg write". It now states the inherited
  K1.5 successful-pointer boundary and that **S8 neither invokes nor tests S7 Iceberg publish, nor a
  quality-failure runtime case**.

### Verification (all re-run)

```text
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_edge_recovery.py -> 17 passed (was 11)
PYTHONPATH=src .venv/bin/python -m pytest -q                             -> 107 passed, 14 skipped
./scripts/verify_edge_recovery.sh -> passed (broker 9/9 incl. partial promotion gate, promote 7/7)
./scripts/verify_kafka_k1.sh      -> passed
./scripts/verify_kafka_k1_5.sh    -> passed
git diff --check                  -> clean
runtime: accepted_total 0 -> 2 -> 3 -> 3; partial promotion BLOCKED with no downstream output;
         K1.5 processed -> skipped with unchanged source_hash and run_id; no broker left running.
Used `.venv/bin/python -m pytest` throughout: `.venv/bin/pytest` has a stale shebang pointing at an
old robot-data-platform-mini path.
```

### Failures encountered during this revision

```text
2 self-inflicted test defects, both fixed (implementation was correct in each case):
1. A new test referenced OTHER_DATE before it was defined in this module -> added the constant.
2. The seal-tamper test restored state by calling _seal() on an already-tampered seal (which now
   correctly raises), and one mutation asserted the range message while the count check fired
   first. Fixed the restore order and made that mutation also fix sealed_event_count so the exact
   sequence-range check is what rejects it.
```

### Remaining Unknowns / risky judgments for Codex

```text
1. Session scope is derived from spool content rather than declared up front. A single-entry
   session therefore trivially satisfies "one machine/one date" - acceptable for S8's bounded
   scope, but confirm you want derivation rather than an explicit caller declaration.
2. Extra-entry rejection makes a sealed session strictly immutable: a stray writer adding seq 4
   permanently fails load until the spool is repaired. This is intended (fail loud), not silent
   recovery - confirm.
3. Still Unknown: power-loss/SIGKILL crash consistency, NFS/object-store behaviour, concurrent
   writers, and two-system atomicity between the landing commit and the spool seal.
4. Backlog unchanged: late/out-of-order telemetry, multi edge_source/session/partition, real OT
   protocols, continuous service, cross-source asset identity.
```

---

## 12. Codex Acceptance (2026-07-21)

Decision: **accept / accepted-closed**.

Codex independently reviewed the revision diff and accepted both remaining design judgments:

- deriving the one-machine/one-business-date scope from sealed spool content is appropriate for
  this bounded slice;
- rejecting any entry added after sealing is the intended fail-loud immutable-seal behavior.

Independent verification:

```text
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_edge_recovery.py -> 17 passed
PYTHONPATH=src .venv/bin/python -m pytest -q                             -> 107 passed, 14 skipped
./scripts/verify_edge_recovery.sh -> passed
  broker checks 9/9; promote checks 7/7
  partial_promotion_blocked=true; no_downstream_output=true
  accepted_total 0 -> 2 -> 3 -> 3
  K1.5 processed -> skipped; source_hash/run_id unchanged within the run
./scripts/verify_kafka_k1.sh   -> passed
./scripts/verify_kafka_k1_5.sh -> passed
git diff --check               -> clean
```

No additional correctness, regression, or claim-boundary finding remains. Unknowns stay bounded
to power-loss/SIGKILL behavior, NFS/object-store semantics, concurrent writers, and landing-commit
to spool-seal atomicity. These are not S8 claims.
