# Failure-State Model

ADR Status: Proposed

## Scenario

A run can fail at several different points.

Successful-run evidence answers only half of the operator question:

```text
What produced this trusted current number?
```

Failure-state evidence answers the other half:

```text
What failed, how far did it get, and did it change current state?
```

This note defines the small state model needed before building a read-only failure forensics report.

## Problem

The project now has several state surfaces:

```text
JSON catalog state
quality_report.json
Iceberg snapshots/history
publish evidence JSON
Airflow task state
```

If a run fails, an operator needs to know whether:

```text
no output was committed
quality failed before publish
table commit succeeded but evidence write failed
current state moved or stayed unchanged
```

Without a named failure-state model, a forensics report would not know which surfaces to read or how to describe partial progress.

## Partial States

Small local state enum:

| State | Meaning | Current-state impact |
|---|---|---|
| `pending` | run started, no output/table commit has completed yet | current unchanged |
| `failed_before_commit` | transform/write failed before a table commit or successful publish | current unchanged |
| `quality_failed` | pipeline ran but quality gate failed | current must not advance |
| `committed_unpublished` | table commit succeeded but run->snapshot evidence JSON write failed | table may be ahead of catalog evidence |
| `published` | quality-passed run was published and evidence was written | current may advance |
| `skipped` | idempotent retry reused existing successful state | current unchanged |

## Local Recording Surfaces

Pipeline side:

```text
JSON catalog _state
per-run catalog_entry.json
quality_report.json
run record fields
```

Table side:

```text
Iceberg .snapshots
Iceberg .history
publish evidence JSON
```

Airflow side:

```text
DagRun state
TaskInstance state
task logs
```

Airflow state is orchestration evidence. It is not sufficient by itself to claim data correctness.

## Contract

Local contract:

```text
latest_successful pointer advances only when:
  quality passed
  required output/evidence was written

failed run never advances latest_successful

publish retry for the same pipeline_run_id + source_hash
  must not create a new Iceberg snapshot

if table commit succeeds but evidence write fails
  the system must treat it as a reconciliation/forensics case,
  not as a clean published state
```

## Reference Pattern

In a production Iceberg design, Write-Audit-Publish can be modeled with branches:

```text
write to audit/staging branch
validate quality
fast-forward main only after validation
```

That avoids exposing quality-failed data to the main branch.

This project does not implement Iceberg branch WAP. The current local simplification is:

```text
JSON catalog latest successful state
-> publish only successful gold
-> write evidence JSON
```

## Decision

For the next local slice, build failure-state forensics as a read-only evidence view.

It should explain partial state by reading existing local evidence surfaces first. It should not add production incident management, rollback, alerting, branch WAP, or distributed transaction semantics.

## Test Contract

Future tests should cover:

```text
quality_failed run does not advance latest_successful
missing success state cannot be published
same published run retry is skipped
committed_unpublished is described as a reconciliation gap
Airflow task failure is not treated as data correctness evidence
```

## Claim Boundary

Allowed:

```text
local failure-state model
read-only evidence-based forensics
quality-failed current-state guard
table/catalog reconciliation question named as Backlog
```

Forbidden:

```text
production incident workflow
rollback system
Iceberg branch WAP implementation
exactly-once table/catalog transaction
distributed lock/concurrent writer handling
OpenLineage/DataHub incident graph
```

## Interview Line

```text
I modeled failure states separately from successful runs, because a scheduler success or failure does not by itself explain whether current data moved. The local project keeps this as read-only evidence forensics; production would likely use stronger publish gates such as Iceberg branch-based WAP.
```
