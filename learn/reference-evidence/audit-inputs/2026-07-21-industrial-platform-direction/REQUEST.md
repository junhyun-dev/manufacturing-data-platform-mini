# Industrial Data Platform Direction - Delegated Documentation Package

> Status: accepted-closed
>
> Claude output and edits were reviewed and corrected by Codex. See `claude-audit.md` §I.

## 1. Target And Preflight

```text
project: repository root (`manufacturing-data-platform-mini`)
target commit: 862527b
expected working tree: clean except this request package created by Codex
mode: Delegated Documentation
review profile: System Traceability + Public Reader + External Benchmark
```

Mandatory preflight:

1. Read current `HEAD` and `git status` instead of reusing an earlier Kafka/Spark conclusion.
2. Treat `VERIFICATION_LOG.md` and current code/tests as implementation truth.
3. Report any state mismatch before editing.
4. Do not run or modify implementation. This package is documentation/research only.

## 2. Read First

```text
README.md
README.ko.md
ROADMAP.md
ROADMAP.ko.md
BENCHMARKS.md
BENCHMARKS.ko.md
DESIGN.md
DESIGN.ko.md
PROJECT_PROGRESS_MAP.ko.md
VERIFICATION_LOG.md (latest Kafka K1/K1.5 and S7 entries)
learn/system-design/00-service-purpose-charter.md
learn/system-design/00a-plain-project-map.md
learn/system-design/01-system-traceability-map.ko.md
learn/system-design/scenarios/00-scenario-seed.md
learn/system-design/scenarios/03-kafka-machine-event-ingestion.md
learn/system-design/scenarios/04-spark-machine-event-batch.md
learn/system-design/slices/05-kafka-raw-ingestion.ko.md
learn/system-design/slices/06-kafka-landing-to-batch.ko.md
learn/system-design/slices/07-spark-machine-event-batch.ko.md
```

Current facts to preserve:

```text
implemented/verified-local:
- synthetic batch bronze/silver/gold + quality/catalog/lineage
- EAV multi-format intake
- operator evidence report
- local Spark/Iceberg partition overwrite and publish
- local Airflow wrappers/runtime evidence within documented boundaries
- bounded Kafka K1 raw landing and K1.5 batch bridge
- S7 local Spark machine-event batch with Python parity and quality-gated Iceberg publish

not implemented:
- real PLC/sensor/robot source
- OPC UA, MQTT, ROS 2/DDS, MCAP
- edge gateway or disconnected durable buffer
- continuous/event-time streaming, watermarks, Flink/Structured Streaming
- asset hierarchy/Unified Namespace/digital twin
- anomaly model, predictive maintenance, closed-loop control
- production/HA/cluster operation
```

Known documentation drift to verify, not blindly copy:

- Korean docs exist and are linked from the public English README.
- `README.ko.md` names S7 but lacks a full S7 reader section and current reproduction path.
- `ROADMAP.md` Phase 3 still lists Kafka ingestion as unchecked despite K1/K1.5 being implemented.
- `ROADMAP.ko.md` does not include the implemented S7 section present in English.
- Existing docs explain tools well, but the next manufacturing service scenario and actors are not yet crisp.

## 3. Audit Goal

Answer these questions with repo evidence and current official sources:

1. What service is this project actually trying to provide to a plant data operator, process/quality engineer, and platform operator?
2. Does the public Korean reading path accurately cover the current S0-S7 implementation without becoming a second source of volatile test counts?
3. What recurring industrial-data problems do current external platforms solve, and which are relevant to this small local project?
4. What 5-7 realistic manufacturing scenarios can follow S7, and which one should be the next bounded vertical slice?
5. How should the roadmap separate implemented foundation, proposed industrial scenarios, and distant Physical-AI/closed-loop backlog?
6. Which benchmark ideas should this project copy, simplify, or avoid?
7. Does any wording imply real factory data, production streaming, digital twin, anomaly detection, or machine control that is not implemented?

The goal is not to add a technology wishlist. Start from operator/service scenarios and failure pressure.

## 4. Research Lanes And Source Rules

Use direct official product or project documentation. Record access date/version where relevant. Search snippets and generic vendor marketing are not sufficient.

| Lane | Confirm | Preferred direct sources |
|---|---|---|
| Edge continuity | offline collection, disk buffer, retry/sync, local processing boundaries | AWS IoT SiteWise Edge docs; Azure IoT Operations data-flow/MQTT buffer docs |
| OT ingestion | OPC UA/MQTT connectors, schemas, asset/device registry | AWS/Azure official docs; OPC Foundation only if a protocol claim is needed |
| Industrial contextualization | how asset, time-series, file/drawing, and source IDs are related | Cognite Data Fusion official docs |
| Unified Namespace | what problem a consistent asset/topic namespace solves | HighByte official architecture/docs; treat product claims as vendor claims |
| Event-time processing | only if a proposed scenario truly requires late/out-of-order/window state | Apache Flink official docs or Spark Structured Streaming official docs |
| Physical AI boundary | data/operations platform versus motion control/model research | current official role/architecture sources; keep inference clearly separate from hard real-time control |

At minimum examine:

```text
https://docs.aws.amazon.com/iot-sitewise/latest/userguide/gateways.html
https://learn.microsoft.com/en-us/azure/iot-operations/connect-to-cloud/overview-dataflow
https://docs.cognite.com/cdf
https://docs.cognite.com/cdf/integration/concepts/contextualization
https://www.highbyte.com/intelligence-hub/unified-namespace
```

Classify every important claim as `confirmed`, `inference`, `unknown`, or `stale-risk`.

## 5. Required Analysis Output

Write `claude-audit.md` in this directory with:

### A. Executive Verdict

- Is the current project foundation coherent?
- What is the strongest plain-language service thesis?
- What is the single most valuable next scenario, if any?

### B. Korean Documentation Audit

Provide an EN/KO drift table and identify only reader-visible gaps that need correction. Do not copy volatile test counts into overview docs.

### C. External Benchmark Matrix

For each external service/pattern:

```text
service/user problem
core state/contract
failure handled
copy / simplify / avoid
relevance to this repo
direct source
```

### D. Manufacturing Scenario Catalog

Produce 5-7 scenarios, for example but not limited to:

```text
edge/cloud disconnection and later replay
late/out-of-order telemetry and sequence gap
sensor/tag/unit/schema replacement
suspicious quality metric and source/telemetry RCA
late inspection correction and trusted-state republish
asset/drawing/time-series contextualization
versioned anomaly inference with human approval
```

Each scenario must state actor, trigger, input/output, invariant, failure/recovery, smallest evidence, and non-goals.

### E. Priority Recommendation

Rank the top three by:

```text
user/operational value
current evidence gap
reuse of S0-S7 assets
implementation risk
portfolio/career value
```

Recommend at most one next slice. It remains `Proposed`; do not create an implementation package or claim it is built.

### F. Safe Claim Boundary

Explicitly list safe and forbidden wording.

## 6. Allowed Documentation Edits

After the analysis, make only evidence-backed, low-drift edits to:

```text
README.ko.md
ROADMAP.md
ROADMAP.ko.md
BENCHMARKS.md
BENCHMARKS.ko.md
learn/system-design/README.md
learn/system-design/scenarios/05-industrial-telemetry-recovery.md   # optional, only if selected as Proposed
learn/reference-evidence/audit-inputs/2026-07-21-industrial-platform-direction/claude-audit.md
```

Editing requirements:

- Keep English implementation claims and Korean claims aligned.
- Add a concise S7 Korean explanation where needed.
- Replace the stale Phase 3 checklist with a scenario-led proposed roadmap, preserving completed K1/K1.5/S7 facts.
- Clearly separate `Implemented Foundation`, `Proposed Next Scenarios`, and `Backlog/Unknown`.
- Benchmark sections must map service pressure to a local decision; do not become vendor feature lists.
- Prefer links and concise summaries over duplicating design documents.
- Do not expand the Question Bank in this pass.
- Do not add implementation status, test counts, runtime claims, or screenshots not already supported by `VERIFICATION_LOG.md`.

Forbidden:

```text
all src/, tests/, dags/, scripts/, dependency files, data, runtime state
VERIFICATION_LOG.md
PROJECT_PROGRESS_MAP.md / PROJECT_PROGRESS_MAP.ko.md
implementation package creation
install/run/benchmark commands
commit, push, publication
```

## 7. Claim Boundary

Safe direction:

```text
The repo currently proves a synthetic/local/bounded manufacturing data-platform foundation.
It is researching industrial edge continuity and contextualization as proposed next scenarios.
```

Forbidden direction:

```text
built or operated an autonomous factory / digital twin / industrial IoT platform
real-time or large-scale streaming without runtime evidence
OPC UA/MQTT/ROS2/MCAP integration before implementation
predictive maintenance or anomaly detection before a model/evaluation slice
robot control, safety control, closed-loop actuation
production/HA/cluster claims
```

## 8. Return Contract

Before returning:

1. Set `claude-audit.md` status to `returned-unreviewed / Codex review required`.
2. Change this request status to `returned-unreviewed` only after all edits and the report are complete.
3. Run `git diff --check` only; do not run implementation tests.
4. Report changed files, source URLs, remaining Unknowns, and risky judgments for Codex.
5. Do not commit or push.
