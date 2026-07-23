# 00. Service Purpose Charter

상태: project thesis / scenario anchor
프로젝트: `manufacturing-data-platform-mini`

이 문서는 `manufacturing-data-platform-mini`가 왜 존재하는지, 어떤 사용자의 어떤 질문에 답하려는지, 그리고 어떤 기능이 그 질문에서 파생되는지 고정한다.

## 1. One-Sentence Service

`manufacturing-data-platform-mini`는 raw manufacturing-style/tabular files를 분석/ML 사용자가 믿고 쓸 수 있는 cataloged, versioned, quality-checked dataset/mart로 바꾸고, 운영자가 나중에 설명할 수 있는 증거를 남기는 작은 데이터 플랫폼이다.

이 repo 이름과 현재 구현 evidence는 manufacturing-style/tabular synthetic data로 맞췄다. ROS2/MCAP/session/sensor 같은 machine/session source slice는 아직 backlog다.

## 2. Why This Service Exists

raw file만 있으면 데이터 사용자와 운영자는 같은 질문을 반복한다.

```text
이 파일은 전에 처리한 것과 같은가?
이 데이터셋은 어떤 schema인가?
이 날짜 결과는 어느 source에서 왔는가?
품질검사는 통과했는가?
schema가 바뀌었는데도 조용히 지나간 건 아닌가?
같은 날짜를 다시 돌렸을 때 중복이 생기지 않았는가?
event source라면 durable landing 전에 offset이 commit되지는 않았는가?
```

이 프로젝트는 raw file을 바로 "쓸 수 있는 데이터"라고 주장하지 않는다. 대신 사용자가 판단할 수 있는 상태와 증거를 만든다.

```text
source identity
schema identity
bronze/silver/gold states
quality result
catalog/version metadata
lineage/run evidence
idempotent rerun evidence
durable event landing / replay evidence
```

## 3. Actors

| Actor | Wants to know / do | Creates | Verifies | Must not do |
|---|---|---|---|---|
| Source owner | daily manufacturing-style data를 제공 | source CSV | source delivery | real company/customer data를 공개 repo에 넣기 |
| Data engineer / operator | run 성공/실패, 품질, 원인 추적 | pipeline run, catalog/lineage record | quality, idempotency, schema drift | business logic을 orchestration wrapper에 숨기기 |
| Analyst / data user | 어떤 metric을 믿고 쓸 수 있는지 판단 | downstream analysis | catalog, quality status, freshness | warning/failure run을 production truth처럼 쓰기 |
| ML / manufacturing/ML data user | 어떤 dataset version으로 학습/평가했는지 재현 | dataset usage | source_hash, schema_hash, version manifest | raw file만 보고 version을 추정하기 |
| Interviewer / reviewer | claim이 code/test evidence와 맞는지 확인 | review question | tests, logs, docs | design-only를 implemented로 오해하기 |

## 4. Primary Scenario

```text
1. Source owner가 synthetic manufacturing-style CSV를 제공한다.
2. Pipeline이 source_hash와 schema_hash를 계산한다.
3. Pipeline이 bronze raw copy와 manifest를 남긴다.
4. Pipeline이 silver에서 business_date 필터링, type 정리, natural key dedup을 수행한다.
5. Pipeline이 gold daily metrics를 만든다.
6. Quality suite가 row reconciliation, conservation, not_null, unique, accepted_values, range, freshness, schema_drift를 확인한다.
7. Catalog/lineage record가 run_id, source_hash, schema_hash, quality result, layer parent links를 저장한다.
8. 사용자는 catalog/run record를 보고 데이터셋을 사용할지 판단한다.
9. 같은 source가 다시 들어오면 idempotency gate가 기존 successful run을 재사용한다.
```

## 5. User / Operator Questions

```text
이 입력은 전에 처리한 것과 같은가?
이 결과는 어느 source/run에서 왔는가?
source schema가 바뀌었는가?
row가 처리 중 유실되지 않았는가?
집계 수치가 silver와 보존 관계를 갖는가?
품질검사는 통과했는가?
같은 날짜를 다시 처리해도 중복이 생기지 않는가?
새 파일 형식이 오면 code change 없이 onboard할 수 있는가?
```

## 6. States The Service Must Create

| State | Why it exists | Example |
|---|---|---|
| source identity | 같은 입력인지 판단 | `source_hash` |
| schema identity | source 구조 변화 감지 | `schema_hash`, `schema_drift` |
| bronze | raw 보존 + replay 근거 | raw copy + manifest |
| silver | typed/deduped common data | manufacturing events or EAV long rows |
| gold | 사용자가 보는 mart | daily line/product metrics, entity daily metrics |
| quality result | publish/use 판단 근거 | `name/status/expected/actual/detail` |
| run/catalog record | 성공/실패/버전 inspect | `lakehouse_runs`, JSON state |
| lineage evidence | input -> output 원인 추적 | layer parent links |
| event landing evidence | Kafka redelivery/replay와 batch 전환 근거 | topic/partition/offset, event_id, accepted/quarantine manifest |
| table publish evidence | 정정 결과와 table commit 추적 | `business_date`, pipeline `run_id`, Iceberg `snapshot_id` |

## 7. Feature From Question Map

| Service question | Feature / state / contract |
|---|---|
| 이 입력은 전에 처리한 것과 같은가? | `source_hash`, idempotency gate |
| source 구조가 바뀌었는가? | actual-header `schema_hash`, `schema_drift=warn` |
| raw와 mart 사이에서 row가 사라졌는가? | source-to-silver reconciliation |
| 집계가 additive measure를 보존하는가? | silver-to-gold / EAV-to-gold conservation |
| 데이터를 열지 않고 무엇을 알 수 있는가? | catalog/version metadata |
| 이 숫자는 어디서 왔는가? | run record + lineage parent links |
| 새 source format은 어떻게 온보딩하는가? | JSON mapping config -> EAV -> gold |
| event를 잃지 않고 batch 경로로 넘기는가? | durable Kafka landing -> deterministic one-date adapter -> existing batch spine |
| 단절 구간이 전부 복구된 뒤에만 결과를 발행하는가? | 봉인 세션 readiness gate + 봉인 event 집합 == batch 입력 집합 검사를 Spark 시작 전에 통과해야 발행 |
| 같은 날짜의 정정 결과를 어떻게 교체하는가? | Iceberg `business_date` partition overwrite + snapshot evidence |
| scheduler가 business logic을 다시 구현하는가? | Airflow는 검증된 CLI만 호출하고 pipeline이 idempotency를 소유 |

## 8. v0 Boundary

Now / implemented:

```text
synthetic CSV ingest/catalog path with mongomock tests
Slice1 bronze/silver/gold pipeline
dbt-style quality checks
schema drift warning
source_hash idempotency
EAV multi-format mapping
JSON CLI smoke runs
local Spark/Iceberg single-gold-table walking skeleton
local Airflow `dags test` + standalone/LocalExecutor runtime verification
local Airflow two-task JSON lakehouse -> Iceberg publish DAG
local Kafka K1 bounded raw landing with landing-before-offset-commit recovery
Kafka K1.5 deterministic one-date adapter into the existing batch/quality/Iceberg path
Spark machine-event batch (S7): one date's silver/gold re-expressed in Spark with verified Python
  parity, published to the Iceberg gold partition only after the existing quality suite passes
edge/cloud recovery (S8): a bounded synthetic simulation of one disconnected session - immutable
  sealed spool, replay through the existing K1 landing, downstream batch blocked until the sealed
  sequence range is fully recovered
recovery-gated publish (S9): one sealed edge session is published to the local Iceberg gold table
  only after the shared readiness gate and exact session-input equality pass
```

Backlog / design-only:

```text
real Mongo runtime verification
full Spark/Iceberg medallion pipeline
continuous Kafka consumer service / Spark Structured Streaming
multi-partition Kafka rebalance and ordering verification
ROS2/MCAP ingest
column-level lineage
production governance UI
```

Explicitly not claiming:

```text
production manufacturing data platform
full Spark/Iceberg pipeline implemented
production or continuous Kafka streaming pipeline implemented
end-to-end exactly-once delivery
real Mongo runtime verified
production Airflow deployment operated
real company/customer schema usage
```

## 9. Success Criteria

This project is useful as a portfolio slice when:

```text
1. code/test evidence supports every implemented claim
2. verification log records tests and CLI smoke runs
3. README/BENCHMARKS separate implemented, partial, backlog, design-only
4. blog posts explain scenario -> pressure -> decision -> evidence -> limitation
5. resume claims stay within synthetic/test-covered boundaries
```

Service success means:

```text
1. a data user can identify the gold grain before using a metric
2. an operator can trace a suspicious gold number to run/source/quality/lineage evidence
3. warning/failure boundaries are visible instead of hidden in the transform code
4. same-input reruns are safe no-ops
5. a bounded Kafka redelivery/replay can be explained without duplicating the accepted batch result
6. a partially recovered session cannot publish a trusted table snapshot, and the refusal names
   the missing sequences or the extra/missing event ids
```

## 10. Interview / Blog Thesis

```text
I built a synthetic manufacturing-style/tabular mini data platform that turns raw CSV files
into bronze/silver/gold datasets with quality checks, schema drift warnings,
source-hash idempotency, catalog/version metadata, and lineage records.
The goal was not to claim a production manufacturing platform, but to prove the core data-platform
loop: identity, quality, reproducibility, rerun safety, and inspectable evidence.
```
