# 01. Spark/Iceberg Partition Overwrite Slice

상태: implemented local walking skeleton / thin slice map

> 이 문서는 Spark/Iceberg 관련 설계 흐름을 한눈에 보기 위한 index다.  
> 최신 테스트 수와 실행 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)가 source of truth다.

## 1. Slice Thesis

```text
같은 business_date에 정정 source가 들어왔을 때,
gold 결과를 중복 없이 교체하고,
run_id와 Iceberg snapshot_id를 연결한 evidence를 남긴다.
```

이 slice는 "Spark/Iceberg 전체 pipeline 구현"이 아니다.

```text
single gold table
local SparkSession
business_date partition overwrite
snapshot metadata evidence
```

## 2. Primary Scenario

시나리오:

```text
business_date=2026-06-29의 gold 결과가 이미 있다.
나중에 같은 business_date의 정정 source가 들어온다.
운영자는 같은 날짜 row가 append 중복되지 않고 새 값으로 교체되길 원한다.
다른 business_date partition은 그대로 유지되어야 한다.
```

관련 scenario / 배경 문서:

- [`../scenarios/01-rerun-same-business-date.md`](../scenarios/01-rerun-same-business-date.md)
- [`../02-slice2-question-map.md`](../02-slice2-question-map.md)
- [`../04-slice2-spark-iceberg-shift.md`](../04-slice2-spark-iceberg-shift.md)
- [`../05-iceberg-spark-mini-primer.md`](../05-iceberg-spark-mini-primer.md)

## 3. Question Areas Pulled

관련 question-bank 영역:

- [`../question-bank/02-quality-rerun-failure.ko.md`](../question-bank/02-quality-rerun-failure.ko.md)
  - rerun / correction / failure state
- [`../question-bank/03-storage-spark-consistency.ko.md`](../question-bank/03-storage-spark-consistency.ko.md)
  - storage / table format / Spark / atomicity
- [`../question-bank/06-cross-area-connection-questions.ko.md`](../question-bank/06-cross-area-connection-questions.ko.md)
  - correction x lineage
  - catalog x table commit
  - quality x current state
- [`../question-bank/05-security-performance-testing-claim.ko.md`](../question-bank/05-security-performance-testing-claim.ko.md)
  - testing / claim boundary

### Core Questions

| Core question | Why Core |
|---|---|
| 같은 `source_hash` rerun은 skip인가? | retry/idempotency contract가 바뀐다. |
| 다른 `source_hash` + 같은 `business_date`는 어떻게 correction 처리하는가? | write semantics가 바뀐다. |
| append / whole-table overwrite / partition overwrite / merge 중 무엇인가? | table write contract가 바뀐다. |
| partition key는 `business_date`인가? | overwrite 범위와 test가 바뀐다. |
| D partition만 교체되고 D2 partition은 유지되는가? | partition overwrite가 실제로 맞는지 증명한다. |
| overwrite 후 snapshot evidence를 어떻게 읽는가? | Iceberg claim의 evidence가 된다. |
| `run_id`와 `snapshot_id`는 어떻게 구분하는가? | pipeline 실행 id와 table commit id를 혼동하지 않는다. |
| local Spark/Iceberg runtime이 실제로 뜨는가? | 구현 가능성을 결정하는 Unknown이었다. |
| 어디까지 public claim할 수 있는가? | walking skeleton과 production lakehouse를 구분한다. |

### Demo Questions

| Demo question | Why not Core |
|---|---|
| 이전 snapshot을 time travel read로 직접 비교할 것인가? | snapshot metadata evidence만으로 이번 core contract는 닫힌다. |
| Spark explain/Spark UI로 shuffle을 보여줄 것인가? | 현재 slice는 write semantics proof이지 performance proof가 아니다. |

### Backlog Questions

| Backlog question | Reason |
|---|---|
| bronze/silver/gold 전체를 Spark로 rewrite할 것인가? | scope가 full port로 커진다. |
| quality checks를 Spark agg/filter로 옮길 것인가? | 별도 quality-on-Spark slice가 필요하다. |
| MERGE/upsert를 구현할 것인가? | row-level late data problem은 이번 correction scenario 밖이다. |
| concurrent writer conflict를 검증할 것인가? | production table operation 영역이다. |
| Airflow가 Spark/Iceberg run을 trigger할 것인가? | Airflow runtime + Spark dependency 문제가 합쳐진 별도 slice다. |
| retention/expire/rollback을 구현할 것인가? | production lakehouse 운영 claim으로 커진다. |

### Unknowns Closed By This Slice

| Unknown | Closed by |
|---|---|
| PySpark/Iceberg runtime jar/catalog 조합이 local에서 뜨는가? | [`../07-spark-iceberg-version-pin.md`](../07-spark-iceberg-version-pin.md), tests |
| `.snapshots` metadata를 local Spark에서 읽을 수 있는가? | Spark/Iceberg skeleton tests |
| `overwritePartitions()`가 D2 partition을 보존하는가? | Spark/Iceberg skeleton tests |

## 4. Decisions

세부 decision 문서:

- [`../../reference-decisions/iceberg-write-semantics.md`](../../reference-decisions/iceberg-write-semantics.md)
- [`../06-spark-iceberg-walking-skeleton-plan.md`](../06-spark-iceberg-walking-skeleton-plan.md)
- [`../07-spark-iceberg-version-pin.md`](../07-spark-iceberg-version-pin.md)

핵심 결정만 요약:

```text
Use DataFrameWriterV2.overwritePartitions()
Use one local Iceberg gold table
Use business_date as the partition boundary
Record run_id -> snapshot_id evidence
Keep full Spark medallion rewrite as Backlog
```

## 5. Evidence

Code / test:

- [`../../../requirements-spark.txt`](../../../requirements-spark.txt)
- [`../../../src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py`](../../../src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py)
- [`../../../tests/test_spark_iceberg_skeleton.py`](../../../tests/test_spark_iceberg_skeleton.py)

Verification:

- [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)
  - `2026-07-11 — Spark/Iceberg single-gold-table walking skeleton`

## 6. Claim Boundary

Allowed:

```text
local Spark/Iceberg single-gold-table walking skeleton
business_date partition overwrite
snapshot metadata evidence
run_id -> snapshot_id mapping
same source_hash rerun creates no new snapshot
```

Forbidden:

```text
full Spark/Iceberg medallion pipeline
production lakehouse
Iceberg rollback system
concurrent writer handling
Airflow-triggered Spark runtime
Spark-based quality suite
performance/scale claim
```

## 7. Next Questions

```text
Should the pipeline record code/logic version identity in each run?
Should Airflow runtime trigger be verified next?
Should failure-state forensics be added for failed correction runs?
Should Spark quality checks be designed as a separate slice?
```
