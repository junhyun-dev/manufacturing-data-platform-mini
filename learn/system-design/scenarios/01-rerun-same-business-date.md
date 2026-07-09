# 01. Scenario Walkthrough — 같은 business_date를 다시 처리한다

상태: 이해용 초안

목적: `02-slice2-question-map.md`의 #1(ACID), #2(write semantics), #10(idempotency)을 실제 시나리오 하나로 풀어서 이해한다.

---

## Scenario

```text
business_date=2026-06-29 데이터를 한 번 처리했다.
silver/gold 결과가 만들어졌다.

나중에 같은 business_date를 다시 처리해야 한다.
```

재처리 이유는 여러 가지일 수 있다.

| 이유 | 의미 |
|---|---|
| 같은 파일을 실수로 다시 넣음 | 중복 처리 방지가 필요하다 |
| 정정 파일이 도착함 | 같은 날짜 결과를 새 입력으로 다시 만들어야 한다 |
| transform 로직을 고침 | 과거 날짜를 새 로직으로 재계산하고 싶다 |
| quality/gold 계산 기준을 고침 | mart를 다시 publish해야 한다 |
| 이전 run이 이상함 | 기존 결과를 교체하거나 비교해야 한다 |

핵심 질문:

```text
같은 business_date를 다시 처리하면 기존 silver/gold는 어떻게 해야 하는가?
```

---

## Options

| Option | 하는 일 | 장점 | 문제 | v0 판단 |
|---|---|---|---|---|
| append | 기존 결과 뒤에 새 결과를 추가 | 단순함 | gold 중복 가능, metric이 두 배가 될 수 있음 | 피함 |
| skip | 같은 입력이면 처리하지 않음 | 실수 재실행 방지 | 정정/로직 변경 재처리가 불편 | 유지 |
| overwrite | 같은 날짜 partition을 새 결과로 교체 | 재처리 허용, 중복 방지 | 어떤 overwrite인지 metadata가 필요 | Core |
| merge/upsert | 늦게 온 row만 반영 | 대규모에서 효율적 | 조건/재집계 범위가 복잡 | Backlog |

---

## Slice1 vs Slice2

### Slice1

```text
same dataset_id + business_date + source_hash + successful run
  -> skip
```

Slice1에서 `source_hash`는 idempotency gate다.

```text
같은 파일이면 새 output을 만들지 않는다.
```

### Slice2

Slice2에서는 재처리를 허용해야 한다.

```text
same file
  -> skip

same business_date + different source_hash
  -> Spark transform 재실행
  -> Iceberg business_date partition overwrite
  -> new snapshot commit
```

Slice2에서 `source_hash`는 여전히 중요하지만 역할이 조금 바뀐다.

| 값 | Slice1 역할 | Slice2 역할 |
|---|---|---|
| `source_hash` | idempotency gate | input identity / lineage / audit |
| `run_id` | run 폴더 식별 | pipeline 실행 식별 |
| `snapshot_id` | 없음 | Iceberg table commit 식별 |
| `business_date` | path/filter 기준 | partition overwrite 기준 |

---

## Proposed v0 Rule

v0는 hybrid idempotency를 쓴다.

```text
Rule A. same source rerun

same dataset_id + business_date + source_hash + successful run
  -> skip
```

```text
Rule B. corrected source rerun

same dataset_id + business_date + different source_hash
  -> run Spark transforms
  -> overwrite business_date partition in Iceberg
  -> record new run_id
  -> record silver_snapshot_id / gold_snapshot_id
```

피하는 것:

```text
append-only gold
MERGE INTO late-row upsert
production rollback semantics
snapshot retention policy
```

---

## State Trace

### First Run

| moment | state | example |
|---|---|---|
| t1 | source arrives | `business_date=2026-06-29`, `source_hash=H1` |
| t2 | run starts | `run_id=R1` |
| t3 | silver write | `silver_snapshot_id=S1` |
| t4 | gold write | `gold_snapshot_id=G1` |
| t5 | metadata persists | `lakehouse_runs(R1, H1, S1, G1, success)` |

### Same File Rerun

| moment | state | example |
|---|---|---|
| t1 | source arrives | `business_date=2026-06-29`, `source_hash=H1` |
| t2 | existing success found | `R1` |
| t3 | result | `skipped`, no new gold rows |

### Corrected File Rerun

| moment | state | example |
|---|---|---|
| t1 | source arrives | `business_date=2026-06-29`, `source_hash=H2` |
| t2 | same date, different source | prior run exists but source changed |
| t3 | run starts | `run_id=R2` |
| t4 | overwrite silver partition | `silver_snapshot_id=S2` |
| t5 | overwrite gold partition | `gold_snapshot_id=G2` |
| t6 | metadata persists | `lakehouse_runs(R2, H2, S2, G2, success)` |

중요:

```text
run_id는 snapshot_id로 대체되지 않는다.
run_id가 table별 snapshot_id를 참조한다.
```

---

## Why Iceberg Helps

Iceberg가 여기서 주는 가치는 "멋진 기능"이 아니라 write semantics다.

| 기능 | 이 시나리오에서의 의미 |
|---|---|
| atomic commit | overwrite가 성공하면 새 snapshot이 current가 되고, 실패하면 이전 snapshot이 유지된다 |
| snapshot | overwrite 전후 결과를 비교할 수 있다 |
| partitioning | `business_date` 단위로 교체/조회할 수 있다 |
| metadata tables | 어떤 snapshot이 언제 만들어졌는지 확인할 수 있다 |

time travel은 core가 아니다.

```text
core:
  같은 날짜 재처리를 중복 없이 원자적으로 처리한다.

demo:
  overwrite 전후 snapshot을 읽어 이전 gold를 재현한다.

backlog:
  production rollback / retention / restore 운영
```

---

## Test Contract

### Test 1. Same file rerun skips

```text
given business_date D, source_hash H1 processed successfully
when the same file is processed again
then the second run is skipped
and gold row count does not increase
```

### Test 2. Corrected file overwrites partition

```text
given business_date D, source_hash H1 processed successfully
and corrected source_hash H2 for the same D
when the corrected file is processed
then the D partition is overwritten
and gold metric reflects H2
and a new gold_snapshot_id is recorded
```

### Test 3. Previous snapshot is readable

```text
given H1 produced gold_snapshot_id G1
and H2 produced gold_snapshot_id G2
when reading VERSION AS OF G1 and VERSION AS OF G2
then the old and corrected gold metrics can be compared
```

---

## Decision Candidate

```text
Decision:
  v0 uses hybrid idempotency.

Same source:
  skip.

Same business_date with changed source:
  overwrite the Iceberg business_date partition.

Metadata:
  keep run_id as pipeline execution id.
  record table snapshot ids in run metadata.

Avoid:
  append-only gold.
  MERGE INTO late-row upsert.
  production rollback semantics.
```

---

## Interview Sentence

> 같은 날짜를 다시 처리할 때 append하면 gold metric이 중복될 수 있기 때문에 피했습니다. 같은 입력이면 Slice1처럼 `source_hash`로 skip하고, 같은 날짜지만 정정 입력이면 Spark로 다시 계산한 뒤 Iceberg의 `business_date` partition을 atomic overwrite합니다. 이때 `run_id`는 파이프라인 실행 단위로 유지하고, Iceberg의 `snapshot_id`는 run metadata에 연결해서 lineage와 time-travel demo에 사용합니다.
