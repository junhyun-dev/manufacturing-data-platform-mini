# Spark Engine-Swap Contract (S7)

ADR Status: Implemented
검토 상태: Codex reviewed / local Spark, Iceberg, Airflow DAG runtime verified

> code/test/local Spark runtime과 Iceberg publish를 검증했고, Codex review의 H1/H2/M1/M2를
> 반영한 뒤 `Implemented`로 승격했다. 최신 테스트 수와 실행 결과는
> `VERIFICATION_LOG.md`가 source of truth다.

## Context

기존 batch spine은 `transform_silver`/`transform_gold`(순수 Python)로 silver/gold를 만든다.
K1.5는 한 `business_date`의 accepted Kafka landing을 canonical CSV + `source_hash`로 바꾼다.
S7의 압력은 하나다.

```text
운영자가 이미 durable하게 landing된 한 날짜를 backfill한다.
연산 표현을 Spark로 옮기더라도 기존 gold grain·합계·재실행 계약은 바뀌면 안 된다.
```

이 Slice는 "Spark로 다시 표현"이지 "새 처리 플랫폼"이 아니다. Structured Streaming, direct
Kafka→Iceberg sink, full medallion Spark rewrite는 이 압력을 풀기 위해 필요하지 않다.

## Decision

```text
input   = K1.5 adapter의 canonical CSV + source_hash (Spark가 raw JSONL을 다시 해석하지 않음)
engine  = Spark DataFrame built-in expressions (Python/pandas UDF 사용 안 함)
parity  = 기존 transform_silver/transform_gold 의미가 기준
quality = 기존 build_quality_checks를 Spark 결과에 적용 (별도 재구현 없음)
publish = quality pass일 때만 overwritePartitions()로 한 gold 테이블에 write
```

### 코드보다 먼저 고정한 계약

| 계약 | S7의 선택 | 깨지면 |
|---|---|---|
| source boundary | Spark는 adapter의 검증된 canonical CSV/`source_hash`만 입력으로 받는다 | provenance 검증을 우회하고 false-green 위험이 생김 |
| grain | gold = `(business_date, plant_id, line_id, product_code)` | 집계 중복·metric 오해 |
| dedup | silver natural key `(work_order_id, machine_id, event_time)`, Kafka coordinate 순서로 first 유지 | 유실을 dedup으로 숨김 |
| rounding | `defect_rate`/`avg_cycle_time_ms`는 `format_number` + comma strip + double cast로 Python `round`와 일치 (`bround`/`round`/decimal-cast는 boundary에서 불일치) | Python↔Spark 결과 불일치 |
| quality gate | Spark 결과가 기존 quality suite를 통과해야 publish | 품질 실패가 trusted current가 됨 |
| write | `overwritePartitions()`로 대상 date partition만 교체 | 다른 날짜 partition 손실 또는 gold 중복 |
| idempotency | 같은 `table+business_date+source_hash` 성공은 skip, 새 snapshot 없음 | retry가 snapshot noise를 만듦 |
| correction | 다른 source의 같은 날짜는 새 snapshot + partition 교체 | 정정이 반영 안 됨 |
| identity | `source_hash`(입력) / `run_id`(실행) / `snapshot_id`(table commit)를 별도 기록 | 세 식별자가 섞임 |

### 왜 dedup을 Kafka coordinate 순서로 하는가

Python 루프는 CSV 파일 순서에서 natural key의 **첫 행**을 유지한다. adapter는 canonical CSV를
`(topic, partition, offset)` 오름차순으로 쓴다. 따라서 Spark에서
`row_number().over(partitionBy(natural_key).orderBy(coordinate))==1`은 Python이 유지하는 바로
그 행과 일치한다. 이 순서를 고정해야 engine parity가 결정적이다.

### 왜 quality를 재구현하지 않는가

Spark용 quality를 새로 쓰면 batch spine의 quality와 조용히 달라질 수 있다. S7은 Spark가
materialize한 silver/gold와 입력 행을 driver로 collect해 기존 `build_quality_checks`에 그대로
넣는다. 즉 "Spark quality"는 spine의 quality suite를 Spark 결과에 적용한 것이며,
reconciliation(입력→silver, silver→gold 보존)까지 동일하다. 이는 **distributed Spark-native
quality 평가가 아니다** (driver collect 후 Python suite 적용). local bounded scope에서만 허용한다.

quality fail은 Iceberg publish를 막을 뿐 아니라 CLI가 non-zero exit하여 orchestration task도
실패시킨다 (BashOperator가 성공으로 오인하지 않도록).

## Failure states

| Failure point | Result | Recovery |
|---|---|---|
| 요청 date와 canonical CSV의 다른 date 혼입 | Spark/publish 전에 실패 | 올바른 adapter 출력을 사용 |
| Spark quality fail | Iceberg write·success state 없음, fail evidence만 기록, CLI non-zero exit | 입력/landing을 조사 (adapter가 정상 입력에서 앞서 막음) |
| table commit 성공 후 evidence write 실패 | two-system 불일치 가능 | **미해결**, limitation으로 남김 (아래 경계) |
| 같은 source 재실행 (snapshot이 table history에 존재) | skip, 새 snapshot 없음 | 정상 |
| state는 있으나 snapshot이 table history에 없음 (warehouse 비움/재생성 등) | write로 복구, non-empty 결과 | recorded snapshot의 history 존재를 확인해 stale skip 방지 |
| 다른 source 같은 날짜 | 새 snapshot, partition 교체 | 정상 |

## Boundaries

- **Rounding 경계**: Spark `bround`/`round`/decimal-cast는 valid boundary double에서 Python
  `round`와 갈린다 (예: `32107/40`은 `802.6749…`로 저장되어 Python `802.67` vs `bround 802.68`;
  bounded audit에서 scale 2 기준 40,400 정수비 중 204건 불일치). `format_number(value, scale)` 후
  grouping comma 제거·double cast한 built-in 표현은 같은 audit에서 mismatch 0건이라 이 값을 쓴다.
  이는 이 gold metric과 같은 정수비 표본 40,400건과 별도 경계 test에서 확인한 결과다. 모든
  정수비나 double에 대한 보편적 float 동치, 성능/정확도 우월성 주장은 아니다. UDF는 쓰지 않는다.
- **Shuffle 경계**: gold `groupBy`의 executed plan과 `Exchange` 관찰을 evidence에 남긴다. 이는
  local execution-plan 학습 evidence이며 성능·throughput·대규모 처리 claim이 아니다.
- **Two-system atomicity 경계**: Iceberg commit과 success-state JSON write는 하나의 transaction이
  아니다. commit 후 evidence write가 실패하는 경우의 복구는 이 Slice에서 해결하지 않는다.
- **Concurrency 경계**: single writer 가정. concurrent Iceberg writer / branch WAP / MERGE /
  compaction은 범위 밖이다.
- **Snapshot retention 경계**: same-source skip은 state가 기록한 snapshot이 table history에 남아
  있을 때만 적용한다. snapshot expiry/GC로 history에서 사라지면 데이터 정확성을 위해 같은
  partition을 다시 write해 불필요한 snapshot 하나가 생길 수 있다. 이 local slice는 snapshot
  expiry를 실행하지 않는다.
- **Filesystem 경계**: local hadoop catalog + local warehouse에서만 검증했다. cluster/분산/HA
  Spark나 production 운영은 주장하지 않는다.

## Alternatives

| Option | Why not S7 |
|---|---|
| Spark Structured Streaming | 아직 없는 window/latency 운영 surface를 끌어온다. |
| direct Kafka -> Iceberg sink | adapter의 provenance·quality gate를 우회한다. |
| full bronze/silver/gold Iceberg 테이블 | full medallion rewrite로 scope가 커진다. |
| Spark용 quality를 새로 작성 | spine quality와 divergence 위험. |
| Python UDF로 transform | built-in expression parity/plan 관찰을 잃는다. |

## Claim boundary

말할 수 있는 것:

```text
provenance-checked Kafka landing adapter를 재사용하는 local bounded Spark batch를 구현했다.
기존 gold grain과 reconciliation 계약을 유지한 채 Python↔Spark engine parity를 검증했다.
quality-passed correction만 한 Iceberg gold 테이블에 publish했다.
same-source no-op, changed-source partition 교체, other-date 보존, shuffle-plan evidence,
얇은 local Airflow wrapper를 검증했다.
```

말할 수 없는 것:

```text
production/cluster Spark 경험
대규모 성능·throughput 향상
full Spark/Iceberg medallion pipeline
continuous Kafka/Spark streaming, end-to-end exactly-once
concurrent writer correctness / distributed transaction
distributed Spark-native quality 평가 (driver collect 후 Python suite 적용)
production Airflow 운영
```

## Evidence

- `src/manufacturing_data_platform/pipeline/spark_machine_event_batch.py`
- `tests/test_spark_machine_event_batch.py`
- `dags/manufacturing_spark_machine_event_batch.py`
- `scripts/verify_spark_machine_event_batch.sh`, `scripts/spark_machine_event_batch_verification.py`
- `VERIFICATION_LOG.md`
