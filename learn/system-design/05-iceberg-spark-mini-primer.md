# 05. Iceberg/Spark mini primer — business_date 재처리 시나리오에 필요한 만큼만

상태: 학습/실습 브리지 초안
프로젝트: `manufacturing-data-platform-mini`

> **STATUS: design-only / learning bridge.** 이 repo에는 아직 Spark/Iceberg 구현 코드가 없고, `pyspark`도 설치되어 있지 않다. 이 문서는 walking skeleton 전 단계의 primer이며 구현 evidence가 아니다.

목적:

```text
Iceberg/Spark 전체를 공부하는 문서가 아니다.
Slice2에서 필요한 개념만 business_date 재처리 시나리오에 연결한다.
읽고 끝내지 않고, walking skeleton으로 바로 확인한다.
```

관련 문서:

- [`02-slice2-question-map.md`](02-slice2-question-map.md)
- [`04-slice2-spark-iceberg-shift.md`](04-slice2-spark-iceberg-shift.md)
- [`../reference-decisions/iceberg-write-semantics.md`](../reference-decisions/iceberg-write-semantics.md)
- [`scenarios/01-rerun-same-business-date.md`](scenarios/01-rerun-same-business-date.md)

## 1. 이 문서가 답하려는 질문

Slice1은 같은 입력 재실행을 이렇게 다룬다.

```text
dataset_id + business_date + source_hash 가 이미 성공했으면 skip
```

Slice2에서 Spark/Iceberg로 옮기면 질문이 바뀐다.

```text
같은 business_date를 다시 처리할 때:
  append 하면 중복되지 않나?
  overwrite 하면 다른 날짜까지 지워지지 않나?
  partition overwrite는 정확히 무엇을 바꾸나?
  overwrite 전 결과는 어디에 남나?
  run_id와 snapshot_id는 어떻게 연결되나?
```

이 질문을 이해하려면 아래 단어를 실제로 다뤄봐야 한다.

```text
SparkSession
DataFrame
Iceberg catalog / warehouse
Iceberg table
snapshot_id
current snapshot
atomic commit
partition overwrite
time travel
```

## 2. 공식 문서 최소 범위

처음부터 끝까지 읽지 않는다. 지금 필요한 부분만 본다.

| topic | 공식 문서 | 지금 볼 이유 |
|---|---|---|
| Iceberg 큰 그림 | <https://iceberg.apache.org/> | Iceberg가 analytic table format이고 schema evolution, hidden partitioning, time travel을 제공한다는 큰 그림 |
| Spark 시작 | <https://iceberg.apache.org/docs/latest/spark-getting-started/> | Spark catalog 설정, table create/read/write 시작점 |
| Spark writes | <https://iceberg.apache.org/docs/latest/spark-writes/> | append / overwrite / partition overwrite / write semantics |
| Reliability | <https://iceberg.apache.org/docs/latest/reliability/> | snapshot, current snapshot, atomic commit을 business_date 재처리에 연결 |
| Partitioning | <https://iceberg.apache.org/docs/latest/partitioning/> | `business_date` partition과 hidden partitioning 이해 |
| Spark SQL/DataFrames | <https://spark.apache.org/docs/latest/sql-programming-guide.html> | DataFrame/SQL/local mode/groupBy 기본 |

읽을 때 질문:

```text
이 기능이 우리 프로젝트의 어떤 pressure를 해결하나?
이 기능은 Core인가 Demo인가 Backlog인가?
이 기능을 README/이력서에서 어떻게 정직하게 말할 수 있나?
```

## 3. 개념을 프로젝트 말로 번역

| 공식 용어 | 우리 프로젝트에서의 의미 | claim boundary |
|---|---|---|
| SparkSession | local mode에서 Slice1 transform을 DataFrame으로 실행하는 진입점 | cluster 운영 아님 |
| DataFrame | CSV row list 대신 쓰는 transform 입력/출력 | 엔진 교체, 비즈니스 contract는 유지 |
| Iceberg catalog | local warehouse의 table namespace | production catalog 운영 아님 |
| Iceberg table | `silver_events`, `gold_daily_metrics` 같은 medallion table | CSV 폴더 대신 table metadata를 가짐 |
| snapshot | table commit 결과 | pipeline run을 대체하지 않음 |
| current snapshot | 현재 reader가 보는 table 상태 | run metadata가 참조할 수 있음 |
| atomic commit | write 성공/실패가 table 상태에 원자적으로 반영 | concurrent production guarantee 과장 금지 |
| partition overwrite | 해당 `business_date` partition만 교체 | 같은 날짜 정정 재처리의 core pressure |
| time travel | 이전 snapshot을 읽어 재처리 전후 비교 | demo/supporting evidence, production rollback 주장 금지 |

핵심 구분:

```text
run_id      = 우리 파이프라인 실행 1회
snapshot_id = Iceberg table commit 1회

run_id가 snapshot_id로 대체되는 것이 아니다.
run_id가 silver/gold snapshot_id를 참조한다.
```

## 4. 최소 실습 walking skeleton

목표:

```text
gold_daily_metrics table 하나로
business_date partition overwrite와 snapshot 비교를 직접 확인한다.
```

순서:

```text
1. local SparkSession 띄우기
2. Iceberg catalog / warehouse 설정
3. gold_daily_metrics table 생성
4. business_date=2026-06-29 row insert
5. 현재 snapshot_id 확인
6. 같은 business_date를 정정 데이터로 overwrite
7. 새 snapshot_id 확인
8. snapshots/history metadata 확인
9. 이전 snapshot과 최신 snapshot 비교
10. 결과를 run metadata 모양으로 기록
```

최소 table:

```text
gold_daily_metrics
  business_date
  plant_id
  line_id
  product_id
  units_produced
  defect_count
  defect_rate
  avg_cycle_time_ms
```

검증해야 할 것:

```text
첫 write 후 snapshot S1이 생긴다.
같은 business_date overwrite 후 snapshot S2가 생긴다.
S2 != S1.
최신 table에는 정정 후 row만 있다. 중복 append가 없다.
이전 snapshot S1을 읽으면 정정 전 row를 볼 수 있다.
다른 business_date partition은 바뀌지 않는다. (두 번째 날짜를 넣는 확장 test)
```

## 5. 이 실습이 현재 프로젝트에 붙는 지점

Slice1 현재 상태:

```text
source CSV
-> bronze CSV
-> silver CSV
-> gold CSV
-> quality_report.json
-> lakehouse_runs / lineage_events
```

Slice2 walking skeleton:

```text
source CSV
-> Spark DataFrame
-> gold_daily_metrics Iceberg table
-> snapshots/history 확인
-> run metadata에 gold_snapshot_id 기록
```

아직 하지 않는 것:

```text
bronze/silver/gold 전체 이식
full quality suite Spark 이식
Mongo runtime 검증
Airflow runtime 검증
Spark cluster 운영
Kafka streaming
production rollback/retention 운영
```

즉 walking skeleton의 claim은 작게 잡는다.

```text
business_date 재처리 시나리오에서
Iceberg partition overwrite와 snapshot 비교를 로컬에서 확인했다.
```

## 6. 블로그 연결

이 문서는 B1 이후의 블로그로 좋다.

블로그 후보:

```text
B1: source_hash로 같은 입력 재처리를 안전하게 skip하기
B5: skip에서 partition overwrite로 — business_date 재처리를 Iceberg로 다시 표현하기
```

B5 구조:

```text
1. Slice1의 문제: 같은 business_date 재처리
2. 현재 해법: source_hash가 같으면 skip
3. 남은 문제: 정정 파일은 skip하면 안 됨
4. Iceberg 관점: 해당 business_date partition만 atomic overwrite
5. snapshot 관점: overwrite 전후 결과 비교
6. run metadata 관점: run_id가 snapshot_id를 참조
7. 한계: local walking skeleton, production rollback/cluster/concurrency 아님
```

이력서에는 B5가 walking skeleton을 통과하기 전까지 이렇게만 쓴다.

```text
Designed the Spark/Iceberg translation path for a synthetic medallion pipeline,
including business_date partition overwrite, snapshot lineage, and claim boundaries.
```

walking skeleton 통과 후에는 이렇게 올릴 수 있다.

```text
Built a local Spark/Iceberg walking skeleton that rewrites a business_date partition
and records snapshot metadata for reproducible rerun comparison.
```

## 7. Claude에게 맡길 보완 질문

Claude는 여기서 research lead로 쓴다.

```text
너는 external benchmark research lead + auditor + supplementer다.

이 mini primer를 Apache Iceberg/Spark 공식 문서와 lakehouse/JD 관점에서 보완해라.
구현 범위를 늘리지 말고, 다음을 분리해라.

1. Core로 직접 실습해야 하는 것
2. Demo로 보여주면 충분한 것
3. Backlog로 둬야 하는 production 운영 항목
4. 지금 claim하면 과장인 표현
5. 블로그/이력서에 더 좋은 표현

특히 business_date 재처리, partition overwrite, snapshot/time travel,
run_id vs snapshot_id 구분을 중점적으로 봐라.
```

## 8. 다음 액션

```text
1. 이 primer를 읽고 모르는 단어를 live-study-notes.md에 질문으로 남긴다.
2. Claude에게 공식 문서/시장 관점 보완을 요청한다.
3. 보완 결과를 Core/Demo/Backlog로 분류한다.
4. local Spark/Iceberg walking skeleton을 별도 branch에서 만든다.
5. 성공하면 VERIFICATION_LOG.md와 Portfolio Ledger에 B5 evidence로 연결한다.
```
