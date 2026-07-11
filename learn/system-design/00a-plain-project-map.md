# 00a. Plain project map — manufacturing-data-platform-mini를 먼저 쉽게 보기

상태: 입문용 지도
목적: Spark/Iceberg 전에, 이 프로젝트가 무엇을 하려는지 파일/상태/질문으로 이해한다.

## 1. 한 문장

이 프로젝트는 manufacturing-style/tabular 작업 데이터를 받아서, 나중에 다시 설명할 수 있도록 **입력, 처리 결과, 품질검사, lineage, version metadata**를 남기는 작은 데이터 플랫폼이다.

더 쉽게 말하면:

```text
파일 하나가 들어온다.
그 파일에서 지표를 만든다.
그 지표가 믿을 만한지 검사한다.
나중에 "이 숫자가 어디서 왔지?"를 설명할 기록을 남긴다.
같은 파일을 다시 돌렸을 때 중복되지 않게 한다.
```

## 2. 왜 그냥 CSV 처리 스크립트가 아닌가

단순 스크립트:

```text
CSV 읽기 -> 합계 계산 -> 결과 CSV 저장
```

운영에서 바로 질문이 생긴다.

```text
이 결과는 어느 원본 파일에서 왔나?
같은 파일을 다시 돌리면 중복되나?
원본 schema가 바뀌었나?
row가 처리 중 사라지지 않았나?
어느 날짜 기준 집계인가?
품질검사는 통과했나?
이전 run과 지금 run이 다른 이유는 뭔가?
```

이 프로젝트는 그 질문에 답하려고 만든 mini platform이다.

## 3. Actor / Responsibility Map

| Actor | Owns | Creates | Verifies | Must not do |
|---|---|---|---|---|
| Source file | raw manufacturing-style rows | CSV input | nothing | 회사/고객 실제 데이터 포함 금지 |
| Pipeline | transform rules | bronze/silver/gold outputs | row counts, quality checks | business logic을 DAG 안에 숨기기 |
| Catalog | dataset/run metadata | run record, version manifest | source/schema/run identity | 실제 처리 결과를 과장해서 claim하기 |
| Quality checks | trust boundary | check report | null/unique/range/conservation/freshness | 단순 row count만 품질이라고 주장하기 |
| Operator/interviewer | asks "why?" | debugging question | lineage/evidence | code 없이 claim만 믿기 |

## 4. 현재 Slice1 흐름

```text
data/raw/manufacturing_events.csv
-> bronze
   원본에 가까운 복사본 + source_hash/schema_hash/row_count manifest
-> silver
   business_date 필터
   natural key dedup
   타입 변환
-> gold
   일별 line/product metric
   units, defects, defect_rate, avg_cycle_time
-> quality
   row_count reconciliation
   unit conservation
   not_null / unique / accepted_values / range / freshness
   schema_drift warning
-> catalog/lineage
   run_id
   source_hash
   schema_hash
   input/output path
   quality result
```

한 줄로:

```text
raw file -> trusted metric -> why/trust/retry evidence
```

## 5. EAV mini slice는 왜 추가됐나

실무 데이터는 항상 같은 컬럼명으로 오지 않는다.

예:

```text
plant_a.csv:
  설비ID, 생산수량, 불량수

plant_b.csv:
  equipment_id, units, defects

line_c.csv:
  machine, output_count, temp_f, pressure_bar
```

그냥 컬럼명을 하드코딩하면 source가 늘 때마다 코드가 늘어난다.

그래서 EAV mini slice는 이렇게 한다.

```text
wide CSV 여러 개
-> mapping config(JSON)
-> EAV long format
-> gold metric mart
-> quality checks
```

핵심 claim:

```text
새 형식이 오면 pipeline code를 바꾸는 게 아니라 mapping config를 추가한다.
```

## 6. 중요한 단어를 쉬운 말로

| 단어 | 쉬운 뜻 | 이 프로젝트에서 |
|---|---|---|
| dataset | 관리 대상 데이터의 이름 | `manufacturing_daily_metrics` |
| dataset version | 특정 입력/실행으로 생긴 버전 | source_hash/schema_hash/run_id |
| source_hash | 원본 파일 내용의 지문 | 같은 파일 재실행인지 판단 |
| schema_hash | 컬럼 구조의 지문 | 새 컬럼/삭제 컬럼 감지 |
| business_date | 지표를 어느 날짜에 귀속할지 | partition/retry/backfill 기준 |
| bronze | 원본에 가까운 상태 | raw copy + manifest |
| silver | 정리/정규화된 상태 | typed, deduped rows |
| gold | 분석/보고용 지표 | daily metrics |
| quality check | 결과를 믿기 위한 검사 | not_null, unique, conservation |
| lineage | 무엇이 무엇에서 왔는지 | bronze -> silver -> gold parent links |
| idempotency | 다시 돌려도 중복 안 생김 | same source_hash이면 skip |

## 7. 지금 구현된 것과 아닌 것

구현/검증됨:

```text
CSV ingest/catalog path는 mongomock test-covered
Slice1 medallion pipeline
quality checks
schema drift warning
source_hash idempotency
EAV multi-format mapping
JSON CLI smoke run
local Spark/Iceberg single-gold-table walking skeleton
```

아직 아님:

```text
real Mongo runtime verification
real Airflow runtime trigger
full Spark/Iceberg medallion pipeline
Kafka streaming
ROS2/MCAP ingest
production lakehouse
```

따라서 지금 이력서/블로그 claim은:

```text
"작은 synthetic data platform slice를 구현하고 검증했다"
```

이지,

```text
"production manufacturing data platform을 만들었다"
```

가 아니다.

machine/session-specific slice가 추가되기 전까지 외부 claim은 아래처럼 낮춘다.

```text
"manufacturing-style/tabular mini data platform"
```

## 8. Spark/Iceberg는 왜 나중인가

Spark/Iceberg는 지금 프로젝트의 다음 표현 방식이다.

현재:

```text
CSV files + JSON/Mongo catalog
```

나중:

```text
Spark DataFrame + Iceberg tables/snapshots
```

하지만 질문은 같다.

```text
같은 날짜를 다시 돌리면?
schema가 바뀌면?
품질검사는 어디서 막나?
어떤 run이 어떤 output을 만들었나?
```

그래서 먼저 이 프로젝트의 기본 질문이 몸에 붙어야 한다. 그 다음 Spark/Iceberg를 보면:

```text
아, source_hash skip이 Iceberg에서는 partition overwrite/snapshot 문제로 바뀌는구나.
```

라고 연결된다.

## 9. 지금 공부 순서

```text
1. 이 plain map을 읽는다.
2. README의 Phase 1 / Phase 2만 다시 읽는다.
3. VERIFICATION_LOG의 2026-07-08 결과만 본다.
4. `source_hash`, `business_date`, `quality`, `lineage` 네 단어만 설명해본다.
5. 그 다음 `slices/spark-iceberg-partition-overwrite/02-state-shift.md`를 읽는다.
6. Spark/Iceberg 공식 문서는 그 다음이다.
```

지금은 Iceberg 단어를 외울 단계가 아니다.

먼저 이 문장을 말할 수 있으면 된다.

> 이 프로젝트는 manufacturing-style synthetic CSV를 받아서 bronze/silver/gold 지표를 만들고, 그 결과가 어느 입력에서 왔는지, 품질검사를 통과했는지, 같은 입력을 다시 돌렸을 때 중복되지 않는지를 기록하는 작은 데이터 플랫폼입니다.
