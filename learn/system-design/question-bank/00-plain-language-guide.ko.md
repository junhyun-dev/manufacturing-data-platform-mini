# 00. 쉬운 말 질문 가이드

상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

이 문서는 question bank를 처음 읽을 때 보는 쉬운 말 버전이다.

목적:

```text
어려운 설계 용어를 외우는 것 X
질문이 실제로 무슨 걱정을 하는지 이해하는 것 O
```

## 1. 전체를 한 문장으로 말하면

데이터 플랫폼 설계 질문은 대부분 아래 질문으로 돌아간다.

```text
이 데이터가 어디서 왔고,
믿을 수 있고,
다시 돌려도 안전하고,
문제가 생겼을 때 설명할 수 있는가?
```

Spark, Iceberg, Airflow 같은 도구도 결국 이 질문에 답하기 위한 수단이다.

## 2. 어려운 말 -> 쉬운 말

| 어려운 말 | 쉬운 말 |
|---|---|
| service workflow | 누가 언제 이걸 쓰는가? |
| data grain | row 하나가 정확히 무엇을 뜻하는가? |
| identity | 같은 것인지 다른 것인지 구분하는 이름표 |
| source_hash | 이 입력 파일 내용이 같은지 보는 지문 |
| schema_hash | 컬럼 구조가 같은지 보는 지문 |
| run_id | 이번 실행의 이름표 |
| snapshot_id | table이 저장한 특정 순간의 이름표 |
| source contract | 입력 파일이 꼭 지켜야 하는 약속 |
| schema evolution | 컬럼 구조가 바뀌었을 때 시스템이 따라가는 방식 |
| quality check | 데이터가 말이 되는지 보는 검사 |
| reconciliation | 앞 단계와 뒤 단계 숫자가 맞는지 대조 |
| idempotency | 같은 일을 다시 해도 결과가 망가지지 않는 성질 |
| backfill | 과거 날짜 데이터를 나중에 채우는 것 |
| correction | 같은 날짜의 잘못된 데이터를 정정하는 것 |
| current state | 사람들이 지금 읽어야 하는 최신 정답 상태 |
| partition | 데이터를 나누는 기준. 보통 날짜 같은 것 |
| partition overwrite | 특정 칸만 갈아끼우는 것 |
| atomicity | 쓰다가 실패해도 반쪽짜리 결과가 안 남는 것 |
| concurrency | 동시에 여러 실행이 같은 데이터를 건드리는 상황 |
| orchestration | 언제 어떤 순서로 실행할지 관리하는 것 |
| observability | 문제가 생겼을 때 상태를 들여다볼 수 있는 것 |
| lineage | 이 결과가 어떤 입력/중간 결과에서 왔는지 추적하는 것 |
| governance | 누가 보고, 얼마나 보관하고, 무엇을 공개하면 안 되는지 정하는 것 |
| claim boundary | 내가 어디까지 했다고 말해도 되는지 선 긋기 |

## 2.1 지금은 신경 안 써도 되는 질문

질문 은행을 보면 모든 걸 다 해야 할 것처럼 느껴질 수 있다.

그렇게 읽으면 안 된다.

```text
좋은 질문을 많이 뽑는다
-> 이번 slice에서 할 것과 안 할 것을 나눈다
-> 안 할 것은 Backlog로 이름만 붙인다
```

예:

```text
concurrent writer:
  production table 운영에서는 중요하다.
  local single-writer skeleton에서는 지금 구현하지 않는다.

PII/RBAC:
  real user data를 다루면 중요하다.
  synthetic portfolio repo에서는 질문만 남기고 구현하지 않는다.

streaming/CDC:
  Kafka/Flink 프로젝트에서는 중요하다.
  현재 batch manufacturing mini project에서는 named Backlog다.

performance benchmark:
  scale claim을 하려면 필요하다.
  현재는 correctness와 local feasibility만 claim한다.
```

좋은 설계는 모든 질문을 구현하는 것이 아니라, 어떤 질문을 이번에 안 할지 분명히 아는 것이다.

## 3. 영역별 질문을 아주 쉽게 풀면

### 3.1 Service / User Workflow

어려운 질문:

```text
service workflow는 무엇인가?
```

쉬운 말:

```text
누가, 언제, 왜 이 시스템을 보나?
```

예:

```text
분석가:
  "이 defect_rate 숫자 이상한데요?"

운영자:
  "그 숫자가 어떤 source에서 왔는지 먼저 보자."

리뷰어:
  "이 사람이 실제로 뭘 구현했고, 뭘 안 했는지 보자."
```

이 질문이 중요한 이유:

```text
사용자가 누구인지 모르면 필요한 evidence가 달라진다.
분석가에게는 metric grain이 중요하고,
운영자에게는 run/source/quality/lineage가 중요하고,
리뷰어에게는 test와 claim boundary가 중요하다.
```

### 3.2 Data Grain

어려운 질문:

```text
gold row grain은 무엇인가?
```

쉬운 말:

```text
gold table의 한 줄이 무엇을 의미하나?
```

예:

```text
business_date=2026-06-29
plant_id=plant-a
line_id=line-1
product_code=gearbox-a

=> 2026-06-29에 plant-a의 line-1에서 gearbox-a를 만든 하루 요약 한 줄
```

이 질문이 중요한 이유:

```text
grain을 모르면 defect_rate가 무엇의 defect_rate인지 설명할 수 없다.
```

### 3.3 Identity

어려운 질문:

```text
source/run/snapshot identity는 무엇인가?
```

쉬운 말:

```text
같은 것과 다른 것을 어떻게 구분하나?
```

예:

```text
source_hash:
  입력 파일 내용이 같은지 보는 값

run_id:
  파이프라인 실행 한 번의 이름

snapshot_id:
  Iceberg table이 commit한 특정 순간의 이름
```

이 질문이 중요한 이유:

```text
"같은 입력을 다시 돌린 것"과
"같은 날짜에 정정 입력이 온 것"을 구분해야 한다.
```

### 3.4 Source Contract

어려운 질문:

```text
source contract는 무엇인가?
```

쉬운 말:

```text
입력 파일이 꼭 갖춰야 하는 약속은 무엇인가?
```

예:

```text
business_date 컬럼이 있어야 한다.
units_produced는 숫자여야 한다.
defect_count는 units_produced보다 클 수 없다.
```

이 질문이 중요한 이유:

```text
입력이 아무렇게나 바뀌는데도 gold를 만들면,
겉으로는 성공했지만 의미가 틀린 결과가 나올 수 있다.
```

### 3.5 Schema Evolution

어려운 질문:

```text
schema evolution을 지원할 것인가?
```

쉬운 말:

```text
컬럼이 바뀌었을 때 시스템이 어떻게 반응해야 하나?
```

선택지:

```text
무시:
  새 컬럼을 그냥 안 본다.

warn:
  바뀐 건 알려주지만 run은 계속한다.

fail:
  바뀌면 멈춘다.

evolve:
  table schema 자체를 바꿔서 새 컬럼을 받아들인다.
```

현재 프로젝트:

```text
schema_hash로 바뀐 걸 감지하고 warn한다.
Iceberg add column은 아직 구현하지 않았다.
```

### 3.6 Quality Check

어려운 질문:

```text
quality suite는 무엇을 보장하는가?
```

쉬운 말:

```text
데이터가 최소한 말이 되는지 어떤 검사를 했나?
```

예:

```text
필수 값이 비어 있지 않은가?
중복 key가 없는가?
operation 값이 허용된 값인가?
defect_count가 units_produced보다 크지 않은가?
silver 합계와 gold 합계가 맞는가?
```

주의:

```text
quality check 통과 = 모든 데이터가 진짜로 완벽하다는 뜻은 아니다.
정의한 검사들을 통과했다는 뜻이다.
```

### 3.7 Reconciliation

어려운 질문:

```text
source -> silver reconciliation은 무엇인가?
```

쉬운 말:

```text
앞 단계에서 뒤 단계로 넘어갈 때 row나 숫자가 의도치 않게 사라지지 않았나?
```

예:

```text
source에는 5줄이 있다.
그중 business_date=2026-06-29는 4줄이다.
중복 1줄을 제거하면 silver는 3줄이어야 한다.

silver가 3줄이면 정상.
silver가 2줄이면 뭔가 사라진 것.
```

이 질문이 중요한 이유:

```text
단순히 source 5줄 vs silver 3줄만 보면 "2줄이 사라졌다"고 오해할 수 있다.
날짜 filtering과 dedup을 구분해야 한다.
```

### 3.8 Idempotency

어려운 질문:

```text
idempotency를 어떻게 보장하나?
```

쉬운 말:

```text
같은 걸 두 번 실행해도 결과가 두 배로 늘어나지 않게 어떻게 막나?
```

예:

```text
같은 source_hash + 같은 business_date가 이미 성공했다.
그러면 새 run을 만들지 않고 기존 run을 재사용한다.
```

이 질문이 중요한 이유:

```text
Airflow retry, 수동 재실행, backfill에서 같은 입력이 여러 번 들어올 수 있다.
그때 gold row가 중복되면 안 된다.
```

### 3.9 Correction / Partition Overwrite

어려운 질문:

```text
changed source same business_date를 partition overwrite로 처리할 것인가?
```

쉬운 말:

```text
같은 날짜의 정정 파일이 오면 그 날짜 결과만 갈아끼울 것인가?
```

예:

```text
2026-06-29 결과가 이미 있다.
정정 파일이 와서 2026-06-29 결과를 다시 만든다.

원하는 것:
  2026-06-29만 새 값으로 교체
  2026-06-30은 그대로 유지
```

이 질문이 중요한 이유:

```text
append하면 같은 날짜 row가 중복될 수 있다.
whole-table overwrite를 잘못하면 다른 날짜까지 지울 수 있다.
```

### 3.10 Current State

어려운 질문:

```text
current state는 어디에 있는가?
```

쉬운 말:

```text
사람들이 지금 읽어야 하는 정답 위치는 어디인가?
```

선택지:

```text
latest successful run file:
  JSON/CSV 기반 mini project에 적합

Iceberg current snapshot:
  table format 기반 current state

manual latest folder:
  단순하지만 실수 위험이 큼
```

이 질문이 중요한 이유:

```text
history는 여러 개일 수 있지만, 분석가는 보통 "지금 기준 정답"을 읽고 싶어 한다.
```

### 3.11 Snapshot

어려운 질문:

```text
snapshot_id는 무엇인가?
```

쉬운 말:

```text
Iceberg table이 저장한 특정 순간의 번호다.
```

예:

```text
S1:
  정정 전 gold table 상태

S2:
  정정 후 gold table 상태
```

주의:

```text
snapshot_id는 run_id가 아니다.
run_id는 파이프라인 실행 이름이고,
snapshot_id는 table commit 이름이다.
```

### 3.12 Atomicity

어려운 질문:

```text
atomic commit이 필요한가?
```

쉬운 말:

```text
쓰다가 실패했을 때 반쯤 망가진 결과가 보이면 안 되지 않나?
```

예:

```text
좋은 상태:
  write 성공 -> 새 table 상태가 보임
  write 실패 -> 이전 table 상태가 그대로 보임

나쁜 상태:
  write 실패 -> 일부 파일만 바뀌어서 이상한 table이 보임
```

주의:

```text
atomic commit을 확인했다는 것이 production rollback system을 만들었다는 뜻은 아니다.
```

### 3.13 Spark / Distributed Processing

어려운 질문:

```text
Spark가 필요한 pressure는 무엇인가?
```

쉬운 말:

```text
왜 그냥 Python으로 처리하지 않고 Spark를 쓰려 하는가?
```

선택지:

```text
지금은 Python이면 충분:
  현재 toy data에는 맞다.

Spark local skeleton:
  Spark/Iceberg 설정과 write semantics를 검증한다.

full Spark port:
  전체 transform을 DataFrame으로 옮긴다. 범위가 커진다.
```

현재 프로젝트:

```text
full Spark port가 아니라 local single-gold-table skeleton만 구현했다.
```

### 3.14 Shuffle

어려운 질문:

```text
shuffle은 어디서 발생하는가?
```

쉬운 말:

```text
Spark가 여러 worker에 흩어진 데이터를 다시 섞어야 하는 순간은 어디인가?
```

예:

```text
groupBy:
  같은 key의 row를 한 곳으로 모아야 한다.

join:
  같은 key끼리 만나야 한다.

dropDuplicates:
  중복인지 확인하려면 같은 key끼리 비교해야 한다.
```

이 질문이 중요한 이유:

```text
shuffle은 비싼 작업이다.
Spark를 쓴다고 자동으로 빠른 것이 아니라, shuffle을 이해해야 한다.
```

### 3.15 Airflow / Orchestration

어려운 질문:

```text
orchestration boundary는 무엇인가?
```

쉬운 말:

```text
Airflow는 일을 직접 하는가, 이미 만든 CLI를 정해진 시간에 실행만 하는가?
```

좋은 방향:

```text
pipeline logic:
  src/manufacturing_data_platform 안에 둔다.

Airflow DAG:
  CLI command를 parameter와 함께 호출한다.
```

이 질문이 중요한 이유:

```text
business logic을 DAG 안에 넣으면 local CLI와 Airflow 실행 결과가 달라질 수 있다.
```

### 3.16 Observability / Operator Evidence

어려운 질문:

```text
observability는 무엇을 제공해야 하는가?
```

쉬운 말:

```text
문제가 생겼을 때 운영자가 무엇을 보고 원인 후보를 좁힐 수 있나?
```

예:

```text
run_id
source_hash
schema_hash
quality summary
row counts
lineage trace
snapshot_id
```

주의:

```text
operator report는 자동으로 원인을 찾아주는 도구가 아니다.
원인 후보를 좁힐 evidence를 보여주는 도구다.
```

### 3.17 Security / Privacy

어려운 질문:

```text
security/privacy/governance 질문은 무엇인가?
```

쉬운 말:

```text
공개하면 안 되는 것이 들어가 있지 않은가?
누가 볼 수 있고, 얼마나 보관하고, 무엇을 지워야 하는가?
```

현재 public repo에서 가장 중요한 질문:

```text
API key가 없는가?
회사/고객/내부 schema가 없는가?
synthetic data라고 명확히 말했는가?
```

### 3.18 Performance / Scale

어려운 질문:

```text
performance/scale claim이 가능한가?
```

쉬운 말:

```text
이게 큰 데이터에서도 빠르다고 말할 근거가 있는가?
```

현재 프로젝트:

```text
성능 claim은 하지 않는다.
Spark/Iceberg local skeleton은 correctness와 runtime feasibility evidence다.
대규모 처리 성능 evidence는 아니다.
```

### 3.19 Testing / Verification

어려운 질문:

```text
verification evidence는 무엇인가?
```

쉬운 말:

```text
내가 했다고 말하는 걸 무엇으로 증명하나?
```

예:

```text
pytest
CLI smoke
VERIFICATION_LOG.md
generated JSON evidence
README claim boundary
```

이 질문이 중요한 이유:

```text
이력서/블로그 문장은 기억이 아니라 evidence에서 나와야 한다.
```

### 3.20 Claim Boundary

어려운 질문:

```text
claim boundary는 무엇인가?
```

쉬운 말:

```text
어디까지 했다고 말해도 되고, 어디부터는 과장인가?
```

예:

```text
허용:
  local Spark/Iceberg single-gold-table walking skeleton을 구현했다.

금지:
  production lakehouse를 구축했다.
  full Spark pipeline을 구현했다.
  Airflow-triggered Spark runtime을 검증했다.
```

## 3.21 Scenario Trigger Index

영역 이름이 어렵다면 상황에서 시작한다.

| 상황 | 먼저 볼 질문 영역 |
|---|---|
| gold 숫자가 이상하다 | quality / reconciliation / lineage / operator evidence |
| 같은 파일을 다시 실행했다 | idempotency / retry / current state |
| 같은 날짜 정정 파일이 왔다 | correction / partition overwrite / snapshot / lineage |
| source에 새 컬럼이 생겼다 | source contract / schema drift / downstream contract |
| Airflow retry가 발생했다 | orchestration / idempotency / failure state |
| Spark를 붙이고 싶다 | distributed processing / storage / claim boundary |
| Iceberg snapshot을 남겼다 | table format / snapshot / run_id mapping / retention |
| README나 이력서에 쓰고 싶다 | testing / verification / claim boundary |
| public repo로 올리고 싶다 | security / privacy / synthetic data / secret scan |
| 나중에 Kafka를 하고 싶다 | streaming / CDC named Backlog |

이 표는 "어느 문서부터 봐야 할지"를 정하기 위한 입구다.

## 4. 질문을 실제 slice로 바꾸는 예시

### 예시: Spark/Iceberg partition overwrite

하고 싶은 것:

```text
같은 business_date의 정정 입력이 오면 gold 결과를 중복 없이 교체하고 싶다.
```

쉬운 질문:

```text
같은 입력인지 정정 입력인지 어떻게 구분하지?
어떤 날짜만 갈아끼워야 하지?
다른 날짜가 지워지면 안 되지?
갈아끼운 뒤 이전/이후 상태를 어떻게 증명하지?
이걸 full Spark pipeline이라고 말하면 과장 아닌가?
```

설계 질문으로 바꾸면:

```text
source_hash idempotency
business_date partition overwrite
D2 partition preservation
snapshot metadata
run_id -> snapshot_id mapping
claim boundary
```

Core로 내린 것:

```text
local SparkSession
Iceberg catalog
single gold table
overwritePartitions()
snapshot metadata read
JSON evidence
pytest + CLI
```

Backlog로 둔 것:

```text
full Spark medallion rewrite
quality-on-Spark
MERGE/upsert
concurrent writer handling
Airflow-triggered Spark runtime
production rollback
```

## 5. 읽는 순서

처음 읽을 때는 이 순서가 좋다.

```text
1. 이 문서
2. ../08-area-question-bank.ko.md
3. 관심 있는 상세 문서 하나
4. 실제 slice 문서: spark-iceberg/04-walking-skeleton-plan.md 같은 것
```

이렇게 읽으면 어려운 용어를 먼저 외우는 것이 아니라, 쉬운 질문이 설계 질문으로 바뀌는 과정을 따라갈 수 있다.
