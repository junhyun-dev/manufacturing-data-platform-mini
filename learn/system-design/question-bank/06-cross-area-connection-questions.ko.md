# 06. 영역 사이 연결 질문

상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

영역별 질문은 중요하지만, 실제 architecture 결정은 영역 사이에서 자주 깨진다.

```text
idempotency는 좋아 보이는데 failure와 만나면?
schema drift는 warn인데 downstream contract와 만나면?
Iceberg commit은 성공했는데 catalog write가 실패하면?
```

이 문서는 그런 seam 질문을 1급으로 모은다.

## 1. Idempotency x Failure

쉬운 질문:

```text
실패한 run 때문에 정당한 retry가 막히면 안 되지 않나?
```

설계 질문:

```text
skip gate는 successful run에만 걸리는가?
failed run의 source_hash가 같은 입력 재실행을 막지 않는가?
Airflow retry가 같은 source_hash를 다시 실행할 때 status는 무엇인가?
```

선택지:

```text
skip any matching run:
  위험하다. 실패 run 때문에 retry가 막힐 수 있다.

skip only successful run:
  현재 프로젝트 방향. idempotency와 retry가 잘 맞는다.

separate retry attempt tracking:
  production/task-level observability에서 필요하다.
```

Core가 되는 경우:

```text
retry/backfill/Airflow를 claim할 때.
```

## 2. Schema Drift x Downstream Contract

쉬운 질문:

```text
schema drift를 warn으로 통과시켰는데, 소비자가 읽는 gold 구조가 몰래 바뀌면 안 되지 않나?
```

설계 질문:

```text
source schema drift와 gold contract drift를 구분하는가?
새 컬럼을 source에서 감지했지만 gold에는 쓰지 않는다면 어떤 claim이 맞는가?
gold output column/type/not-null contract는 따로 검증하는가?
```

선택지:

```text
source drift warn only:
  현재 프로젝트 대부분의 범위.

gold model contract:
  downstream 소비자에게 gold output schema를 약속한다.

Iceberg schema evolution:
  table schema를 실제로 바꾼다. 구현/test 없이는 claim 금지.
```

## 3. Quality x Current State

쉬운 질문:

```text
quality fail 결과가 사람들이 읽는 최신 정답이 되면 안 되지 않나?
```

설계 질문:

```text
quality fail run은 latest_successful_run을 갱신하는가?
failed output이 current table/snapshot으로 노출되는가?
warn과 fail은 current state에 다르게 작용하는가?
```

선택지:

```text
success-only current:
  current pointer는 successful run만 가리킨다.

write failed output with failed status:
  forensic에는 좋지만 downstream read guard가 필요하다.

quarantine:
  bad rows/output을 별도 위치로 분리한다.
```

## 4. Correction x Lineage

쉬운 질문:

```text
정정 overwrite 후에도 이 숫자가 어느 source에서 왔는지 추적되나?
```

설계 질문:

```text
partition overwrite 후 current row의 source_hash를 알 수 있는가?
이전 snapshot의 source_hash와 새 snapshot의 source_hash를 구분하는가?
run_id -> snapshot_id mapping이 correction history를 설명하는가?
```

선택지:

```text
latest run only:
  현재 상태 설명은 쉽지만 history 비교가 약하다.

run_id -> snapshot_id history:
  correction 전후 evidence를 남긴다.

column/row-level lineage:
  더 강하지만 현재 scope 밖이다.
```

## 5. Reconciliation x Grain

쉬운 질문:

```text
row count를 비교할 때, 무엇을 한 줄로 세고 있는지 같은 기준인가?
```

설계 질문:

```text
source distinct key와 silver natural key가 같은 grain을 쓰는가?
gold conservation metric은 gold grain과 맞는가?
grain이 바뀌면 reconciliation expected도 바뀌는가?
```

선택지:

```text
raw row count only:
  filtering/dedup을 false loss로 오해할 수 있다.

distinct natural key:
  silver grain과 맞는 expected를 만든다.

metric conservation:
  row count가 아니라 units/defects 같은 business metric을 보존한다.
```

## 6. Rerun x Orchestration

쉬운 질문:

```text
Airflow가 retry했을 때 pipeline은 중복 output을 만들지 않나?
```

설계 질문:

```text
Airflow retry는 같은 CLI args를 다시 호출하는가?
pipeline idempotency가 source_hash로 retry를 skip하는가?
operator가 보는 status는 Airflow retry success인가, pipeline skipped인가?
```

선택지:

```text
orchestrator-only retry:
  pipeline idempotency가 없으면 중복 위험이 있다.

pipeline idempotency:
  retry해도 same source_hash는 no-op이 된다.

task attempt tracking:
  Airflow attempt와 pipeline run status를 함께 봐야 한다.
```

## 7. Catalog x Table Commit

쉬운 질문:

```text
Iceberg table commit은 성공했는데 run_id -> snapshot_id 기록이 실패하면 무엇을 믿어야 하나?
```

설계 질문:

```text
table commit과 catalog/evidence write의 순서는 무엇인가?
둘 중 하나만 성공하면 recovery는 어떻게 하나?
source of truth는 Iceberg metadata인가, 우리 catalog인가, 둘 다인가?
```

선택지:

```text
table commit first:
  table state는 안전하지만 catalog mapping 누락 위험이 있다.

catalog pending -> table commit -> catalog success:
  복구 모델을 만들 수 있지만 상태가 복잡해진다.

single local JSON evidence:
  walking skeleton에는 충분하지만 production consistency claim은 금지.
```

## 8. Freshness Semantics x Consumer

쉬운 질문:

```text
freshness pass라고 했을 때 소비자가 "최신 데이터"라고 오해하지 않나?
```

설계 질문:

```text
freshness_business_date는 partition/date validity인가, source age freshness인가?
consumer-facing 문서에 이 차이를 설명했는가?
age freshness SLA가 필요한 소비자가 있는가?
```

선택지:

```text
partition/date validity:
  현재 프로젝트 의미.

age freshness:
  source 도착 시간이 SLA를 만족하는지 본다.

both:
  이름과 report field를 분리해야 한다.
```

## 9. Security x Public Claim

쉬운 질문:

```text
블로그나 README 예시가 public-safe한가?
```

설계 질문:

```text
synthetic data임을 반복해서 명시했는가?
credential/local path/non-public hint가 없는가?
실무 경험과 개인 프로젝트 구현 범위를 구분했는가?
```

Core가 되는 경우:

```text
public push, blog draft, resume line 작성 전에는 항상 Core gate다.
```
