# 02. Quality / Rerun / Failure 질문 상세

상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

이 문서는 "데이터를 만들었다" 다음에 반드시 나오는 질문들을 다룬다.

```text
믿을 수 있는가?
다시 돌려도 안전한가?
실패하면 무엇이 남는가?
```

## 1. Quality / Reconciliation

### 질문의 의도

quality check는 단순히 fail/pass를 붙이는 장식이 아니다.

목적은 아래를 구분하는 것이다.

```text
정상 filtering
정상 dedup
실제 row loss
metric conservation 깨짐
schema drift
domain violation
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| 어떤 check가 trust를 만드는가? | 품질 claim 근거 | not_null / unique / accepted_values / range / freshness / reconciliation | README/blog에 quality를 말할 때 |
| reconciliation expected는 독립적으로 계산되는가? | tautology 방지 | transform 결과 재사용 / source에서 독립 계산 | row loss claim을 할 때 |
| filtering과 loss를 구분하는가? | false alarm 방지 | detail에 breakdown / 단순 count 비교 | source->silver check가 있을 때 |
| conservation metric은 무엇인가? | 집계 검증 | units / defects / amount / count | silver->gold aggregation이 있을 때 |
| quality fail이면 write를 막는가? | 상태 오염 방지 | fail before write / write then mark failed / quarantine | current table을 만들 때 |

### 선택지 예시

quality fail policy:

```text
fail before catalog success:
  successful run만 current로 보게 한다.

write output but mark failed:
  forensic에는 좋지만 downstream이 failed output을 읽지 않게 해야 한다.

warn only:
  schema drift처럼 허용 가능한 변화에 적합하다.
```

freshness:

```text
partition/date validity:
  active business_date가 맞는지 확인한다.

age freshness:
  source가 얼마나 최근에 도착했는지 확인한다.

현재 프로젝트의 freshness_business_date는 partition/date validity다.
```

### 놓치기 쉬운 질문

```text
quality check 이름이 외부 도구(dbt/DataHub)의 의미와 충돌하지 않는가?
warning과 failure가 downstream current state에 미치는 영향이 다른가?
quality report 자체의 schema/version은 관리하는가?
```

## 2. Rerun / Backfill / Correction

### 질문의 의도

재처리는 하나가 아니다.

```text
retry:
  같은 입력을 다시 실행한다.

backfill:
  과거 business_date를 새로 채운다.

correction:
  같은 business_date에 다른/정정된 입력이 들어온다.
```

이 셋을 구분하지 않으면 append 중복이나 잘못된 skip이 생긴다.

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| 같은 입력인지 어떻게 아는가? | retry skip 판단 | source_hash / upstream id / run args | idempotency claim이 있을 때 |
| 같은 날짜 다른 입력은 무엇인가? | correction 판단 | new run only / overwrite current / merge | business_date 재처리가 있을 때 |
| append가 안전한가? | 중복 방지 | append / partition overwrite / merge | gold current table을 만들 때 |
| overwrite 범위는 무엇인가? | blast radius 제한 | whole table / partition / row-level merge | Iceberg/warehouse write가 있을 때 |
| rerun evidence는 무엇인가? | 감사 가능성 | reuse_count / snapshot_id / run status | operator report/blog claim이 있을 때 |

### 선택지 예시

same source:

```text
always recompute:
  단순하지만 불필요한 output/commit이 늘어난다.

skip existing success:
  retry/backfill idempotency가 명확하다.

overwrite anyway:
  current state는 유지되지만 audit trail이 흐려질 수 있다.
```

changed source same business_date:

```text
new run folder only:
  history는 남지만 current gold 교체 의미가 약하다.

partition overwrite:
  날짜 단위 correction에 적합하다.

MERGE:
  row-level late arrival에 적합하지만 scope가 커진다.
```

### 놓치기 쉬운 질문

```text
같은 file 내용이 다른 path로 들어오면 같은 입력인가?
source_hash는 raw file 전체 기준인가, normalized source 기준인가?
correction이 실패하면 이전 current state를 유지해야 하는가?
overwrite 후 이전 값은 snapshot/history로 재현 가능한가?
```

## 3. Failure State / Retry / Recovery

### 질문의 의도

성공 run만 있는 시스템은 운영 질문의 절반만 답한다.

장애/실패 설계는 아래를 묻는다.

```text
어디까지 쓰고 죽었는가?
사용자가 무엇을 보면 되는가?
retry하면 안전한가?
failed output이 current로 노출되지 않는가?
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| 실패 run도 catalog에 남기는가? | forensic 가능성 | 남김 / 안 남김 / 일부만 남김 | operator debugging을 실패까지 확장할 때 |
| partial output은 어떻게 처리하나? | 오염 방지 | temp then commit / run folder isolation / cleanup | write 중 실패가 가능한 slice |
| retry는 어떤 key로 안전해지는가? | 중복 방지 | source_hash / run_id / task attempt id | Airflow/Spark retry가 있을 때 |
| failure reason은 어디에 남나? | 운영자 가시성 | exception text / structured error / quality report | 실패 report claim을 할 때 |
| recovery action은 무엇인가? | 운영 절차 | rerun / rollback / manual fix / quarantine | production-like claim을 할 때 |

### 선택지 예시

failure record:

```text
no failed record:
  구현은 쉽지만 실패 조사 evidence가 없다.

failed run record:
  운영자가 볼 수 있지만 schema가 필요하다.

failed task-level record:
  Airflow/task split 이후 유용하다.
```

partial output handling:

```text
run folder isolation:
  failed run output이 successful current와 섞이지 않는다.

table atomic commit:
  commit 실패 시 이전 snapshot이 유지된다.

manual cleanup:
  운영 비용이 크고 claim하기 어렵다.
```

### 놓치기 쉬운 질문

```text
quality fail은 technical failure인가 business failure인가?
failed run도 latest_successful_run baseline에 영향을 주면 안 되지 않는가?
Airflow retry와 pipeline idempotency가 서로 충돌하지 않는가?
```
