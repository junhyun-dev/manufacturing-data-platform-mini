# 04. Orchestration / Observability 질문 상세

상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

이 문서는 Airflow/Dagster 같은 orchestration과 운영자 evidence/report 질문을 다룬다.

## 1. Orchestration / Scheduling / Airflow

### 질문의 의도

Airflow는 business logic을 넣는 곳이 아니라, 이미 검증된 pipeline을 언제/어떤 parameter로 실행할지 관리하는 곳이다.

나쁜 방향:

```text
DAG 안에 transform logic을 직접 쓴다.
local CLI와 Airflow 실행 경로가 달라진다.
retry가 idempotency 없이 output을 중복 생성한다.
```

좋은 방향:

```text
pipeline logic은 src/ module에 둔다.
CLI로 먼저 검증한다.
Airflow는 같은 CLI entrypoint를 parameter와 함께 호출한다.
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| DAG는 무엇을 호출하는가? | logic 중복 방지 | CLI / Python function / inline logic | Airflow wrapper를 만들 때 |
| parameter는 어떻게 전달하나? | backfill 가능성 | dag_run.conf / env var / hard-coded | business_date/raw_path가 필요할 때 |
| retry는 안전한가? | 중복 방지 | Airflow retry only / pipeline idempotency / both | scheduler를 붙일 때 |
| task split 기준은? | 운영 가시성 | one task / bronze-silver-gold split / quality separate | failure isolation이 필요할 때 |
| runtime 검증은 했는가? | claim boundary | import only / command contract / real trigger | 이력서/README에 Airflow를 말할 때 |

### 선택지 예시

DAG shape:

```text
single CLI task:
  작고 안전하다. business logic이 DAG에 없다.

multi-task DAG:
  실패 지점이 보이지만 intermediate contract가 많아진다.

inline Python logic:
  빠르게 보일 수 있지만 testability와 reuse가 떨어진다.
```

runtime claim:

```text
wrapper command contract verified:
  현재 프로젝트 상태.

Airflow runtime import verified:
  Airflow 설치 후 DAG import까지 확인.

Airflow trigger verified:
  실제 Airflow task run까지 확인.
```

### 놓치기 쉬운 질문

```text
Airflow retry와 source_hash skip이 같이 있을 때 의도한 run status가 무엇인가?
Airflow execution date와 business_date는 같은가 다른가?
Spark/Iceberg skeleton을 Airflow가 trigger하면 jar/package resolution은 어디서 보장하는가?
```

## 2. Observability / Operator Evidence

### 질문의 의도

운영자는 raw file이나 코드를 열기 전에 먼저 evidence를 보고 원인 후보를 좁히고 싶다.

observability는 dashboard만 뜻하지 않는다.

```text
run_id
source_hash
schema_hash
quality summary
row counts
lineage trace
snapshot_id
claim boundary
```

이런 것도 작지만 중요한 operator evidence다.

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| 운영자가 가장 먼저 보는 것은? | triage 순서 | run summary / quality / lineage / raw source | operator report를 만들 때 |
| lineage 수준은 어디까지인가? | overclaim 방지 | path-level / dataset-level / column-level | lineage claim을 할 때 |
| report가 판단을 자동화하는가? | anomaly/RCA 과장 방지 | evidence only / rule-based RCA / anomaly detection | 블로그/README 표현 시 |
| successful run만 보는가? | scope 명시 | latest success / failed run / all attempts | debugging report를 만들 때 |
| metric/log/alert는 있는가? | 운영 수준 | JSON report / logs / Prometheus / alert | production-like claim 시 |

### 선택지 예시

operator report scope:

```text
latest successful run:
  데이터 사용자가 현재 믿을 수 있는 evidence를 본다.

failed run forensics:
  왜 실패했는지 본다. 별도 state model이 필요하다.

full incident workflow:
  alerts, owners, escalation까지 필요하다. 현재 scope 아님.
```

lineage level:

```text
path-level:
  gold -> silver -> bronze -> source path를 보여준다.

dataset-level:
  dataset 간 dependency를 보여준다.

column-level:
  column transformation까지 추적한다. 구현 비용이 크다.
```

### 놓치기 쉬운 질문

```text
report 자체가 stale하면 어떻게 알 수 있는가?
quality fail이 없다는 것이 "데이터가 정상"이라는 뜻인가, "정의한 check는 통과"라는 뜻인가?
operator report가 자동 RCA처럼 읽히지 않게 문구를 제한했는가?
```
