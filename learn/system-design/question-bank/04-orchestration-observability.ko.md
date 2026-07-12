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
| worker runtime dependency는 맞는가? | scheduler 실행 실패 방지 | Airflow-only venv / project deps 포함 / packaged image | scheduler/worker 경로를 붙일 때 |

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

Airflow standalone scheduler verified:
  local standalone에서 scheduler/LocalExecutor가 task를 성공 처리.
  단, production deployment와는 별개.
```

### 놓치기 쉬운 질문

```text
Airflow retry와 source_hash skip이 같이 있을 때 의도한 run status가 무엇인가?
Airflow execution date와 business_date는 같은가 다른가?
Spark/Iceberg skeleton을 Airflow가 trigger하면 jar/package resolution은 어디서 보장하는가?
Airflow worker의 `python`은 project dependency와 Spark dependency를 모두 갖고 있는가?
```

### 실무 심화 질문

아래 질문들은 "Airflow를 붙였다"를 넘어서, 실제 운영 DAG 설계 리뷰에서 자주 갈리는 지점이다.

| 질문 | 쉬운 말로 풀면 | 왜 묻는가 | 선택지 | 이번 프로젝트 판단 |
|---|---|---|---|---|
| `logical_date`, `data_interval`, `business_date`는 같은 개념인가? | Airflow가 생각하는 날짜와 데이터 날짜가 같은가? | backfill 때 날짜가 섞이면 잘못된 partition을 처리한다. | `ds` 그대로 사용 / `dag_run.conf`로 명시 / 별도 calendar table | Core: `business_date`는 `dag_run.conf`로 명시 가능하게 둔다. |
| manual trigger의 conf를 검증하는가? | 사용자가 이상한 날짜/path를 넣으면 막는가? | 잘못된 path나 날짜가 production output을 덮을 수 있다. | validation 없음 / CLI argparse 검증 / DAG 시작 task에서 검증 | Backlog: 현재는 CLI와 test 중심, conf schema 검증은 별도 slice. |
| `catchup=True`인가, manual backfill인가? | 과거 날짜를 자동으로 모두 돌릴 것인가? | Airflow catchup은 의도치 않은 대량 재처리를 만들 수 있다. | catchup off / scheduled catchup / explicit backfill command | Core: `catchup=False`, manual backfill-style run. |
| DAG run이 동시에 여러 개 떠도 되는가? | 같은 table/partition을 동시에 쓰게 둘 것인가? | 같은 `business_date` correction이 동시에 오면 current state race가 생긴다. | `max_active_runs=1` / Airflow pool / external lock / table optimistic commit 의존 | Core-lite: local DAG는 `max_active_runs=1`; production concurrency는 Backlog. |
| task retry는 어떤 side effect를 다시 만들 수 있는가? | 재시도하면 파일/table/catalog가 중복되나? | Airflow retry는 같은 command를 다시 실행한다. pipeline idempotency 없으면 위험하다. | retry 없음 / retry + source_hash skip / retry + publish state skip | Core: lakehouse는 source_hash skip, publish는 same run publish skip. |
| timeout은 어디에 걸 것인가? | 오래 걸리는 task를 언제 실패로 볼 것인가? | Spark job이 멈추면 worker slot을 계속 잡는다. | task execution_timeout / Spark config timeout / external watchdog | Core-lite: DAG task timeout만 둔다. |
| task boundary는 어떤 state boundary와 맞는가? | task 하나가 끝났다는 건 무엇이 완료됐다는 뜻인가? | task를 너무 잘게 쪼개면 중간 state contract가 늘어난다. | one CLI task / pipeline task + publish task / bronze-silver-gold split | 현재: `lakehouse -> publish` 2-task가 가장 작은 meaningful boundary. |
| worker dependency는 어떻게 고정되는가? | worker가 실행하는 `python`에 필요한 패키지가 다 있는가? | scheduler/webserver venv와 worker venv가 다르면 import/runtime 실패가 난다. | ad hoc venv / pinned requirements / packaged image | Core-lite: pinned requirements + local venv. production image는 Backlog. |
| DAG deploy/version은 어떻게 관리하는가? | DAG 파일이 바뀐 시점과 pipeline code 버전을 추적하는가? | 같은 data/source라도 code version이 다르면 결과가 달라질 수 있다. | git SHA 기록 / package version / Airflow deployment version | Backlog: code/logic version identity 질문과 연결. |
| XCom에 무엇을 넣을 것인가? | 큰 데이터나 JSON을 Airflow DB에 넣지 않는가? | XCom은 metadata 전달용이지 dataset 저장소가 아니다. | path만 전달 / run_id만 전달 / 큰 payload 저장 | Core: CLI output/evidence는 파일에 쓰고, DAG는 command만 실행. |
| connection/credential은 어디에서 관리하는가? | DB 비밀번호나 인증값을 DAG 파일에 쓰지 않는가? | 공개 repo/운영 보안 모두에서 위험하다. | env var / Airflow Connection / credential backend | 현재 synthetic/local이라 민감정보 없음. real system은 Backlog. |
| 로그는 어디에 남고 얼마나 보존되는가? | 실패 후 로그를 다시 볼 수 있는가? | local 로그와 production remote log는 claim 수준이 다르다. | local stdout / Airflow task log / remote logging | Core-lite: local task log만. production remote logging은 Backlog. |
| `dags test`, `dags trigger`, scheduler run은 각각 무엇을 증명하는가? | 실행 방법마다 claim이 다르다는 걸 아는가? | `dags test` 성공을 production scheduler 검증처럼 말하면 과장이다. | command render / local task run / scheduler-executor path / production deployment | 문서에서 각각 분리해서 claim한다. |

### 실무에서 자주 나오는 나쁜 답변

```text
"Airflow가 retry하니까 idempotent하다."
  -> 틀림. retry는 다시 실행할 뿐이고, idempotency는 pipeline/table/catalog contract가 만든다.

"DAG가 성공했으니 데이터가 맞다."
  -> 틀림. DAG success는 command exit 0이고, data correctness는 quality/evidence가 따로 증명한다.

"dags test가 됐으니 Airflow 운영 검증이다."
  -> 과장. dags test는 local 단일 DagRun 검증이고, scheduler/executor/worker fleet 운영과 다르다.
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
| report 자체가 stale한가? | 오래된 evidence 오해 방지 | generated_at / source run timestamp / freshness check | operator report를 소비자가 볼 때 |
| metric/log/alert는 있는가? | 운영 수준 | JSON report / logs / SLI/SLO / delay alert / Prometheus | production-like claim 시 |
| downstream consumer를 아는가? | impact analysis | none / manual list / dbt exposures / catalog ownership | gold schema나 metric을 바꿀 때 |

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

observability depth:

```text
JSON evidence report:
  현재 프로젝트 수준. 작은 repo evidence로 적합하다.

logs:
  실행 중 어떤 일이 있었는지 시간 순서로 본다.

SLI/SLO / alert:
  지연, 실패율, freshness breach를 운영 신호로 본다.

impact analysis:
  upstream 변경이 어떤 downstream dataset/dashboard/model에 영향을 주는지 본다.
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
gold를 바꾸면 어떤 downstream 소비자가 영향을 받는지 알고 있는가?
```

### 실무 심화 질문

| 질문 | 쉬운 말로 풀면 | 왜 묻는가 | 선택지 | 이번 프로젝트 판단 |
|---|---|---|---|---|
| 운영자가 보는 첫 화면은 run인가, dataset인가, alert인가? | 문제를 어디서부터 조사하게 할 것인가? | entrypoint가 없으면 evidence가 있어도 못 쓴다. | dataset page / failed run page / alert detail / CLI report | Core: operator report는 business_date 기준 read-only entrypoint. |
| 어떤 SLI/SLO를 볼 것인가? | 성공률/지연/품질 중 무엇을 운영 지표로 삼나? | observability claim은 지표 정의 없이는 흐릿하다. | run success rate / freshness lag / quality fail rate / publish latency | Backlog: production SLI/SLO는 아직 claim하지 않음. |
| alert는 언제 울려야 하는가? | 어떤 상태가 사람을 깨울 만큼 중요한가? | 모든 fail을 alert로 만들면 noise가 된다. | task failure / quality fail / freshness breach / publish missing | Backlog: alerting stack 없음. |
| failure-state evidence는 남는가? | 실패한 run도 조사할 수 있는가? | successful run report만 있으면 실패 원인 분석은 약하다. | latest success only / failed attempts / partial artifacts / incident timeline | 다음 slice 후보: failure-state forensics. |
| evidence retention은 얼마나 되는가? | 예전 run 증거를 언제까지 보관하나? | snapshot/log/catalog retention이 다르면 재현성이 깨질 수 있다. | keep all / time-based retention / compacted summaries | Backlog: local evidence는 보존, retention policy는 없음. |
| lineage는 upstream만 보는가, downstream impact도 보는가? | 이 데이터가 어디서 왔는지와 누가 쓰는지가 둘 다 보이는가? | schema/metric 변경 시 영향 범위를 알아야 한다. | upstream path / downstream exposures / owner mapping | Core: upstream path-level. downstream exposure는 Backlog. |
| metric definition은 versioning되는가? | defect_rate 공식이 바뀌면 추적되는가? | 같은 gold column도 정의가 바뀌면 다른 metric이다. | code only / metric registry / semantic layer | Backlog: metric semantic layer 없음. |
| report가 stale한지 어떻게 알 수 있는가? | 오래된 evidence를 최신으로 오해하지 않게 하는가? | operator가 과거 run을 current로 착각할 수 있다. | generated_at / source run timestamp / current pointer check | Core-lite: run_id/source_hash/snapshot evidence로 일부 방지. |
| table snapshot과 catalog run mapping이 깨지면 어떻게 찾는가? | Iceberg commit은 됐는데 evidence JSON이 없으면? | table과 catalog는 서로 다른 system이라 불일치가 생길 수 있다. | reconciliation report / table metadata scan / manual repair | Backlog: exactly-once table/catalog transaction은 claim하지 않음. |
| "자동 RCA"라고 말할 수 있는가? | report가 원인을 찾아주는가, 후보를 좁혀주는가? | overclaim 방지. | evidence only / rule-based RCA / ML anomaly detection | Core: evidence only. 자동 RCA 아님. |
