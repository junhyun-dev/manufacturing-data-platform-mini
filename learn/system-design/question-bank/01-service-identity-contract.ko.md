# 01. Service / Identity / Source Contract 질문 상세

상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

이 문서는 기능을 만들기 전에 가장 먼저 묻는 질문들을 다룬다.

```text
누가 쓰는가?
무엇을 믿고 싶은가?
row/source/run/table의 identity는 무엇인가?
입력 contract가 바뀌면 어떻게 알 수 있는가?
```

## 1. Service / User Workflow

### 질문의 의도

기능 이름이 아니라 사용자의 막힘에서 출발하기 위한 영역이다.

나쁜 출발:

```text
Spark를 붙인다.
Airflow를 붙인다.
Iceberg를 붙인다.
```

좋은 출발:

```text
분석가가 gold 숫자를 의심할 때 raw file을 열기 전에 무엇을 확인할 수 있어야 하는가?
운영자가 같은 날짜 정정 파일을 받았을 때 중복 없이 반영됐는지 어떻게 확인할 수 있어야 하는가?
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| 누가 이 slice를 쓰는가? | actor를 고정한다 | analyst / operator / reviewer / scheduler | actor마다 필요한 output이 달라질 때 |
| 사용자가 어떤 상황에서 막히는가? | scenario pressure를 만든다 | 이상한 gold 숫자 / 재처리 / schema drift / 실패 run | 구현할 기능의 trigger가 달라질 때 |
| 이 slice가 끝나면 어떤 질문에 답해야 하는가? | service question을 만든다 | "어디서 왔나" / "다시 돌려도 안전한가" / "정정됐나" | output/evidence shape가 달라질 때 |
| 사용자가 보지 않아도 되는 것은 무엇인가? | 과한 UI/API를 막는다 | raw file 직접 열기 / Spark UI / warehouse 내부 metadata | 사용자-facing evidence를 설계할 때 |

### 선택지 예시

```text
operator-first:
  run/source/quality/lineage evidence를 빠르게 본다.

analyst-first:
  gold row grain과 metric definition을 먼저 본다.

reviewer-first:
  repo evidence, test, verification log, claim boundary를 본다.
```

현재 프로젝트는 operator + reviewer 성격이 강하다.

### 놓치기 쉬운 질문

```text
사용자가 이 기능을 CLI로 쓰는가, report 파일로 보는가, API로 보는가?
사용자가 성공 run만 보는가, 실패 run도 봐야 하는가?
사용자가 "정답"을 원하는가, "원인 후보를 좁히는 evidence"를 원하는가?
```

## 2. Data Grain / Identity / Versioning

### 질문의 의도

데이터 플랫폼은 결국 identity 관리다.

```text
source는 무엇인가?
run은 무엇인가?
dataset은 무엇인가?
version은 무엇인가?
snapshot은 무엇인가?
gold row 하나는 무엇을 의미하는가?
```

이 질문이 흐리면 lineage, idempotency, resume claim이 모두 흐려진다.

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| source identity는 무엇인가? | 같은 입력인지 판단 | file hash / path / upstream event id | rerun/idempotency가 필요할 때 |
| schema identity는 무엇인가? | 구조 변화 감지 | required columns hash / actual header hash / catalog schema version | schema drift claim을 할 때 |
| run_id는 무엇인가? | 실행 단위를 고정 | timestamp+uuid / orchestrator run id / deterministic id | run evidence를 남길 때 |
| gold row grain은 무엇인가? | metric 의미를 고정 | date-line-product / date-entity / date-plant | gold table/report/blog claim이 있을 때 |
| snapshot_id는 run_id를 대체하는가? | engine id와 business id 분리 | 대체 / 참조 / 둘 다 기록 안 함 | Iceberg/Delta 같은 table format을 쓸 때 |

### 선택지 예시

source identity:

```text
path-based:
  쉽지만 같은 path에 다른 파일이 올 수 있다.

content hash:
  같은 내용 재실행을 안정적으로 감지한다.

upstream id:
  실제 source system이 event/file id를 제공할 때 좋다.
```

snapshot mapping:

```text
run_id -> snapshot_id:
  pipeline 실행과 table commit을 분리해 설명할 수 있다.

snapshot_id만 사용:
  table 관점은 좋지만 pipeline run/source/quality와 연결이 약해진다.

run_id만 사용:
  table commit evidence가 사라진다.
```

현재 프로젝트는 `run_id -> snapshot_id`를 선택했다.

### 놓치기 쉬운 질문

```text
한 run이 여러 table commit을 만들면 1:1 mapping이 깨지지 않는가?
dataset_version과 lakehouse run은 같은 개념인가?
gold row grain이 바뀌면 기존 블로그/이력서 claim도 바뀌는가?
```

## 3. Source Contract / Schema Evolution

### 질문의 의도

입력이 바뀌었을 때 조용히 잘못된 gold를 만들지 않기 위한 영역이다.

```text
필수 컬럼이 빠졌는가?
새 컬럼이 생겼는가?
타입이 바뀌었는가?
그 변화는 허용 가능한 evolution인가, 위험한 breaking change인가?
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| required columns는 무엇인가? | 최소 입력 계약 | hard-coded list / schema file / catalog schema | transform이 의존할 때 |
| missing required column은 어떻게 처리하나? | 실패 정책 | fail-fast / quality fail record / quarantine | 잘못된 입력을 public claim으로 다룰 때 |
| added column은 어떻게 처리하나? | schema drift 정책 | ignore / warn / fail / evolve table | schema drift를 claim할 때 |
| type change는 어떻게 처리하나? | semantic break 감지 | cast fail / warn / explicit migration | numeric/string contract가 중요할 때 |
| Iceberg schema evolution은 언제 쓰나? | detect에서 evolve로 넘어갈지 결정 | design-only / demo / implemented | table schema를 실제로 바꿀 때 |

### 선택지 예시

missing required column:

```text
fail-fast:
  구현이 단순하고 잘못된 입력을 빨리 막는다.
  대신 quality report로 남기기 어렵다.

structured quality fail:
  운영자가 report에서 볼 수 있다.
  transform 전 validation layer가 필요하다.

quarantine:
  일부 bad row만 분리할 수 있다.
  scope가 커진다.
```

schema drift:

```text
warn:
  legitimate schema change를 막지 않는다.
  downstream contract는 별도 방어가 필요하다.

fail:
  안전하지만 evolution을 막을 수 있다.

evolve:
  Iceberg/warehouse table schema에 반영한다.
  public claim을 하려면 code/test evidence가 필요하다.
```

### 놓치기 쉬운 질문

```text
schema drift는 source schema 변화인가, gold contract 변화인가?
새 컬럼을 감지했지만 gold에는 쓰지 않는다면 claim을 어떻게 표현해야 하나?
rename은 added+removed로 보이는가?
과거 run과 현재 run의 schema_hash 비교 기준은 latest successful run인가, 같은 business_date인가?
```
