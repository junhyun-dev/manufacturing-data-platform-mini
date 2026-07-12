# 05. Security / Performance / Testing / Claim 질문 상세

상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

이 문서는 public portfolio로 갈 때 특히 중요한 영역을 다룬다.

```text
민감정보가 없는가?
scale/performance를 과장하지 않는가?
테스트와 verification log가 있는가?
이력서/블로그 claim이 evidence를 넘지 않는가?
```

## 1. Security / Privacy / Governance / Retention

### 질문의 의도

개인 프로젝트라도 공개 repo라면 보안/프라이버시 질문은 Core gate다.

이 영역은 "보안 기능을 구현했는가"보다 먼저 아래를 묻는다.

```text
공개하면 안 되는 것이 들어갔는가?
synthetic data임을 명확히 했는가?
권한/PII/retention을 claim하고 있지는 않은가?
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| credential이 repo에 없는가? | 공개 안전성 | rg scan / git history scan / credential scanner | public push 전 항상 |
| private/company data가 없는가? | IP/윤리 | synthetic only / anonymized / real data 금지 | portfolio repo 항상 |
| PII column이 있는가? | governance | none / tag only / access control | PII claim이 있을 때 |
| retention은 필요한가? | storage/privacy | no policy / manual cleanup / expire snapshots | snapshot/history를 오래 남길 때 |
| 접근 제어가 필요한가? | security scope | none / local only / auth/RBAC | API를 public service처럼 말할 때 |

### 선택지 예시

data policy:

```text
fully synthetic:
  public portfolio에 가장 안전하다.

anonymized real data:
  재식별 위험과 설명 부담이 있다.

real internal schema:
  public repo에는 부적절하다.
```

retention:

```text
not implemented:
  mini project에서는 정직하게 backlog로 둔다.

manual cleanup:
  local demo에는 가능하지만 운영 claim은 약하다.

automated retention:
  production-like storage 운영 claim이 가능하지만 범위가 커진다.
```

### 놓치기 쉬운 질문

```text
.env가 .gitignore에 있어도 이미 commit된 적은 없는가?
블로그 예시에 실제 이메일/path/company hint가 들어가지 않았는가?
Iceberg snapshot/history는 데이터를 오래 보존하므로 retention 질문이 생기지 않는가?
```

## 2. Performance / Scale / Cost

### 질문의 의도

toy project라도 scale 질문을 완전히 무시하면 설계가 얕아진다.

다만 scale 질문을 했다고 바로 대규모 구현을 해야 하는 것은 아니다.

```text
지금은 local proof다.
하지만 나중에 커지면 어디가 병목인지 알고 있다.
```

이 정도로 정리하는 것이 현재 portfolio에는 적절하다.

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| 현재 row 수에서 Spark가 필요한가? | 도구 과잉 방지 | Python 충분 / Spark demo / Spark required | Spark 도입 명분을 말할 때 |
| 가장 비싼 연산은 무엇인가? | 병목 이해 | groupBy / join / dedup / count checks | distributed processing claim 시 |
| 비싼 연산을 어떻게 관측하나? | 감이 아니라 evidence로 설명 | explain plan / Spark UI stage / query metrics / simple timing | 성능이나 shuffle을 말할 때 |
| small files 위험은 있는가? | table layout 이해 | ignore / compact backlog / partition 조정 | Iceberg 운영 claim 시 |
| cost를 어떻게 제한하나? | 운영 현실 | local only / retention / partition pruning | cloud/object storage를 쓸 때 |
| benchmark가 필요한가? | claim 근거 | no / micro benchmark / realistic load test | performance claim을 할 때 |

### 선택지 예시

performance claim:

```text
no performance claim:
  현재 프로젝트에 가장 안전하다.

local feasibility claim:
  Spark/Iceberg가 local에서 동작함을 말한다.

scale claim:
  load test, metrics, realistic data volume이 필요하다.
```

관측 방법:

```text
explain plan:
  어떤 연산에서 shuffle/exchange가 생기는지 본다.

Spark UI / stages:
  local mode에서도 stage 수와 shuffle을 볼 수 있다.

simple timing:
  toy project에서는 참고만 가능하다. scale claim 근거로는 약하다.

load test:
  성능 claim을 하려면 realistic data volume과 반복 측정이 필요하다.
```

### 놓치기 쉬운 질문

```text
pytest 통과는 correctness evidence이지 performance evidence가 아니다.
Spark를 썼다고 자동으로 대규모 처리 경험이 되는 것은 아니다.
partition pruning을 claim하려면 query/explain evidence가 있어야 하지 않는가?
```

## 3. Testing / Local Reproducibility / CI

### 질문의 의도

claim은 테스트와 실행 로그로 닫아야 한다.

```text
테스트가 있는가?
CLI smoke가 있는가?
환경 의존성이 실패하면 이유가 명확한가?
verification log에 날짜/명령/결과가 있는가?
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| pure function test가 있는가? | logic 검증 | transform unit test / integration only | transform claim 시 |
| CLI smoke가 있는가? | 사용자 실행성 | none / local CLI / Docker CLI | README run command가 있을 때 |
| optional dependency test는 어떻게 처리하나? | 환경 차이 대응 | skip reason / separate requirements / hard fail | Spark/Airflow/Mongo runtime |
| verification log가 최신인가? | evidence 추적 | no log / manual log / CI artifact | 블로그/이력서 claim 전 |
| 실패 테스트도 있는가? | boundary 검증 | happy path only / fail/warn cases | quality/schema claim 시 |

### 선택지 예시

Spark test policy:

```text
skip if pyspark unavailable:
  base install을 가볍게 유지한다.

requirements-spark install 후 run:
  local skeleton evidence를 만든다.

CI mandatory:
  더 강하지만 CI 시간/네트워크/jar 의존성이 커진다.
```

### 놓치기 쉬운 질문

```text
테스트가 skip됐는데 passed처럼 말하고 있지 않은가?
CLI가 /tmp output을 덮어쓰는 경우 clean 옵션이 있는가?
verification log가 실제 command 결과와 일치하는가?
```

## 4. Public Claim / Blog / Resume Boundary

### 질문의 의도

포트폴리오에서 가장 위험한 것은 구현보다 크게 말하는 것이다.

이 영역의 핵심은 다음이다.

```text
implemented = code + tests + verification log
designed = docs/question map/decision note
walking skeleton = local proof, not production
runtime unverified = explicit caveat
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| 이 claim은 evidence가 있는가? | 과장 방지 | code/test/log 있음 / design-only / 없음 | README/blog/resume 작성 시 |
| wording이 범위를 넘는가? | 시장 언어 조정 | implemented / designed / modeled / explored | 외부 공개 전 |
| runtime 미검증을 명시했는가? | 정직성 | verified / wrapper only / unverified | Airflow/Mongo/Spark runtime |
| synthetic data임을 말했는가? | public-safe | synthetic / anonymized / real | 모든 글/README |
| forbidden wording은 무엇인가? | 선 긋기 | production / operated / full pipeline / lakehouse | resume/blog 전 |

### 선택지 예시

Spark/Iceberg wording:

```text
허용:
  local Spark/Iceberg single-gold-table walking skeleton
  business_date partition overwrite
  snapshot evidence

금지:
  production lakehouse
  full Spark/Iceberg pipeline
  operated Iceberg in production
```

Airflow wording:

```text
허용:
  Airflow local dags test runtime wrapper is verified.
  Airflow local standalone scheduler/LocalExecutor run is verified for the Spark/Iceberg skeleton.

금지:
  operated production Airflow pipelines in this repo.
  production scheduler/worker deployment verified.
```

### 놓치기 쉬운 질문

```text
블로그 제목이 본문보다 크게 주장하지 않는가?
이력서 한 줄이 README claim boundary와 충돌하지 않는가?
Claude audit output을 검증 없이 claim으로 승격하지 않았는가?
```
