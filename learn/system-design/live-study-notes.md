# Live Study Notes — Slice2 Spark/Iceberg

상태: raw Q/A 공부 로그

용도: 문서를 읽다가 생긴 질문과 답변을 **원문 그대로** 계속 붙이는 곳이다. 여기서는 요약하거나 정리하려고 하지 않는다. 정리는 나중에 필요할 때 `scenarios/`, `reference-decisions/`, README로 따로 승격한다.

형식:

```text
## YYYY-MM-DD — topic

### Q
<질문 원문>

### A
<답변 원문>
```

---

## 2026-07-07 — Slice / business_date

### Q
슬라이스는 뭐야? 하나씩 이햏봐야 겠다 그리고 business_date는 영업일이야?

### A
좋아. 여기부터 하나씩 잡으면 돼.

**Slice가 뭐냐**

여기서 `Slice`는 “프로젝트 전체를 한 번에 다 만들지 않고, 의미 있는 작은 단위로 잘라서 구현한 단계”야.

```text
전체 목표:
  작은 데이터 플랫폼 만들기

Slice1:
  Python/CSV + Mongo/JSON으로 medallion pipeline 구현
  bronze/silver/gold, quality, lineage, idempotency 증명

Slice2:
  같은 medallion pipeline을 Spark + Iceberg로 확장
  ACID overwrite, partitioning, snapshot, schema evolution 일부 증명
```

즉 `Slice`는 기능 조각이긴 한데, 그냥 작은 기능 하나가 아니라 **끝까지 동작하는 얇은 세로 조각**이야.

예를 들면:

```text
나쁜 쪼개기:
  Spark 설치만 함
  Iceberg 문서만 읽음
  quality 함수만 만듦

좋은 Slice:
  작은 입력 -> 처리 -> 저장 -> 품질검사 -> lineage -> 테스트
  이 흐름이 끝까지 돈다
```

그래서 Slice1은 “작지만 완결된 CSV 기반 데이터 플랫폼”이고, Slice2는 “그걸 Spark/Iceberg로 확장하는 다음 완결 조각”이야.

**business_date가 영업일이냐**

여기서는 `business_date`를 “영업일”이라고 번역하면 조금 헷갈릴 수 있어.

더 정확히는:

```text
business_date = 이 row/metric을 어느 업무 기준 날짜에 귀속할 것인가
```

한국어로는 보통:

```text
기준일
업무 기준일
집계 기준일
처리 대상일
```

에 가까워.

제조/로봇 이벤트 예시에서는:

```text
event_time = 실제 이벤트가 발생한 시각
business_date = 이 이벤트를 어느 날짜의 생산/품질 집계에 넣을지
```

예:

```text
event_time: 2026-06-29T23:58:00Z
business_date: 2026-06-30
```

이럴 수 있어. 왜냐면 공장 교대조, 타임존, 마감 기준 때문에 실제 timestamp와 집계 기준일이 다를 수 있거든.

일반 회사 데이터에서도 비슷해.

```text
order_created_at = 주문이 실제 생성된 시각
sales_date = 매출 기준일

payment_time = 결제가 실제 승인된 시각
settlement_date = 정산 기준일

event_time = 사용자가 행동한 시각
business_date = 리포트에 반영할 기준일
```

그래서 이 프로젝트에서 `business_date`는 그냥 “영업일”이라기보다:

> gold metric을 어떤 날짜 partition에 넣을지 결정하는 업무 기준일

이렇게 이해하면 돼.

**왜 business_date가 중요하냐**

같은 날짜를 다시 처리한다는 말은:

```text
2026-06-29에 해당하는 silver/gold 결과를 다시 만든다
```

는 뜻이야.

Iceberg에서는 이게 partition 기준이 될 수 있어.

```text
gold_daily_metrics
partition: business_date
```

그러면 재처리할 때:

```text
business_date = 2026-06-29 partition만 overwrite
```

할 수 있어.

즉 `business_date`는 단순 컬럼이 아니라:

```text
- 재처리 단위
- partition 단위
- quality check 기준
- lineage/run 조회 기준
- gold mart grain의 일부
```

가 돼.

한 줄로 정리하면:

```text
Slice = 끝까지 동작하는 학습/구현 단위
business_date = 이벤트를 어느 업무 기준 날짜의 결과로 볼지 정하는 기준일
```

이 두 개가 지금 시나리오의 핵심이야.

---

## 2026-07-07 — live study notes 용도

### Q
그냥 읽고 내가 질문 있는거 또 쫙 질문하고 그러면 또 답변하고 이런 느낌인거지 그냥 원본 그대로? 라이브 스터디 노트 자체가 그 용도로

### A
응. 정확히 그 용도야.

`live-study-notes.md`는 **정리된 문서가 아니라 공부 대화 원본을 쌓는 곳**이야.

흐름은 그냥 이렇게 가면 돼.

```text
1. 너가 문서를 읽는다.
2. 헷갈리는 걸 쭉 질문한다.
3. 내가 답한다.
4. 그 Q/A 원문을 live-study-notes.md에 그대로 붙인다.
5. 다시 읽는다.
6. 또 질문한다.
7. 반복한다.
```

정리하려고 하지 않아도 돼.
요약하지 않아도 돼.
예쁘게 만들 필요도 없어.

형식도 이 정도면 충분해.

```md
## 2026-07-07 — question map 읽기

### Q
<내 질문 원문>

### A
<답변 원문>
```

나중에 “아 이건 중요하다” 싶은 것만 따로 승격하면 돼.

```text
반복해서 나오는 개념
  -> scenario 문서

실제 선택으로 굳어진 것
  -> reference decision

테스트로 증명해야 하는 것
  -> test contract

면접에서 말할 문장
  -> README
```

그러니까 지금은 그냥:

> 읽고 → 묻고 → 답 듣고 → 원문 붙이고 → 또 묻기

이게 맞아.
`live-study-notes.md` 자체가 그 목적의 raw 공부 로그야.

---

## 2026-07-07 — business_date 의미와 table grain

### Q
기준일이라는게 여러 의미가 있구나 이벤트가 발생한 시각이랑 어느 날짜의 생산/품질 집계에 넣을지 파티션에 넣는다 재처리 단위구나 근데 데이터 마다 다를 수 있어? 아니면 이건 딱 하나로 고정되어야 해? gold에서? 좀 어렵네

### A
맞아. `기준일`은 데이터마다 의미가 달라질 수 있어. 그래서 더 헷갈리는 개념이야.

핵심은 이거야.

```text
기준일의 "값"은 row마다 다를 수 있다.
하지만 기준일의 "의미"는 한 table 안에서는 고정되어야 한다.
```

예를 들어 `gold_daily_metrics`가 있다면:

```text
business_date = 이 생산/품질 metric을 귀속할 날짜
```

라고 정했으면, 이 table 안에서는 계속 그 의미로 써야 해.

나쁜 경우:

```text
어떤 row의 business_date는 이벤트 발생일
어떤 row의 business_date는 적재일
어떤 row의 business_date는 생산 마감일
어떤 row의 business_date는 품질 검사일
```

이렇게 섞이면 안 돼. 그러면 partition, 재처리, quality, lineage가 다 헷갈려.

보통 날짜 컬럼은 여러 개가 있을 수 있어.

```text
event_time       = 실제 이벤트 발생 시각
ingested_at      = 우리 플랫폼에 들어온 시각
processed_at     = pipeline이 처리한 시각
business_date    = 업무/집계 기준일
```

제조 예시로 보면:

```text
event_time:
  로봇 이벤트가 실제 발생한 시각

business_date:
  이 이벤트를 어느 생산일/품질 집계일에 넣을지

ingested_at:
  CSV가 플랫폼에 들어온 시각

processed_at:
  silver/gold를 만든 시각
```

`gold`에서는 보통 하나의 기준일을 명확히 잡아야 해.

```text
gold_daily_metrics grain:
  business_date + plant_id + line_id + product_code
```

그러면 한 row의 의미는:

```text
특정 business_date의
특정 공장/라인/제품에 대한
일별 생산/불량 metric
```

이렇게 고정돼.

다만 다른 gold table이면 기준일 의미가 달라질 수 있어.

```text
gold_daily_production_metrics
  business_date = 생산 기준일

gold_daily_quality_metrics
  business_date = 품질 판정 기준일

gold_daily_settlement_metrics
  business_date = 정산 기준일
```

이렇게 table마다 다를 수는 있어. 하지만 그 경우에는 이름을 더 명확히 하는 게 좋아.

```text
production_date
quality_date
settlement_date
```

이 프로젝트에서는 단순하게 가는 게 좋아.

```text
business_date = 생산/품질 metric을 귀속할 업무 기준일
```

그리고 이걸:

```text
- gold grain의 일부
- Iceberg partition column
- 재처리 단위
- quality/lineage 조회 기준
```

으로 쓰는 거야.

한 줄로 정리하면:

> 기준일은 데이터셋마다 다를 수 있지만, 한 table 안에서는 의미가 고정되어야 한다.  
> 이 프로젝트의 `business_date`는 gold metric을 어느 날짜 결과에 넣을지 정하는 업무 기준일이다.

---

## 2026-07-07 — business_date와 backfill

### Q
아 그러면 business date로 한다면 그거는 백필이나 이런걸 위해서라도 꼭 gold 쪽에서 고정이 되어 있어야 하겠구나?

### A
맞아. 정확히 그거야.

`business_date`를 gold의 기준일로 쓰기로 했다면, 그 의미는 gold table에서 반드시 고정되어 있어야 해. 그래야 backfill, rerun, overwrite, quality, lineage가 모두 같은 단위로 움직일 수 있어.

예를 들어 gold table grain이 이렇게 정해졌다고 하자.

```text
gold_daily_metrics grain:
  business_date + plant_id + line_id + product_code
```

그러면 `business_date=2026-06-29` backfill은 이런 뜻이 돼.

```text
2026-06-29에 귀속되는 생산/품질 metric을 다시 계산한다.
gold_daily_metrics에서 business_date=2026-06-29 partition을 교체한다.
quality도 2026-06-29 기준으로 검증한다.
lineage도 이 날짜 결과가 어떤 source/run/snapshot에서 나왔는지 남긴다.
```

만약 `business_date` 의미가 섞이면 문제가 생긴다.

```text
어떤 row는 이벤트 발생일 기준
어떤 row는 적재일 기준
어떤 row는 품질 판정일 기준
```

이러면 `business_date=2026-06-29`를 backfill한다는 말이 애매해져.

```text
무슨 날짜를 다시 만든다는 거지?
어떤 partition을 overwrite해야 하지?
quality row count는 어떤 기준으로 비교하지?
이 gold 숫자는 어느 날짜 업무 결과지?
```

그래서 gold에서는 기준일 의미가 고정되어야 한다.

한 줄로 말하면:

```text
business_date는 gold의 partition column이기 전에,
gold row가 어느 업무 날짜의 사실인지를 정의하는 contract다.
```

그래서 backfill/reprocessing 관점에서는:

```text
business_date = 재처리 단위
business_date = partition overwrite 단위
business_date = quality 검증 단위
business_date = lineage 조회 단위
```

가 된다.

이 프로젝트에서는 단순하게 이렇게 고정하면 돼.

```text
business_date:
  manufacturing event를 생산/품질 일별 metric에 귀속할 업무 기준일.

gold_daily_metrics:
  business_date + plant_id + line_id + product_code 단위의 일별 metric.
```

그러면 "2026-06-29를 다시 돌린다"는 말이 명확해져.

```text
2026-06-29 gold partition을 다시 계산해서 원자적으로 교체한다.
```

---

## 2026-07-07 — business_date as quality/operation contract

### Q
metric을 다시 계산한다 오케이 재처리 단위이고 덮어쓰는 단위 리니지 조회단위 이런게 잘 구분되어야 하는구나이게 단순하다 이런걸 넘어서 이런게 잘 되야 데이터 품질 관리가 잘 되겠네? 원자적이라는 표현을 쓰는구나

### A
맞아. 지금 이해가 핵심으로 들어왔어.

`business_date`는 단순히 “날짜 컬럼 하나”가 아니야. 잘 정하면 데이터 품질과 운영을 묶어주는 기준축이 돼.

```text
business_date =
  재처리 단위
  overwrite 단위
  quality 검증 단위
  lineage 조회 단위
  gold mart grain의 일부
```

이게 잘 구분되어 있어야 데이터 품질 관리가 가능해져.

예를 들어 운영자가 묻는 질문이 있어.

```text
2026-06-29 생산 지표가 이상한데?
```

그러면 시스템은 같은 기준으로 답할 수 있어야 해.

```text
어떤 source가 2026-06-29 결과를 만들었나?
어떤 run_id가 만들었나?
어떤 silver/gold snapshot_id가 current인가?
quality check는 2026-06-29 기준으로 통과했나?
다시 계산하면 어떤 partition이 바뀌나?
이전 결과와 새 결과를 비교할 수 있나?
```

이 질문들이 모두 `business_date`를 중심으로 연결돼.

그래서 “단순하게 business_date로 partition한다”보다 더 정확한 말은:

```text
business_date를 gold의 업무 기준일 contract로 고정하고,
그 contract를 partition, backfill, quality, lineage에 같이 사용한다.
```

그리고 “원자적”이라는 표현은 여기서 중요해.

원자적이라는 말은:

```text
바뀌려면 전체가 성공적으로 바뀐다.
실패하면 이전 상태가 그대로 남는다.
중간 상태가 사용자에게 보이지 않는다.
```

데이터 파이프라인에서는 예를 들어:

```text
2026-06-29 gold partition을 overwrite하는 중에 실패했다.
```

이때 나쁜 상태는:

```text
row 일부는 새 결과
row 일부는 옛 결과
quality는 새 결과 기준
lineage는 옛 결과 기준
```

이런 반쪽 상태야.

Iceberg의 atomic commit은 이런 상태를 피하게 해준다.

```text
overwrite commit 성공:
  새 snapshot이 current가 된다.

overwrite commit 실패:
  이전 snapshot이 current로 남는다.
```

그래서 이 시나리오에서 Iceberg ACID를 설명할 때 핵심은 동시성보다 원자성이야.

```text
local v0라 multi-writer concurrency는 핵심이 아니다.
하지만 같은 business_date partition을 재처리할 때
반쪽 gold가 보이지 않게 하는 atomic overwrite는 설명 가치가 있다.
```

한 줄로 말하면:

> `business_date`는 품질/재처리/lineage의 기준축이고, Iceberg atomic overwrite는 그 기준축 단위의 교체를 반쪽 상태 없이 처리하게 해준다.

---

## 2026-07-07 — question quality drives design quality

### Q
어떤 소스가 어떤 run_id가 스냅샷 아이디 current? 인건 뭐지 아무튼 기준으로 통과했나 다시 계산 파티션? 6/29일이 바뀌어야 하지 않아? 새 겨로가 비교해야 할 수 있냐 바뀌려면 전체가 성공으로 실패하면 이전 상태 그대로 그렇구나 이런 아토믹 이런게 필요하구나 좋아 점점 이해된다 나는 그래서 결국 설계할 때 각각 어떤 질문들을 잘 뽑아낼 수 있느냐가 설계의 품질을 결정짓는다고 생각해

### A
맞아. 그게 거의 핵심이야.

설계 품질은 “처음부터 정답 아키텍처를 아는가”보다, **상황에서 좋은 질문을 뽑아낼 수 있는가**에 많이 달려 있어.

예를 들어 “같은 business_date를 다시 처리한다”라는 상황 하나에서도 질문이 쭉 나온다.

```text
같은 입력인가, 정정 입력인가?
append하면 중복되지 않나?
skip하면 정정 데이터를 못 반영하지 않나?
overwrite하면 이전 결과는 사라지나?
실패하면 반쪽 결과가 보이지 않나?
어떤 source가 이 결과를 만들었나?
어떤 run_id가 이 결과를 만들었나?
Iceberg snapshot_id는 무엇을 가리키나?
현재 사용자가 읽는 snapshot은 어느 것인가?
quality check는 어느 날짜/어느 snapshot 기준으로 통과했나?
다시 계산하면 어느 partition이 바뀌나?
이전 결과와 새 결과를 비교할 수 있나?
```

이 질문들이 없으면 그냥 이렇게 구현하기 쉽다.

```text
CSV 읽기
groupBy 하기
gold 파일 쓰기
```

그런데 그러면 나중에 운영 질문에 답을 못 한다.

```text
왜 숫자가 바뀌었지?
어느 입력 때문이지?
다시 돌려도 중복 안 생기나?
실패 중간에 사용자가 잘못된 값을 읽지 않나?
이전 결과로 비교 가능하나?
```

그래서 좋은 설계는 질문을 잘 뽑고, 그 질문 중에서 지금 v0가 책임질 것과 안 할 것을 나누는 과정이다.

```text
질문을 뽑는다.
Core / Demo / Backlog / Unknown으로 분류한다.
Core는 architecture decision으로 내린다.
Decision은 test contract로 증명한다.
Backlog는 안 한다고 명시한다.
```

이 프로젝트에서 지금 배우는 것도 Spark/Iceberg 기능 자체보다 이 사고 방식이다.

```text
business_date는 왜 필요한가?
partition은 왜 필요한가?
atomic overwrite는 어떤 문제를 막는가?
snapshot_id는 lineage와 time travel에서 어떤 역할인가?
source_hash와 run_id는 snapshot_id와 어떻게 다른가?
```

한 줄로 말하면:

> 설계는 기능을 고르는 일이 아니라, 상황에서 생기는 질문을 드러내고 그중 무엇을 책임질지 정하는 일이다.

---

## 2026-07-07 — 질문을 잘 뽑는 능력

### Q
맞아 지금 질문 뽑는게 잘 적혀있나?  예를 들어 “같은 business_date를 다시 처리한다” 하나만 해도:

  같은 입력인가, 정정 입력인가?
  append하면 중복되지 않나?
  skip하면 정정 데이터를 못 반영하지 않나?
  overwrite하면 이전 결과는 사라지나?
  실패하면 반쪽 결과가 보이지 않나?
  어떤 source가 이 결과를 만들었나?
  어떤 run_id가 이 결과를 만들었나?
  현재 사용자가 읽는 snapshot은 어느 것인가?
  quality check는 어느 날짜/어느 snapshot 기준으로 통과했나?
  이전 결과와 새 결과를 비교할 수 있나?

  이런 질문이 나와야 설계가 단단해져.

  그래서 네가 잡은 방향이 맞아.

  좋은 설계 =
    질문을 잘 뽑고
    Core / Demo / Backlog / Unknown으로 나누고
    Core는 decision으로 내리고
    test로 증명하고
    안 하는 것은 안 한다고 명시하는 것
 이런식으로 해가지고 그러니까 질문이 제일 중요하다 설계를 잘하기 위해서는 그 전에 질문을 잘 뽑고 그래야 한다 이런게 진짜 중요한거같은데 뭘 하더라도 기존 oss 분석하는것도 마찬가지고

### A
맞아. 이건 이번 프로젝트를 넘어서 전체 방법론의 핵심이야.

지금 프로젝트 문서 기준으로 보면:

```text
02-slice2-question-map.md
  질문을 넓게 펼치는 문서

scenarios/01-rerun-same-business-date.md
  "같은 business_date 재처리" 질문 묶음을 실제 상황으로 푸는 문서

reference-decisions/iceberg-write-semantics.md
  그 질문들 중 append/skip/overwrite/merge 결정을 수렴하는 문서
```

질문 자체는 꽤 잘 잡혀 있어. 특히 아래 질문들은 이미 question map/scenario에 들어가 있다.

```text
append / overwrite / merge 중 무엇인가?
Slice1의 skip을 무엇으로 대체하나?
partition 단위 overwrite가 idempotency를 어떻게 재현하나?
overwrite하면 이전 결과는 사라지나, snapshot으로 남나?
write 실패 시 이전 snapshot이 유지되나?
run_id와 snapshot_id는 대체가 아니라 참조 관계인가?
quality check는 Spark/Iceberg 위에서도 같은 contract를 지키나?
lineage에 silver_snapshot_id / gold_snapshot_id를 남기나?
```

다만 네가 방금 말한 메타 원칙:

```text
좋은 설계는 좋은 질문을 뽑는 능력에서 시작한다.
```

이건 프로젝트 문서에만 둘 게 아니라 workspace-level decision learning playbook에 기준으로 들어가야 한다. 그래야 OSS 분석이든 포트폴리오 설계든 같은 방식으로 진행할 수 있다.

기준 문장:

```text
설계는 기능을 고르는 일이 아니라,
상황에서 생기는 질문을 드러내고,
그중 무엇을 Core / Demo / Backlog / Unknown으로 둘지 결정하는 일이다.
```

OSS 분석에도 똑같이 적용된다.

```text
이 코드는 무엇을 하나?
```

보다 먼저:

```text
이 시스템은 어떤 질문에 답하려고 이런 상태/테이블/큐/정책을 만들었나?
```

를 봐야 한다.

---

## 2026-07-07 — question map audit process

### Q
`slices/spark-iceberg-partition-overwrite/01-question-map.md` 근데 이거 너무 빈약한거 아니야? 내가 볼때 질문을 잘 뽑는게 너 혼자 하는거면 안되고 클로드 피드백도 받아가면서 이걸 잘 작성해야 할거 같은데 이게 프로세스화 되어 있어야 할거 같거든?

### A
맞아. 이건 정확한 지적이야.

`02-slice2-question-map.md`가 완전히 빈약한 건 아니야. 이미 Spark/Iceberg Slice2에서 중요한 축은 들어가 있어.

```text
ACID / atomic commit
append / overwrite / merge
partitioning
shuffle
schema evolution
time travel
catalog integration
quality on Spark
lineage across snapshots
idempotency
local walking skeleton
testing
operability
```

하지만 문제는 **질문 품질을 검증하는 프로세스가 약하다**는 거야.

질문을 잘 뽑는 게 설계 품질을 결정한다면, question map은 한 번에 완성되는 문서가 아니어야 해.

```text
Codex/user 초안
-> Claude/외부 benchmark audit
-> 빠진 질문/과한 질문/과장 위험 반영
-> Core / Demo / Backlog / Unknown 확정
-> decision note로 수렴
```

이게 프로세스화되어야 한다.

Claude에게 맡길 일은 구현이 아니라 **question map audit**이야.

```text
구현 계획을 세우지 말고,
빠진 질문을 찾아라.
과한 질문을 Backlog로 내려라.
Core인데 빠진 질문을 올려라.
Demo를 production claim처럼 말하는 위험을 잡아라.
공식 문서/잘 만든 OSS 관점에서 blind spot을 찾아라.
```

그래서 기준본과 프로젝트 문서에 아래 단계를 추가했다.

```text
scenario seed
-> question map
-> question map audit / challenge
-> state trace / evidence
-> reference decision
-> test contract
-> implementation
```

수정한 문서:

```text
workspace-level decision learning playbook
learn/system-design/README.md
learn/system-design/slices/spark-iceberg-partition-overwrite/01-question-map.md
```

`02-slice2-question-map.md`에는 Claude audit prompt도 추가했다.

핵심 결론:

```text
question map은 Codex 단독 최종본이 아니다.
초안 -> 감사 -> 반박/수용 -> 분류 확정 -> decision으로 내려가는 산출물이다.
```

---

## 2026-07-08 — rerun questions status check

### Q
좋아 계속 진행해볼까? 같은 입력인가, 정정 입력인가?
append하면 중복되지 않나?
skip하면 정정 데이터를 못 반영하지 않나?
overwrite하면 이전 결과는 사라지나?
실패하면 반쪽 결과가 보이지 않나?
어떤 source가 이 결과를 만들었나?
어떤 run_id가 이 결과를 만들었나?
Iceberg snapshot_id는 무엇을 가리키나?
현재 사용자가 읽는 snapshot은 어느 것인가?
quality check는 어느 날짜/어느 snapshot 기준으로 통과했나?
다시 계산하면 어느 partition이 바뀌나?
이전 결과와 새 결과를 비교할 수 있나? 이런 질문에 대한 답을 내야 하나? 지금 어디까지 공부된거지?

### A
지금 이 질문들은 전부 한 번에 “최종 확정”해야 하는 질문은 아니다.

역할이 세 단계로 나뉜다.

```text
1. 이해해야 하는 질문
   용어와 문제가 무슨 뜻인지 알아야 한다.

2. decision으로 답해야 하는 질문
   v0 architecture가 어떻게 행동할지 정해야 한다.

3. test/walking skeleton으로 확인해야 하는 질문
   말로는 답했지만 실제 Spark/Iceberg에서 되는지 검증해야 한다.
```

현재 공부된 상태:

| 질문 | 현재 상태 | 답 후보 |
|---|---|---|
| 같은 입력인가, 정정 입력인가? | 공부됨 / decision 후보 있음 | `source_hash`가 같으면 같은 입력, 다르면 정정/변경 입력으로 본다 |
| append하면 중복되지 않나? | 공부됨 | gold에는 append를 피한다. 같은 날짜 metric이 중복될 수 있다 |
| skip하면 정정 데이터를 못 반영하지 않나? | 공부됨 | 맞다. 그래서 same source는 skip, changed source는 overwrite로 간다 |
| overwrite하면 이전 결과는 사라지나? | 부분 답변됨 | current table에서는 교체되지만 Iceberg snapshot history에는 남는다 |
| 실패하면 반쪽 결과가 보이지 않나? | 개념 답변됨 / 구현 검증 필요 | Iceberg atomic commit이면 실패 시 이전 snapshot이 current로 남아야 한다 |
| 어떤 source가 이 결과를 만들었나? | decision 후보 있음 | `source_hash`를 run metadata에 남긴다 |
| 어떤 run_id가 이 결과를 만들었나? | decision 후보 있음 | `lakehouse_runs.run_id`가 pipeline 실행 단위로 남는다 |
| Iceberg snapshot_id는 무엇을 가리키나? | 공부됨 | Iceberg table commit 단위다. run_id를 대체하지 않고 run이 참조한다 |
| 현재 사용자가 읽는 snapshot은 어느 것인가? | 개념 답변됨 / 구현 확인 필요 | Iceberg table의 current snapshot이다 |
| quality check는 어느 날짜/어느 snapshot 기준으로 통과했나? | 아직 더 설계 필요 | `business_date`와 `gold_snapshot_id` 기준으로 묶어야 한다 |
| 다시 계산하면 어느 partition이 바뀌나? | 공부됨 | `business_date=D` partition만 바뀐다 |
| 이전 결과와 새 결과를 비교할 수 있나? | demo 후보 있음 | `VERSION AS OF`로 S1/S2 snapshot 비교를 test로 보인다 |

요약:

```text
이미 이해한 것:
  append / skip / overwrite / merge의 차이
  business_date가 재처리/partition/quality/lineage 기준축이라는 점
  run_id != snapshot_id
  atomic overwrite가 반쪽 결과를 막는다는 점

decision 후보까지 나온 것:
  hybrid idempotency
  same source_hash -> skip
  different source_hash with same business_date -> partition overwrite
  run metadata에 source_hash, run_id, snapshot_id 기록

아직 확인/보강할 것:
  Iceberg overwrite API가 실제 local Spark에서 어떻게 되는지
  quality를 commit 전/후 어느 시점에 계산할지
  current snapshot id를 어떻게 읽고 metadata에 저장할지
  Claude audit으로 빠진 질문이 없는지 확인
```

따라서 지금은 “질문에 대한 답을 전부 끝낸 상태”가 아니라:

```text
핵심 질문의 의미를 이해했고,
v0 decision 후보가 생겼고,
이제 audit + walking skeleton + test contract로 검증할 단계
```

라고 보면 된다.

---

## 2026-07-08 — Iceberg 이해 수준과 공부됨 기준

### Q
나 근데 iceberg 하나도 이해못했는데 그리고 지금 공부됨 기준이 뭔지 모르겠네 iceberg 공부를 좀 더 해야되나?

### A
맞아. 여기서 "공부됨"이라는 말을 조심해야 해.

지금 상태는:

```text
Iceberg를 이해했다
```

가 아니라:

```text
같은 business_date 재처리 시나리오에서
왜 Iceberg의 snapshot / atomic commit / partition overwrite 같은 질문이 나오는지
대략 감을 잡기 시작했다
```

에 가까워.

즉 공부 수준을 나누면 이렇게 봐야 한다.

```text
Level 0. 단어만 들음
  Iceberg, snapshot, partition, catalog 같은 말이 낯설다.

Level 1. 이 프로젝트에서 왜 필요한지 안다
  같은 날짜 재처리에서 append/skip/overwrite 문제가 생기고,
  Iceberg가 snapshot과 atomic commit으로 이 문제를 다룬다는 그림을 안다.

Level 2. 작은 예제로 설명할 수 있다
  table 생성 -> insert -> overwrite -> snapshot 확인 -> time travel 읽기를
  로컬 Spark에서 한 번 돌려보고 설명할 수 있다.

Level 3. 구현에 적용할 수 있다
  silver/gold partition overwrite, snapshot_id 기록, quality/lineage 연결을 코드와 테스트로 만든다.

Level 4. 운영 이슈까지 안다
  compaction, snapshot retention, concurrent writers, catalog 운영 등을 안다.
  v0 목표는 아님.
```

지금 우리는 Level 1 초입이다.

그래서 Iceberg 공부를 더 해야 한다. 다만 깊게 전부 공부할 필요는 없고, Slice2에 필요한 최소 개념만 먼저 보면 된다.

최소 공부 질문:

```text
1. Iceberg table은 그냥 Parquet 파일 폴더와 무엇이 다른가?
2. snapshot은 무엇이고 언제 생기나?
3. current snapshot은 무엇인가?
4. atomic commit은 무슨 뜻인가?
5. partition overwrite는 append와 무엇이 다른가?
6. catalog는 무엇을 저장하나?
7. time travel은 snapshot을 어떻게 읽는 것인가?
8. Spark는 Iceberg table에 어떻게 write/read하나?
```

지금 당장 목표는 "Iceberg 전문가"가 아니라:

```text
같은 business_date 재처리 시나리오를
Iceberg table / snapshot / partition overwrite / atomic commit으로
5분 설명할 수 있는 수준
```

이다.

그래서 다음 공부 순서는 이렇게 잡으면 된다.

```text
1. Iceberg 개념 미니 프라이머 작성
   - table, data files, metadata, manifest, snapshot, catalog

2. 같은 business_date 재처리 시나리오에 다시 연결
   - overwrite하면 어떤 snapshot이 생기는가?
   - current snapshot은 무엇인가?
   - 이전 결과는 어떻게 읽는가?

3. walking skeleton
   - local Spark + Iceberg table 하나 만들기
   - insert
   - overwrite
   - snapshots 확인
   - VERSION AS OF 읽기
```

즉 "공부됨" 기준은 이렇게 바꿔야 한다.

```text
공부됨 = 개념을 외웠다
```

가 아니라:

```text
공부됨 = 이 프로젝트의 시나리오에서
그 개념이 어떤 문제를 해결하는지 말할 수 있고,
작은 테스트로 확인할 수 있다
```

아직 Iceberg는 공부 시작 단계다. 그래서 다음에는 Iceberg primer를 먼저 보는 게 맞다.

---

## 2026-07-08 — Iceberg/Spark official docs and hands-on need

### Q
https://iceberg.apache.org/ 이거나 https://spark.apache.org/ 이런거를 좀 직접 다뤄봐야 하나? 너무 정보가 없는 상태에서 뭘 하려는건지 모르겠네

### A
응. 지금은 공식 문서와 직접 실행을 같이 해야 한다.

다만 Apache Iceberg/Spark 사이트를 처음부터 끝까지 읽는 방식은 비효율적이다. 지금 목표는 "Iceberg 전문가"가 아니라 `business_date` 재처리 시나리오를 설명하고 구현할 최소 이해를 만드는 것이다.

볼 범위:

```text
Iceberg:
  table format이 뭔가
  snapshot이 뭔가
  atomic commit이 뭔가
  partition overwrite가 뭔가
  Spark에서 Iceberg table을 어떻게 read/write하나

Spark:
  DataFrame이 뭔가
  Spark SQL이 뭔가
  local mode에서 table read/write를 어떻게 하나
  groupBy/shuffle이 왜 생기나
```

공식 문서에서 먼저 볼 것:

```text
1. Iceberg home / concepts
   Iceberg가 analytic table format이고 snapshot, schema evolution, hidden partitioning, time travel을 제공한다는 큰 그림.

2. Iceberg Spark getting started
   SparkSession catalog 설정, table create, write, read.

3. Iceberg Spark writes
   append, overwrite, dynamic partition overwrite, DataFrameWriterV2/SQL write semantics.

4. Iceberg reliability / partitioning
   atomic commit, snapshot/current snapshot, hidden partitioning.

5. Spark SQL/DataFrame guide
   DataFrame, SQL, action/transformation, groupBy 같은 기본 실행 모델.
```

직접 다뤄볼 최소 실습:

```text
1. local SparkSession을 띄운다.
2. Iceberg catalog/warehouse를 설정한다.
3. gold_daily_metrics table 하나를 만든다.
4. business_date=2026-06-29 rows를 insert한다.
5. 같은 business_date를 overwrite한다.
6. snapshots/history metadata를 본다.
7. 이전 snapshot과 최신 snapshot을 비교한다.
```

이 실습을 해야 아래 말이 감으로 잡힌다.

```text
snapshot_id
current snapshot
atomic commit
partition overwrite
time travel
```

지금 단계 결론:

```text
공식 문서 읽기만 하면 추상적이다.
바로 구현부터 하면 의미를 모른다.

그래서 "공식 문서 최소 범위 읽기 -> mini primer 작성 -> walking skeleton 실행" 순서가 맞다.
```

다음 산출물 후보:

```text
learn/system-design/05-iceberg-spark-mini-primer.md
```

목적:

```text
Iceberg/Spark를 전부 설명하는 문서가 아니라,
Slice2에서 필요한 개념만 business_date 재처리 시나리오에 연결한다.
```

---

## 2026-07-08 — Iceberg/Spark primer와 블로그 연결

### Q
이런거 관련해서도 블로그 및 지금 프로젝트랑 연관지어서 뭔가 해볼 수 있나?

### A
가능하다. 다만 "Iceberg/Spark를 공부했다"가 아니라, 현재 프로젝트의 `business_date` 재처리 문제를 Spark/Iceberg 용어로 다시 표현하는 산출물이어야 한다.

연결 방식:

```text
현재 프로젝트 Slice1:
  dataset_id + business_date + source_hash 가 같으면 skip

남은 질문:
  정정된 파일이면 skip하면 안 된다.
  append하면 중복된다.
  overwrite하면 다른 날짜까지 건드릴 수 있다.

Slice2 / Iceberg 방향:
  같은 source_hash면 skip
  다른 source_hash면 해당 business_date partition만 atomic overwrite
  overwrite 전후 snapshot을 비교
  run_id는 유지하고, run metadata가 gold_snapshot_id를 참조
```

그래서 산출물은 세 단계가 좋다.

```text
1. learn/system-design/05-iceberg-spark-mini-primer.md
   공식 문서 최소 범위와 용어를 business_date 재처리 시나리오에 연결한다.

2. local Spark/Iceberg walking skeleton
   gold_daily_metrics table 하나로 insert -> partition overwrite -> snapshot/history 확인.

3. 블로그
   "skip에서 partition overwrite로: business_date 재처리를 Iceberg로 다시 표현하기"
```

이력서/블로그 claim은 walking skeleton 전후로 다르게 둬야 한다.

```text
walking skeleton 전:
  Designed the Spark/Iceberg translation path for a synthetic medallion pipeline.

walking skeleton 후:
  Built a local Spark/Iceberg walking skeleton that rewrites a business_date partition
  and records snapshot metadata for reproducible rerun comparison.
```

중요한 claim boundary:

```text
말해도 됨:
  local Spark/Iceberg walking skeleton
  business_date partition overwrite
  snapshot metadata comparison
  run_id references snapshot_id

말하면 안 됨:
  production lakehouse
  Spark cluster 운영
  Kafka streaming
  production rollback/restore 운영
  concurrent writer 처리
```

---

## 2026-07-08 — 프로젝트 자체가 어렵게 느껴짐

### Q
솔직히 안다뤄봐서 너무 어려워 그리고 robo data platform이 어떻게 설계되었고 이런게 잘 감이 안와서 이것도 좀 어렵고

### A
맞다. 지금 어려운 이유는 이해력이 부족해서가 아니라, 한 번에 두 층을 보고 있기 때문이다.

```text
1층: manufacturing-data-platform-mini 자체가 무엇을 하는가?
2층: 그걸 Spark/Iceberg로 어떻게 다시 표현하는가?
```

아직 1층이 몸에 안 붙었는데 2층 용어를 보면 당연히 어렵다.

그래서 지금은 Spark/Iceberg를 잠깐 멈추고, 프로젝트를 이렇게 낮춰서 봐야 한다.

```text
파일 하나가 들어온다.
그 파일에서 지표를 만든다.
그 지표가 믿을 만한지 검사한다.
나중에 "이 숫자가 어디서 왔지?"를 설명할 기록을 남긴다.
같은 파일을 다시 돌렸을 때 중복되지 않게 한다.
```

이게 이 프로젝트의 핵심이다.

먼저 붙어야 하는 단어는 네 개다.

```text
source_hash
business_date
quality check
lineage
```

이 네 개를 설명할 수 있으면 Spark/Iceberg는 나중에 붙는다.

새 입문 문서:

```text
learn/system-design/00a-plain-project-map.md
```

공부 순서:

```text
1. 00a-plain-project-map.md
2. README Phase 1 / Phase 2
3. VERIFICATION_LOG 2026-07-08
4. source_hash / business_date / quality / lineage 네 단어 설명
5. 그 다음 04-slice2-spark-iceberg-shift.md
6. 마지막에 05-iceberg-spark-mini-primer.md
```

지금은 Iceberg 단어를 외울 단계가 아니다. 먼저 아래 문장을 말할 수 있으면 된다.

> 이 프로젝트는 로봇/제조 비슷한 synthetic CSV를 받아서 bronze/silver/gold 지표를 만들고, 그 결과가 어느 입력에서 왔는지, 품질검사를 통과했는지, 같은 입력을 다시 돌렸을 때 중복되지 않는지를 기록하는 작은 데이터 플랫폼입니다.

---

## 2026-07-09 — 00-service-purpose-charter 시작

### Q
좋아 그러면 시작해보자

### A
첫 문서 `00-service-purpose-charter.md`의 핵심은 이거다.

```text
이 프로젝트는 CSV를 읽어서 gold 숫자를 만드는 도구가 아니다.
raw file을 믿고 쓸 수 있는 dataset/mart로 바꾸고,
나중에 운영자가 그 숫자의 근거를 설명할 수 있게 증거를 남기는 작은 데이터 플랫폼이다.
```

이 문서가 답하는 질문은 크게 세 개다.

```text
1. 이 서비스는 왜 존재하는가?
2. 누가 이 서비스를 쓰고, 무엇을 알고 싶어 하는가?
3. 어떤 상태/증거를 남겨야 그 질문에 답할 수 있는가?
```

## 1. 왜 존재하는가

raw file만 있으면 사용자는 이런 걸 모른다.

```text
이 파일이 전에 처리한 파일과 같은가?
schema가 바뀌었는가?
이 날짜 gold 결과는 어느 source에서 왔는가?
quality check는 통과했는가?
같은 날짜를 다시 돌렸을 때 중복이 생기지 않았는가?
```

그래서 이 프로젝트는 raw file을 바로 "쓸 수 있는 데이터"라고 주장하지 않는다.

대신 아래 evidence를 만든다.

```text
source_hash
schema_hash
bronze / silver / gold
quality result
catalog/version metadata
lineage/run evidence
idempotent rerun evidence
```

## 2. 누가 쓰는가

주요 actor는 이렇게 나뉜다.

```text
Source owner:
  synthetic manufacturing CSV를 제공한다.

Data engineer / operator:
  run 성공/실패, 품질, 원인 추적을 본다.

Analyst / data user:
  gold metric을 믿고 써도 되는지 판단한다.

ML / manufacturing data user:
  어떤 dataset version으로 학습/평가했는지 재현하고 싶다.

Interviewer / reviewer:
  README claim이 실제 code/test evidence와 맞는지 본다.
```

중요한 점: interviewer/reviewer도 actor로 들어간다. 포트폴리오 프로젝트이기 때문에 “말한 것과 구현한 것이 맞는가”도 시스템 목적의 일부다.

## 3. 어떤 상태를 만드는가

이 문서에서 가장 중요한 표는 `States The Service Must Create`다.

```text
source identity   -> source_hash
schema identity   -> schema_hash / schema_drift
bronze            -> raw 보존
silver            -> typed/deduped common data
gold              -> 사용자가 보는 mart
quality result    -> publish/use 판단 근거
run/catalog record -> 성공/실패/버전 inspect
lineage evidence  -> input -> output 원인 추적
```

즉 이 프로젝트는 transform 결과뿐 아니라, 결과를 믿을지 판단하는 증거를 같이 만든다.

## 4. 지금 잡아야 할 한 문장

```text
manufacturing-data-platform-mini는 synthetic manufacturing CSV를 bronze/silver/gold dataset으로 만들고,
source_hash, schema_hash, quality result, run record, lineage evidence를 남겨
data user와 operator가 gold 숫자를 믿고 추적할 수 있게 하는 작은 데이터 플랫폼이다.
```

## 5. 지금 외울 필요 없는 것

아직 Spark/Iceberg는 뒤다.

지금은 아래 네 단어가 먼저다.

```text
source_hash:
  같은 입력인지 판단한다.

schema_hash:
  source 구조가 바뀌었는지 판단한다.

quality result:
  이 결과를 믿고 써도 되는지 판단한다.

lineage/run evidence:
  이 숫자가 어느 입력과 실행에서 왔는지 추적한다.
```

다음으로 볼 문서는 `00a-plain-project-map.md`다. 그 문서는 이 charter를 더 쉽게 풀어주는 지도 역할이다.

---

## 2026-07-09 — service scope and v0 boundary

### Q
그러면 꼭 공장 데이트 플랫폼은 아니네? 아 근데 들어오는게 csv로 올테고 그러면 누가 쓰는지 품질 원인 추적 애널리스트가 있고 그렇구나 학습 평가 이런것도 만들 수 있구나 transform 결과뿐만 아니라 그렇구나 dataset 만들고 schema hash  좋아 일단 스키마를 그러면 정의해놔야 겠네 분석 ML도 분치려고 하는거네  버전도 있고 row가 사라졌는가 숫자가 어디서 왔는지 source format 어떻게 온보딩 8. v0 Boundary
Now / implemented:

synthetic CSV ingest/catalog path with mongomock tests
Slice1 bronze/silver/gold pipeline
dbt-style quality checks
schema drift warning
source_hash idempotency
EAV multi-format mapping
JSON CLI smoke runs
Backlog / design-only:

real Mongo runtime verification
Airflow runtime trigger verification
Spark/Iceberg implementation
Kafka streaming
ROS2/MCAP ingest
column-level lineage
production governance UI
Explicitly not claiming:

production manufacturing data platform
Spark/Iceberg implemented
Kafka streaming implemented
real Mongo runtime verified
Airflow runtime operated
real company/customer schema usage 이런것도 알아야 하는거지?

### A
맞아. 정확히 봤어.

이 프로젝트는 **진짜 공장 운영 플랫폼**이라고 주장하면 안 된다.

더 정확한 표현은:

```text
synthetic manufacturing-style/tabular data를 쓰는
작은 데이터 플랫폼 포트폴리오 프로젝트
```

이다.

즉 "제조 도메인"은 현실감을 주는 예시 도메인이고, 핵심은 공장 자체가 아니라 데이터 플랫폼 판단이다.

```text
source identity
schema identity
quality checks
bronze/silver/gold states
gold grain
lineage
catalog/version metadata
safe rerun
```

## 누가 쓰는가

네가 말한 actor들이 맞다.

```text
Analyst:
  gold metric을 믿고 분석해도 되는지 본다.

ML / data user:
  어떤 dataset version으로 학습/평가했는지 재현하고 싶다.

Operator / data engineer:
  run 성공/실패, 품질 실패, 숫자 이상 원인을 추적한다.

Reviewer / interviewer:
  README에 쓴 claim이 실제 code/test evidence와 맞는지 본다.
```

그래서 transform 결과만 있으면 부족하다.

```text
gold 숫자
```

만 만드는 게 아니라:

```text
이 gold 숫자가 어느 source에서 왔는가?
어떤 schema였는가?
어떤 run_id가 만들었는가?
quality는 통과했는가?
row가 어디서 줄었는가?
같은 입력을 다시 돌려도 중복이 안 생기는가?
```

를 답할 수 있어야 한다.

## schema를 정의해야 하는가

응. 최소한 source contract는 있어야 한다.

여기서 schema 정의는 거창한 schema registry가 아니라:

```text
required columns
column names
types/cast expectations
natural key
business_date 의미
accepted values
range expectations
```

정도다.

이게 있어야 `schema_hash`도 의미가 생긴다.

```text
schema_hash:
  source header/structure identity

schema_drift:
  이전 schema와 달라졌는지 warning/failure로 남기는 quality signal
```

## v0 Boundary를 알아야 하는가

응. 아주 중요하다.

`v0 Boundary`는 이 프로젝트가 무엇을 **했는지**, 무엇은 **아직 안 했는지**, 무엇은 **절대 주장하면 안 되는지**를 나누는 장치다.

Now / implemented:

```text
실제로 코드와 테스트로 증명된 것
README/면접에서 말해도 되는 것
```

Backlog / design-only:

```text
설계상 중요하지만 아직 구현하지 않은 것
다음 slice 후보
말할 때 "planned/backlog"라고 해야 하는 것
```

Explicitly not claiming:

```text
절대 구현한 척하면 안 되는 것
이력서/README/면접에서 과장하면 안 되는 것
```

예를 들어 지금 말해도 되는 건:

```text
synthetic CSV 기반 medallion pipeline을 구현했다.
quality checks, schema drift warning, source_hash idempotency를 테스트했다.
EAV multi-format mapping과 JSON CLI smoke run이 있다.
```

말하면 안 되는 건:

```text
production manufacturing data platform을 운영했다.
Spark/Iceberg pipeline을 구현했다.   # 아직 backlog/design-only
Kafka streaming을 구현했다.
Airflow를 운영했다.
real company data/schema를 썼다.
```

## 왜 이게 중요한가

포트폴리오 프로젝트에서는 구현보다 더 위험한 게 overclaim이다.

```text
작게 구현했는데 크게 말하면 신뢰가 깨진다.
작게 구현하고 정확히 말하면 오히려 설계 판단이 보인다.
```

그래서 이 charter의 `v0 Boundary`는 단순 참고가 아니라, README/블로그/면접 claim을 통제하는 안전장치다.

한 줄로 말하면:

> 이 프로젝트는 production 공장 플랫폼이 아니라, synthetic manufacturing-style CSV로 데이터 플랫폼의 핵심 루프(identity, quality, lineage, reproducibility, rerun safety)를 작게 증명하는 프로젝트다.

---

## 2026-07-09 — 00a plain project map

### Q
좋아 같이 한번 봐보자

### A
좋아. `00a-plain-project-map.md`는 훨씬 쉬운 지도야.

이 문서의 핵심은:

```text
CSV 읽기 -> 합계 계산 -> 결과 저장
```

이게 아니라,

```text
파일 하나가 들어온다.
그 파일에서 지표를 만든다.
그 지표가 믿을 만한지 검사한다.
나중에 "이 숫자가 어디서 왔지?"를 설명할 기록을 남긴다.
같은 파일을 다시 돌렸을 때 중복되지 않게 한다.
```

이걸 이해하는 거야.

**CSV 스크립트와 mini data platform의 차이**

단순 CSV 스크립트는:

```text
CSV 읽기 -> 합계 계산 -> 결과 CSV 저장
```

여기서 끝나.

근데 운영자가 이렇게 물으면 답하기 어려워.

```text
이 결과는 어느 원본 파일에서 왔나?
같은 파일을 다시 돌리면 중복되나?
원본 schema가 바뀌었나?
row가 처리 중 사라지지 않았나?
어느 날짜 기준 집계인가?
품질검사는 통과했나?
이전 run과 지금 run이 다른 이유는 뭔가?
```

이 프로젝트는 바로 이 질문에 답하려고 만든 거야.

**Slice1 흐름**

```text
raw CSV
-> bronze
   원본에 가까운 복사본 + manifest

-> silver
   business_date 필터
   natural key dedup
   타입 변환

-> gold
   일별 line/product metric
   units, defects, defect_rate, avg_cycle_time

-> quality
   row count reconciliation
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

한 줄로는:

```text
raw file -> trusted metric -> why/trust/retry evidence
```

**지금 붙여야 하는 4단어**

```text
source_hash:
  원본 파일 내용의 지문.
  같은 파일을 다시 돌렸는지 판단.

business_date:
  지표를 어느 날짜에 귀속할지.
  retry/backfill/partition 기준.

quality:
  결과를 믿을 수 있는지 검사.
  not_null, unique, conservation 같은 체크.

lineage:
  무엇이 무엇에서 왔는지.
  bronze -> silver -> gold parent links.
```

**EAV mini slice는 왜 있나**

실무 데이터는 컬럼명이 매번 다를 수 있어.

```text
plant_a.csv:
  설비ID, 생산수량, 불량수

plant_b.csv:
  equipment_id, units, defects
```

그래서 매번 코드를 고치는 대신:

```text
wide CSV
-> mapping config(JSON)
-> EAV long format
-> gold metric mart
```

로 가는 작은 slice를 추가한 거야.

핵심 claim은:

```text
새 형식이 오면 pipeline code를 바꾸는 게 아니라 mapping config를 추가한다.
```

**지금 단계에서 말할 수 있어야 하는 문장**

```text
이 프로젝트는 manufacturing-style synthetic CSV를 받아서 bronze/silver/gold 지표를 만들고,
그 결과가 어느 입력에서 왔는지, 품질검사를 통과했는지,
같은 입력을 다시 돌렸을 때 중복되지 않는지를 기록하는 작은 데이터 플랫폼입니다.
```

다음으로 네가 이해했는지 확인할 질문은 이거야.

```text
source_hash, business_date, quality, lineage를 각각 한 문장으로 설명할 수 있나?
```

이 4개가 붙으면 다음 문서 `01-scenario-seed.md`로 넘어가면 된다.

---

## 2026-07-09 — live notes append check

### Q
`learn/system-design/live-study-notes.md` 여기다 안쓰고 있어?

### A
맞아요. 방금 `00a` 설명은 답변만 하고 `live-study-notes.md`에 붙이지 않았습니다. 플레이북 기준이면 붙였어야 하니 지금 바로 원문 Q/A 형식으로 추가하겠습니다.

---

## 2026-07-09 — Live Study Mode automation question

### Q
이거 근데 너무 늘어나면 너가 계속 붙여넣기 어려워져? 아니면 그런 기능 하나 추가 못하나? 어떤 모드 들어가면 여기서 너랑 Q/A한거 있잖아 이 세션에서 그냥 자동으로 붙여넣기 되도록

### A
완전 자동으로 “대화가 생성될 때마다 백그라운드 훅이 실행”되게 하기는 어렵다.

하지만 실사용 기준으로는 거의 같은 효과를 내는 `Live Study Mode`를 만들 수 있다.

```text
Live Study Mode로 진행하자.
```

라고 말하면:

```text
1. 대상 프로젝트의 live-study-notes.md를 찾는다.
2. 사용자의 개념/설계/문서 이해 질문에 답한다.
3. 답변 직후 Q/A 원문을 live-study-notes.md에 append한다.
4. 요약/정리하지 않는다.
5. 다음 질문으로 계속 간다.
```

한계는 있다.

```text
대화 시스템에 진짜 백그라운드 자동 저장 훅이 있는 것은 아니다.
에이전트가 답변 후 파일에 append하는 운영 모드다.
```

그래도 사용자는 매번 "노트에 붙여줘"라고 말할 필요가 없다. 새 세션에서는 session bootstrap이나 workspace-level decision learning playbook을 읽게 하고, Live Study Mode를 켜면 된다.

이 규칙을 아래 문서에 추가했다.

```text
workspace-level decision learning playbook
workspace-level session bootstrap
```

---

## 2026-07-09 — automatic transcript append limits

### Q
그러니까 토큰 안쓰고 그렇게 못해? 내가 궁금한거는 여기 세션 대화 있잖아 그거를 그냥 자동으로 코드처럶 붙여넣기 한데?

### A
정확히 말하면, **토큰을 전혀 안 쓰고 현재 세션 대화를 자동으로 읽어서 파일에 붙이는 건 내가 직접 할 수 없다.**

이유:

```text
나는 대화 UI의 전체 transcript 파일이나 내부 로그에 직접 접근하는 기능이 없다.
내가 파일에 쓰려면, 답변 내용을 다시 생성하거나 도구 호출로 append해야 한다.
그 과정은 결국 토큰/도구 사용을 쓴다.
```

가능한 방식은 세 가지다.

```text
1. 현재 방식
   답변 후 내가 live-study-notes.md에 Q/A를 append한다.
   장점: 바로 정리됨.
   단점: 토큰과 도구 호출이 든다.

2. 세션 끝에 한 번에 정리
   대화 중에는 기록하지 않고,
   마지막에 "오늘 Q/A를 live notes에 정리해줘"라고 한다.
   장점: 중간 append 비용이 줄어든다.
   단점: 중간중간 파일 보면서 공부하기 어렵다.

3. 사용자가 transcript를 export/copy해서 파일에 붙임
   만약 UI가 세션 export를 제공하면 그걸 저장한다.
   장점: 모델이 다시 쓰는 비용이 적다.
   단점: 자동화는 UI/도구 지원에 달려 있다.
```

내가 만들 수 있는 보조 도구는 있다.

```text
scripts/append_live_note.py
```

같은 스크립트를 만들어서, 사용자가 복사한 Q/A 텍스트를 stdin으로 넣으면 live-study-notes.md에 붙이게 할 수 있다.

하지만 그 스크립트도 **현재 채팅 UI의 대화를 자동으로 몰래 가져오지는 못한다.** 입력은 누군가가 넘겨줘야 한다.

결론:

```text
완전 자동 transcript sync:
  현재 환경에서는 불가.

실사용 대안:
  Live Study Mode로 내가 답변 후 append하거나,
  세션 끝에 한 번에 append하거나,
  사용자가 export/copy한 transcript를 append helper로 넣는다.
```

---

## 2026-07-09 — resume context vs transcript file

### Q
왜냐면 resume하면 이 세션 불러와지잖아 이거 메모장이나 이런곳에 있는거 아니야?

### A
그렇게 느껴지는 게 자연스럽다. 하지만 내가 접근하는 방식은 “내가 어떤 메모장 파일을 열어서 transcript를 읽는 것”과는 다르다.

보통 resume/continuation에서는 시스템이 이전 대화 일부나 요약된 컨텍스트를 모델 입력으로 다시 넣어준다.

```text
사용자/assistant 대화
-> 시스템이 컨텍스트로 다시 제공
-> 내가 그 컨텍스트를 보고 이어서 답함
```

하지만 그 컨텍스트가 내 파일시스템에:

```text
session-transcript.txt
chat-log.md
conversation.json
```

같은 형태로 노출되어 있는 것은 아니다.

그래서 나는:

```text
이전 대화를 참고해서 답변할 수는 있음
```

하지만:

```text
현재 세션 전체 원문 transcript 파일을 찾아서 자동으로 live-study-notes.md에 복사
```

는 할 수 없다.

차이는 이거다.

```text
resume context:
  모델에게 읽히는 입력 컨텍스트.
  내가 답변에 참고할 수 있음.
  파일 경로로 접근하거나 raw export할 수 있는 것은 아님.

live-study-notes.md:
  실제 repo 안의 markdown 파일.
  내가 도구로 append 가능.
  git diff/commit/push 가능.
```

즉 resume은 “기억이 이어지는 것처럼 보이게 컨텍스트가 주입되는 것”에 가깝고, live-study-notes는 “명시적으로 파일에 저장되는 학습 로그”다.

그래서 자동 transcript sync가 어렵다.

```text
내가 보는 context != 내가 읽을 수 있는 로컬 transcript 파일
```

현실적인 운영은 계속 세 가지다.

```text
1. Live Study Mode:
   답변 후 내가 Q/A를 파일에 append.

2. 세션 끝 정리:
   오늘 대화 중 중요한 Q/A를 한 번에 append.

3. 사용자가 UI에서 transcript를 복사/export:
   그 텍스트를 파일에 붙이거나 append helper에 넣음.
```

---

## 2026-07-09 — demand for AI chat transcript logging

### Q
그리고 이런 거에 대해서 사람들 수요가 있지 않아? 궁금한데 이런 기능을 만든 oss나 이런 프로젝트는 없나?

### A
응. 수요는 확실히 있어 보인다.

다만 시장/OSS를 보면 두 층으로 나뉜다.

```text
1. 이미 흔한 영역:
   ChatGPT/Claude/Gemini 대화를 Markdown/JSON/PDF로 export/import/backup.

2. 아직 더 좁고 덜 제품화된 영역:
   개발/학습 세션 중 Q/A를 프로젝트 repo의 live-study-notes.md 같은 파일에
   자동으로 누적하고, 나중에 scenario/decision/test/README로 승격하는 workflow.
```

이미 있는 사례:

```text
LibreChat:
  open-source AI chat app.
  대화 import/export를 지원하고, GitHub README에는 markdown/text/json export가 언급된다.

Claude Chat Exporter:
  Claude 대화를 Markdown으로 export하는 커뮤니티 OSS.

Convoviz / chatgpt-history-export-to-md 계열:
  ChatGPT 공식 export ZIP/JSON을 Markdown 파일로 변환.
  Obsidian 같은 local knowledge base로 넣는 용도.

Continue.dev:
  coding assistant 쪽에서도 chat history/export markdown 요청 issue가 있었다.

Obsidian / Chrome extension 생태계:
  ChatGPT/Claude 대화를 Obsidian Markdown으로 저장하거나 import하려는 플러그인/확장들이 있다.

OpenAI/Google AI Studio community:
  Markdown/PDF/JSON export 요청이나 커뮤니티 exporter가 있다.
```

수요가 있는 이유:

```text
AI 대화가 연구 노트/설계 결정/코드 설명/학습 기록이 되는데,
채팅 UI 안에만 있으면 검색, git 관리, 재사용, 인용, 회고가 어렵다.
Markdown은 Obsidian/Notion/GitHub/VS Code와 잘 맞는다.
```

특히 너의 use case는 일반 export보다 더 구체적이다.

```text
그냥 대화 백업:
  오늘 한 AI 대화를 Markdown으로 저장한다.

너의 workflow:
  공부 질문과 답변을 live-study-notes.md에 쌓는다.
  중요한 내용은 scenarios/reference-decisions/tests/README로 승격한다.
  프로젝트 repo의 evidence와 같이 관리한다.
```

그래서 이미 있는 도구들은 참고할 수 있지만, 네가 원하는 건 약간 다르다.

```text
필요한 기능:
  current project 감지
  live-study-notes.md 위치 감지/생성
  Q/A turn append
  topic/date heading 자동 생성
  나중에 "승격 후보" 표시
  git diff/commit 흐름과 연결
```

작은 OSS/도구 아이디어로 만들면:

```text
live-study-logger

기능:
  1. stdin으로 Q/A markdown을 받는다.
  2. 현재 repo에서 learn/system-design/live-study-notes.md를 찾는다.
  3. 없으면 만든다.
  4. 날짜/topic 아래에 append한다.
  5. optional: promotion markers를 달 수 있다.
```

하지만 "현재 ChatGPT/Codex UI의 transcript를 자동으로 몰래 읽기"는 별도 브라우저 확장/앱 통합이 필요하다. CLI만으로는 안 된다.

결론:

```text
수요:
  있음. export/backup/Obsidian 연동 생태계가 이미 많다.

기존 OSS:
  있음. LibreChat, Claude exporter, ChatGPT export-to-md, Obsidian import/export 계열.

네 workflow의 차별점:
  단순 export가 아니라 "학습 Q/A -> project live notes -> scenario/decision/test로 승격"하는
  repo-native design learning workflow다.
```

참고 링크:

```text
LibreChat:
  https://github.com/danny-avila/LibreChat
  https://www.librechat.ai/docs/features/import_convos

Claude Chat Exporter:
  https://github.com/agarwalvishal/claude-chat-exporter

Convoviz:
  https://github.com/mohamed-chs/convoviz

Continue chat export request:
  https://github.com/continuedev/continue/issues/1283

OpenAI community markdown export request:
  https://community.openai.com/t/export-chat-as-a-markdown-pdf/760565
  https://community.openai.com/t/feature-request-export-project-chat-history-as-markdown/1381686

Google AI Studio community exporter:
  https://discuss.ai.google.dev/t/community-tool-chat-exporter-export-ai-studio-conversations-to-markdown-pdf-and-json-local-only/138329

Obsidian AI chat importer discussion:
  https://forum.obsidian.md/t/plugin-nexus-ai-chat-importer-import-chatgpt-claude-mistral-le-chat-and-perplexity-conversations-to-your-vault/71664
```

---

## 2026-07-09 — Codex / Claude Code transcript logging

### Q
이거는 코덱스나 claude code인데

### A
맞다. 지금 봐야 하는 건 일반 ChatGPT/Claude 웹앱 export가 아니라 **Codex CLI / Claude Code 같은 코딩 에이전트의 로컬 세션 transcript**다.

이 범위에서는 이미 수요와 도구가 있다.

핵심 차이:

```text
일반 AI chat export:
  웹 대화를 Markdown/JSON/PDF로 백업한다.

Codex / Claude Code transcript:
  터미널/IDE 코딩 에이전트 세션의 user prompt, assistant response,
  tool call, tool result, approval, plan, resume context를 기록한다.
```

관련 사례:

```text
Claude Code official sessions:
  Claude Code는 project directory에 tied된 saved conversation/session 개념이 있다.

Claude Code Log:
  Claude Code transcript JSONL 파일을 HTML/Markdown으로 변환하는 Python CLI.

Claude conversation extractor / claude-code-transcripts:
  ~/.claude/projects 쪽 transcript를 추출/변환하는 도구들.

Codex CLI:
  공식 docs에는 resume된 run이 original transcript/plan history/approvals를 유지한다고 설명한다.
  GitHub issue에는 codex export --session, transcripts list/show 같은 요구가 있었다.

Codex History Viewer:
  VS Code extension으로 local Codex CLI / Claude Code session history를 browse/search/tag/import/export하는 도구.
```

중요한 점:

```text
코딩 에이전트 세션 transcript는 단순 채팅 기록보다 더 가치가 크다.
왜냐하면 tool calls, file diffs, command outputs, approvals, plan changes가 들어 있기 때문이다.
```

네가 원하는 workflow는 기존 transcript viewer/exporter보다 한 단계 더 좁다.

```text
기존 도구:
  세션 transcript를 보여주거나 Markdown/HTML로 export한다.

네 workflow:
  학습/설계 질문 Q&A를 프로젝트의 live-study-notes.md에 누적하고,
  중요한 내용만 scenario/decision/test/README로 승격한다.
```

그래서 구현 아이디어는:

```text
1. Codex/Claude Code local session JSONL을 읽는다.
2. 현재 project path에 해당하는 최근 session을 찾는다.
3. user/assistant turn만 추출한다.
4. tool call noise는 제외하거나 접는다.
5. learn/system-design/live-study-notes.md에 append한다.
6. optional: "study Q&A only" 필터를 둔다.
```

이건 충분히 작은 도구/OSS 아이디어가 될 수 있다.

```text
agent-session-to-live-notes

Input:
  ~/.codex/sessions/.../*.jsonl
  ~/.claude/projects/.../*.jsonl

Output:
  <repo>/learn/system-design/live-study-notes.md
```

단, 지금 이 세션에서 내가 바로 자동으로 현재 transcript 파일을 찾아 붙이는 건 별개 문제다. Codex/Claude Code가 로컬에 저장하는 위치/형식은 환경과 버전에 따라 다를 수 있고, 현재 실행 중인 이 대화의 session id와 파일 경로를 내가 항상 자동으로 아는 것은 아니다.

현실적인 다음 단계:

```text
1. ~/.codex/sessions 또는 ~/.claude/projects 구조를 실제로 확인한다.
2. 최근 JSONL transcript 샘플을 읽는다.
3. user/assistant turn 추출 스크립트를 만든다.
4. live-study-notes.md append 포맷으로 변환한다.
```

참고:

```text
Codex CLI docs:
  https://developers.openai.com/codex/cli/features

Codex transcript export feature request:
  https://github.com/openai/codex/issues/2765

Claude Code sessions docs:
  https://code.claude.com/docs/en/sessions

Claude Code Log:
  https://github.com/daaain/claude-code-log

Claude conversation extractor:
  https://github.com/ZeroSumQuant/claude-conversation-extractor

Codex History Viewer:
  https://marketplace.visualstudio.com/items?itemName=hiztam.codex-history-viewer
```

---

## 2026-07-09 — CSV script vs platform evidence

### Q
일단 그럼 여기서는 다시 공부 진행해보자 그러니까 csv 처리 스크립트가 아니다 csv 읽기 합계 계산해서 저장하는데 원본 파일어디서 여러 원본 파일 있을 수 있고 만약 같은 파일 다시 돌리면 기존 히스토리 삭제하도록 해야겠지 기준에 따라 busienss_date일 수도 있고 row가 사라지는건 어떻게 추적하지? 날짜 기준 여러 날짜가 있으니까 백필에 대한 기준도 되겠지 품질검사는 주로 뭘하는지 원본에 대한거? 이전 run 그래서 run_id 세팅해야 하는구나

### A
맞아. 지금 이해가 좋은 방향으로 가고 있어.

핵심은 이거야.

```text
CSV 처리 스크립트:
  CSV 읽기 -> 합계 계산 -> 결과 저장

mini data platform:
  CSV 읽기 -> 합계 계산 -> 결과 저장
  + 이 결과가 어디서 왔는지
  + 믿어도 되는지
  + 다시 돌려도 안전한지
  + 이전 run과 뭐가 다른지
  + row가 어디서 줄었는지
  를 설명할 evidence를 남김
```

## 원본 파일이 여러 개일 수 있다

맞아. 원본 파일은 여러 개일 수 있다.

```text
manufacturing_events_2026-06-29.csv
manufacturing_events_2026-06-30.csv
plant_a.csv
plant_b.csv
```

그래서 파일 이름만 믿으면 안 된다.

```text
파일 이름은 바뀔 수 있다.
같은 내용이 다른 이름으로 들어올 수 있다.
다른 내용이 같은 이름으로 덮일 수 있다.
```

그래서 `source_hash`가 필요하다.

```text
source_hash = 원본 파일 내용의 지문
```

## 같은 파일을 다시 돌리면 기존 히스토리를 삭제하나?

Slice1에서는 삭제보다 **skip**이 핵심이다.

```text
같은 dataset_id + business_date + source_hash로 성공한 run이 이미 있다
-> 새로 처리하지 않는다
-> 기존 successful run을 재사용한다
```

즉 같은 파일을 다시 돌렸다고 기존 히스토리를 삭제하는 게 아니라:

```text
중복 output을 만들지 않는다.
기존 run evidence를 유지한다.
```

나중에 Iceberg로 가면 정정 입력은 overwrite가 될 수 있지만, 그때도 “히스토리 삭제”라기보다:

```text
current 결과는 교체
이전 결과는 snapshot/history로 남김
```

에 가깝다.

## business_date는 백필 기준이 될 수 있다

맞아.

```text
business_date = 어느 업무 날짜의 metric으로 볼 것인가
```

그래서:

```text
2026-06-29를 다시 돌린다
```

는 말은:

```text
business_date=2026-06-29에 해당하는 silver/gold 결과를 다시 만든다
```

는 뜻이 된다.

이게 backfill/reprocessing 기준이 된다.

## row가 사라지는 건 어떻게 추적하나?

여기서 quality check가 필요하다.

예를 들어 source에는 5 rows가 있었는데 silver에는 3 rows만 남을 수 있다.

정상 이유:

```text
business_date가 다른 row를 필터링했다.
natural key 중복 row를 dedup했다.
```

위험한 이유:

```text
파싱 실패로 row가 빠졌다.
타입 변환 실패로 row가 사라졌다.
필터 조건이 잘못됐다.
```

그래서 reconciliation이 필요하다.

```text
source row count
-> silver row count
-> gold aggregate
```

그리고 왜 줄었는지를 quality detail에 남겨야 한다.

## 품질검사는 원본에 대한 건가?

원본만 보는 게 아니라 layer 사이의 contract를 본다.

```text
source/bronze:
  파일이 들어왔는가?
  row_count는 얼마인가?
  schema_hash는 무엇인가?

silver:
  required column이 null이 아닌가?
  natural key가 unique한가?
  accepted_values/range가 맞는가?

gold:
  silver의 additive measure가 gold에서 보존되는가?
  gold grain이 unique한가?
  business_date 기준 freshness가 맞는가?

cross-layer:
  source -> silver row reconciliation
  silver -> gold conservation
```

그래서 quality는 “원본 검사”만이 아니라:

```text
각 layer boundary가 깨지지 않았는지 보는 것
```

에 가깝다.

## run_id가 왜 필요한가

맞아. 그래서 `run_id`가 필요하다.

같은 dataset이라도 여러 번 실행될 수 있다.

```text
R1: 2026-06-29 source_hash=H1 처리 성공
R2: 같은 파일 다시 들어와서 skipped
R3: 정정 파일 source_hash=H2 처리 성공
R4: schema drift warning 발생
```

`run_id`가 없으면:

```text
이 gold 숫자가 어떤 실행에서 나왔는지
그 실행의 source_hash가 무엇인지
quality가 통과했는지
schema drift가 있었는지
```

를 추적하기 어렵다.

한 줄로 정리하면:

> 이 프로젝트는 CSV 결과를 만드는 것보다, 그 결과를 믿고 재현하고 디버깅할 수 있게 `source_hash`, `business_date`, `quality`, `run_id`, `lineage`를 남기는 것이 핵심이다.

---

## 2026-07-09 — core terms from plain project map

### Q
슬라이스라는건 무슨 뜻이지? 파이프라인은 카탈로드?는 뭘 의미하지? 퀄리티 체크는 어떤 정책과 기준이 있어야 겠네 리니지 디버 ㅣ런거겠네 슬라이스 흐름 원본에 타입 변환 퀄리티 EAV는 컬럼명으로 오지 않는다 data/raw/manufacturing_events.csv
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
   quality result 이건 어떤 의미지?  컬럼명을 mapping 컨피그를 추가한다 EAV라는게 무슨 의미지? dataset 관리 대상 데이터의 이름이다 그러니까 csv를 의미하는 거야? 아니면 이런 형식의 csv를 통틀어서? 스키마  idempotency 중복 안생기게

### A
좋아. 지금 헷갈리는 단어를 하나씩 풀자.

**Slice**

`Slice`는 프로젝트 전체를 잘라낸 “작게 끝까지 도는 구현 단위”야.

```text
나쁜 slice:
  source_hash 함수만 만듦
  quality 함수만 만듦

좋은 slice:
  raw CSV 입력
  -> bronze/silver/gold 생성
  -> quality 검사
  -> catalog/lineage 기록
  -> 테스트 통과
```

즉 slice는 기능 하나가 아니라 **입력부터 증거까지 세로로 얇게 끝까지 연결된 흐름**이야.

**Pipeline**

`Pipeline`은 데이터를 단계별로 바꾸는 처리 흐름이야.

```text
raw CSV
-> bronze
-> silver
-> gold
-> quality
-> catalog/lineage
```

여기서 “pipeline이 돈다”는 말은:

```text
입력을 읽고
정리하고
집계하고
검사하고
기록한다
```

는 뜻이야.

**Catalog**

`Catalog`는 데이터를 열어보지 않고도 알 수 있게 해주는 metadata야.

예를 들어:

```text
dataset_id
run_id
source_hash
schema_hash
business_date
row_count
quality status
output path
```

즉 catalog는 “데이터 자체”가 아니라 **데이터에 대한 설명/색인/기록**이야.

**Quality Check**

quality check는 “이 결과를 믿어도 되는가?”를 검사하는 정책과 기준이야.

예:

```text
not_null:
  필수 컬럼이 비어 있으면 안 됨

unique:
  natural key가 중복되면 안 됨

accepted_values:
  operation은 assembly, inspection 같은 허용값이어야 함

range:
  defect_count는 음수면 안 됨

freshness:
  데이터가 너무 오래되면 안 됨

reconciliation:
  source -> silver 과정에서 row가 왜 줄었는지 설명 가능해야 함

conservation:
  silver의 units 합계와 gold의 units 합계가 맞아야 함
```

**Lineage**

lineage는 “무엇이 무엇에서 왔는가”야.

```text
source CSV
-> bronze
-> silver
-> gold
```

그리고 여기에:

```text
어떤 run_id가 만들었는가
어떤 source_hash에서 왔는가
quality는 어땠는가
```

를 연결하는 거야.

즉 나중에:

```text
이 gold 숫자 어디서 왔어?
```

라고 물으면 lineage로 추적하는 거야.

**흐름 의미**

네가 붙인 흐름은 이렇게 보면 돼.

```text
data/raw/manufacturing_events.csv
```

원본 파일.

```text
-> bronze
   원본에 가까운 복사본 + source_hash/schema_hash/row_count manifest
```

bronze는 원본 보존 단계.
여기서 “이 파일이 무엇이었는지” 기록해.

```text
-> silver
   business_date 필터
   natural key dedup
   타입 변환
```

silver는 분석 가능한 깨끗한 row로 만드는 단계.

```text
business_date 필터:
  이번 처리 대상 날짜만 남김

natural key dedup:
  같은 이벤트가 중복으로 들어오면 하나만 남김

타입 변환:
  "10" 문자열을 숫자 10으로 바꿈
```

```text
-> gold
   일별 line/product metric
   units, defects, defect_rate, avg_cycle_time
```

gold는 사용자가 보는 지표.

예:

```text
2026-06-29, line-1, gearbox-a
units = 100
defects = 3
defect_rate = 0.03
avg_cycle_time = 120ms
```

```text
-> quality
```

각 단계가 믿을 만한지 검사.

```text
-> catalog/lineage
```

이 결과가 어떤 입력, 어떤 실행, 어떤 품질 결과에서 왔는지 기록.

**EAV**

EAV는 `Entity-Attribute-Value`야.

쉽게 말하면 wide table을 long format으로 바꾸는 방식.

wide format:

```text
equipment_id, units, defects, temperature
M1, 100, 3, 70
```

EAV long format:

```text
entity_id, attribute, value
M1, units, 100
M1, defects, 3
M1, temperature, 70
```

왜 하냐?

source마다 컬럼명이 다를 수 있으니까.

```text
plant_a.csv:
  설비ID, 생산수량, 불량수

plant_b.csv:
  equipment_id, units, defects
```

이걸 code에 하드코딩하면 source가 늘 때마다 코드가 바뀌어.

대신 mapping config를 둔다.

```json
{
  "설비ID": "equipment_id",
  "생산수량": "units",
  "불량수": "defects"
}
```

그러면 새 source가 와도:

```text
pipeline code는 그대로
mapping config만 추가
```

할 수 있어.

**Dataset**

dataset은 꼭 CSV 파일 하나를 뜻하는 게 아니야.

더 정확히는 **관리 대상 데이터 묶음의 이름**이야.

예:

```text
manufacturing_events
```

라는 dataset은 여러 파일을 가질 수 있어.

```text
manufacturing_events_2026-06-29.csv
manufacturing_events_2026-06-30.csv
manufacturing_events_2026-07-01.csv
```

즉:

```text
CSV file = 실제 들어온 파일 하나
dataset = 같은 의미/구조/목적을 가진 데이터 묶음
dataset version = 특정 입력과 실행으로 만들어진 버전
```

**Schema**

schema는 데이터 구조야.

```text
컬럼 이름
컬럼 타입
필수 여부
의미
```

예:

```text
event_time: timestamp
plant_id: string
line_id: string
units_produced: integer
defect_count: integer
business_date: date
```

**Idempotency**

idempotency는 같은 걸 다시 해도 결과가 중복되거나 망가지지 않는 성질이야.

이 프로젝트에서는:

```text
같은 source_hash + business_date가 이미 성공했다
-> 다시 처리하지 않고 skip
```

그래서 같은 파일을 실수로 다시 넣어도 gold가 두 배가 되지 않아.

한 줄로 정리하면:

```text
dataset = 관리 대상 데이터 묶음
pipeline = raw를 trusted metric과 evidence로 바꾸는 흐름
catalog = 데이터에 대한 설명/기록
lineage = 숫자가 어디서 왔는지
quality = 믿어도 되는지 검사
EAV = 서로 다른 컬럼명을 공통 형태로 바꾸는 방식
idempotency = 다시 돌려도 중복 안 생기게 하는 성질
```

---

## 2026-07-09 — missed live note append

### Q
아직 안붙인거야?

### A
맞아요. 방금 답변은 아직 파일에 안 붙였습니다. 바로 붙이겠습니다.

---

## 2026-07-09 — transcript logger project feasibility

### Q
그래도 너가 지금 잘하고 있긴하네 근데 대신 토큰 소모가 계속 되는거지? 붙여넣기 할때마다 그러면 이게 가능하냐는게 궁금해 이것도 하나의 프로젝트로 진행해볼만해?

### A
맞아. 지금 방식은 잘 작동하지만 비용 구조가 있다.

```text
내가 답변한다.
그 답변을 다시 live-study-notes.md에 붙인다.
붙이는 내용을 생성/전달하는 데 토큰과 도구 호출이 든다.
```

그래서 장기적으로는 자동화할 가치가 있다.

핵심 아이디어는:

```text
모델이 답변을 다시 써서 붙이지 않게 한다.
이미 로컬에 저장된 Codex/Claude Code JSONL transcript를 읽는다.
기계적으로 user/assistant turn을 Markdown으로 변환한다.
live-study-notes.md에 append한다.
```

이렇게 하면 토큰을 크게 줄일 수 있다.

토큰이 드는 경우:

```text
요약
분류
승격 후보 판단
scenario/decision으로 정제
```

토큰이 거의 안 들어도 되는 경우:

```text
JSONL transcript 읽기
user/assistant turn 추출
Markdown Q/A 블록으로 변환
파일에 append
```

그래서 프로젝트로 충분히 진행해볼 만하다. 다만 범위를 작게 잡아야 한다.

v0 프로젝트 아이디어:

```text
agent-session-to-live-notes
```

목표:

```text
Codex/Claude Code 로컬 session transcript를 읽어서
현재 repo의 live-study-notes.md에 Q/A 원문을 append한다.
```

v0 기능:

```text
1. Codex JSONL session 파일 하나를 입력으로 받는다.
2. user message / assistant message를 추출한다.
3. tool call / command output은 기본적으로 제외하거나 접는다.
4. Markdown Q/A 형식으로 변환한다.
5. 지정한 live-study-notes.md에 append한다.
```

v0에서 하지 말 것:

```text
Claude Code까지 동시에 지원
브라우저 UI 자동 수집
실시간 watch daemon
Obsidian sync
semantic 요약
자동 승격
```

좋은 첫 Done 기준:

```text
given Codex session JSONL sample
when converter runs
then live-study-notes.md에 Q/A markdown이 append된다
and tool output noise는 들어가지 않는다
and 원본 JSONL은 수정하지 않는다
```

이건 포트폴리오 메인 프로젝트라기보다 internal tool / workflow tool로 좋다.

나중에 커지면:

```text
Codex 지원
Claude Code 지원
current repo 자동 감지
latest session 자동 선택
study Q/A만 필터
promotion markers
```

까지 갈 수 있다.

결론:

```text
가능하다.
프로젝트로 진행해볼 만하다.
하지만 v0는 "JSONL -> live-study-notes.md 변환기"로 아주 작게 시작해야 한다.
```
