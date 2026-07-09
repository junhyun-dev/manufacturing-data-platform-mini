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

이건 프로젝트 문서에만 둘 게 아니라 `/home/junhyun/dev/DECISION_LEARNING_PLAYBOOK.md`에 기준으로 들어가야 한다. 그래야 OSS 분석이든 포트폴리오 설계든 같은 방식으로 진행할 수 있다.

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
/home/junhyun/dev/projects/manufacturing-data-platform-mini/learn/system-design/02-slice2-question-map.md 근데 이거 너무 빈약한거 아니야? 내가 볼때 질문을 잘 뽑는게 너 혼자 하는거면 안되고 클로드 피드백도 받아가면서 이걸 잘 작성해야 할거 같은데 이게 프로세스화 되어 있어야 할거 같거든?

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
/home/junhyun/dev/DECISION_LEARNING_PLAYBOOK.md
/home/junhyun/dev/projects/manufacturing-data-platform-mini/learn/system-design/README.md
/home/junhyun/dev/projects/manufacturing-data-platform-mini/learn/system-design/02-slice2-question-map.md
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
