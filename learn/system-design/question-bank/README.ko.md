# Question Bank 상세 지도

상태: 영역별 상세 질문 은행
상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

이 폴더는 `08-area-question-bank.ko.md`의 상세판이다.

`08`은 전체 지도이고, 이 폴더는 각 영역별로 아래를 더 자세히 적는다.

```text
질문
-> 질문의 의도
-> 선택지
-> Core가 되는 경우
-> 놓치기 쉬운 질문
```

## 읽는 순서

처음 읽는다면 먼저 쉬운 말 버전부터 본다.

0. [`00-plain-language-guide.ko.md`](00-plain-language-guide.ko.md)
   - 어려운 설계 용어를 쉬운 말과 예시로 번역한다.
   - `current state`, `snapshot`, `atomicity`, `shuffle`, `claim boundary` 같은 말을 먼저 풀어준다.
1. [`01-service-identity-contract.ko.md`](01-service-identity-contract.ko.md)
   - service/user workflow
   - data grain / identity / versioning
   - source contract / schema evolution
2. [`02-quality-rerun-failure.ko.md`](02-quality-rerun-failure.ko.md)
   - quality / reconciliation
   - rerun / backfill / correction
   - failure state / retry / recovery
3. [`03-storage-spark-consistency.ko.md`](03-storage-spark-consistency.ko.md)
   - storage / table format / file layout
   - Spark / distributed processing
   - concurrency / atomicity / consistency
4. [`04-orchestration-observability.ko.md`](04-orchestration-observability.ko.md)
   - orchestration / scheduling / Airflow
   - observability / operator evidence
5. [`05-security-performance-testing-claim.ko.md`](05-security-performance-testing-claim.ko.md)
   - security / privacy / governance / retention
   - performance / scale / cost
   - testing / local reproducibility / CI
   - public claim / resume boundary

## 사용법

새 slice를 시작할 때:

```text
1. slice thesis를 쓴다.
2. 이 폴더에서 관련 영역 문서를 연다.
3. 질문을 넓게 복사한다.
4. 각 질문에 working answer를 단다.
5. Core / Demo / Backlog / Unknown으로 분류한다.
6. Core만 decision note + test contract로 내린다.
```

중요:

```text
질문을 많이 뽑는 것은 scope를 키우기 위한 것이 아니다.
질문을 많이 뽑는 이유는 무엇을 이번에 안 할지 명확히 하기 위해서다.
```
