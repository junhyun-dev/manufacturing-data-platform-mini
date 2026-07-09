# System Design Notes

이 폴더는 `manufacturing-data-platform-mini`를 기능별로 보기 전에, 시스템을 **서비스 목적 -> 시나리오 -> 질문 지도 -> 결정 -> 테스트 -> 구현** 순서로 이해하기 위한 학습 노트다.

기준 프로세스: [`scenario-question-decision-loop.md`](/home/junhyun/personal/learning/method/scenario-question-decision-loop.md)

핵심은 문서 번호가 아니라 사고 순서다. 앞으로 다른 프로젝트도 같은 방식으로 읽고 쓸 수 있게, 이 폴더의 기본 루프는 아래와 같다.

```text
service purpose charter
-> scenario seed
-> question map
-> question map audit / challenge
-> state trace / evidence
-> reference decision
-> test contract
-> implementation
-> interview explanation
```

## Thinking Order

1. **Service purpose charter**
   - 이 프로젝트가 왜 존재하는지, 누가 어떤 질문을 위해 쓰는지 먼저 고정한다.
   - 기능 목록이 아니라 service question에서 feature를 도출한다.
2. **Scenario seed**
   - 어떤 상황에서 문제가 생기는지 작게 잡는다.
   - 시나리오는 하나로 고정되지 않는다. late data, schema drift, rerun, bad run, operator debugging처럼 계속 늘어날 수 있다.
3. **Question map**
   - 시나리오에서 어떤 설계 질문이 생기는지 넓게 펼친다.
   - 각 질문을 `Core`, `Demo`, `Backlog`, `Unknown`으로 나눈다.
4. **Question map audit / challenge**
   - Claude/외부 benchmark 관점으로 빠진 질문, 과한 질문, 과장 위험을 찾는다.
   - 구현 범위는 늘리지 않고 질문 품질만 검토한다.
5. **State trace / evidence**
   - 질문이 실제 데이터 상태 전이 어디에서 생기는지 확인한다.
   - 기존 코드와 테스트가 이미 답한 contract를 확인한다.
6. **Reference decision**
   - 질문 하나를 골라 options/tradeoff/decision/test로 수렴한다.
7. **Implementation**
   - decision의 test contract를 먼저 검증하고 코드를 붙인다.

## Documents

0. [`00-service-purpose-charter.md`](00-service-purpose-charter.md)
   - 이 프로젝트가 왜 존재하는지, 누가 어떤 질문을 위해 쓰는지, 어떤 상태를 만들어야 하는지 고정한다.
   - 모든 scenario/question map의 상위 anchor다.
1. [`00a-plain-project-map.md`](00a-plain-project-map.md)
   - Spark/Iceberg 전에 이 프로젝트 자체를 쉽게 보는 입문 지도.
   - 파일이 들어와서 어떤 상태와 증거가 남는지 먼저 이해한다.
2. [`live-study-notes.md`](live-study-notes.md)
   - 정식 decision이 되기 전, 채팅하며 이해한 내용을 쌓는 실시간 공부장.
   - 충분히 정리된 내용만 `scenarios/`나 `reference-decisions/`로 승격한다.
3. [`01-scenario-seed.md`](01-scenario-seed.md)
   - question map을 만들기 위한 scenario seed.
   - 애초에 어떤 상황에서 어떤 문제가 생기는가?
   - 시나리오는 앞으로 계속 추가될 수 있다.
4. [`02-slice2-question-map.md`](02-slice2-question-map.md)
   - Slice2에서 무슨 질문들이 나올 수 있고 각각 어디서/어떻게 풀리나?
   - 이 문서가 Slice2 설계 대화의 중심이다.
   - 질문을 먼저 넓게 펼치고, 이후 decision note로 하나씩 수렴한다.
5. [`03-source-contract.md`](03-source-contract.md)
   - question map 이후에 보는 source evidence.
   - 이 시스템은 정확히 무엇을 입력으로 받는가?
   - source row의 grain과 required columns는 무엇인가?
6. [`04-slice2-spark-iceberg-shift.md`](04-slice2-spark-iceberg-shift.md)
   - Slice1의 state 전이를 Spark/Iceberg로 어떻게 다시 표현하는가?
   - 무엇이 그대로고(contract) 무엇이 바뀌나(엔진/저장소)?
   - question map에서 고른 질문을 state trace로 검증할 때 참고한다.
7. [`05-iceberg-spark-mini-primer.md`](05-iceberg-spark-mini-primer.md)
   - Iceberg/Spark 전체 공부가 아니라, Slice2에 필요한 공식 문서 범위와 walking skeleton을 정리한다.
   - `business_date` 재처리, partition overwrite, snapshot, run_id vs snapshot_id를 연결한다.

## Scenario Walkthroughs

- [`scenarios/01-rerun-same-business-date.md`](scenarios/01-rerun-same-business-date.md)
  - 같은 `business_date`를 다시 처리할 때 append/skip/overwrite/merge 중 무엇을 선택하는가?
  - #1 ACID, #2 write semantics, #10 idempotency를 하나의 시나리오로 이해한다.
- [`scenarios/02-operator-debugging-wrong-gold.md`](scenarios/02-operator-debugging-wrong-gold.md)
  - gold 숫자가 이상할 때 operator가 source/run/quality/lineage evidence를 어떤 순서로 확인하는가?
  - 기존 catalog/lineage claim을 실제 RCA walkthrough로 exercise한다.

그 다음에 개별 의사결정으로 내려간다.

```text
scenario
-> question map
-> question map audit
-> state trace
-> decision note
-> test contract
-> implementation
```

관련 개별 의사결정 노트:

- [`../reference-decisions/schema-drift.md`](../reference-decisions/schema-drift.md)
- [`../reference-decisions/gold-grain.md`](../reference-decisions/gold-grain.md)
- [`../reference-decisions/iceberg-write-semantics.md`](../reference-decisions/iceberg-write-semantics.md)
