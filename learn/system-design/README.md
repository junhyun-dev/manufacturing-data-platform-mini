# System Design Notes

이 폴더는 `robot-data-platform-mini`를 기능별로 보기 전에, 시스템을 **시나리오 -> 질문 지도 -> 결정 -> 테스트 -> 구현** 순서로 이해하기 위한 학습 노트다.

기준 프로세스: [`scenario-question-decision-loop.md`](/home/junhyun/personal/learning/method/scenario-question-decision-loop.md)

핵심은 문서 번호가 아니라 사고 순서다. 앞으로 다른 프로젝트도 같은 방식으로 읽고 쓸 수 있게, 이 폴더의 기본 루프는 아래와 같다.

```text
scenario seed
-> question map
-> state trace / evidence
-> reference decision
-> test contract
-> implementation
-> interview explanation
```

## Thinking Order

1. **Scenario seed**
   - 어떤 상황에서 문제가 생기는지 작게 잡는다.
   - 시나리오는 하나로 고정되지 않는다. late data, schema drift, rerun, bad run, operator debugging처럼 계속 늘어날 수 있다.
2. **Question map**
   - 시나리오에서 어떤 설계 질문이 생기는지 넓게 펼친다.
   - 각 질문을 `Core`, `Demo`, `Backlog`, `Unknown`으로 나눈다.
3. **State trace / evidence**
   - 질문이 실제 데이터 상태 전이 어디에서 생기는지 확인한다.
   - 기존 코드와 테스트가 이미 답한 contract를 확인한다.
4. **Reference decision**
   - 질문 하나를 골라 options/tradeoff/decision/test로 수렴한다.
5. **Implementation**
   - decision의 test contract를 먼저 검증하고 코드를 붙인다.

## Documents

1. [`00-system-scenario.md`](00-system-scenario.md)
   - question map을 만들기 위한 scenario seed.
   - 애초에 어떤 상황에서 어떤 문제가 생기는가?
   - 시나리오는 앞으로 계속 추가될 수 있다.
2. [`01-source-contract.md`](01-source-contract.md)
   - 이 시스템은 정확히 무엇을 입력으로 받는가?
   - streaming event인가, batch CSV인가?
   - source row의 grain과 required columns는 무엇인가?
3. [`02-slice2-question-map.md`](02-slice2-question-map.md)
   - Slice2에서 무슨 질문들이 나올 수 있고 각각 어디서/어떻게 풀리나?
   - 이 문서가 Slice2 설계 대화의 중심이다.
   - 질문을 먼저 넓게 펼치고, 이후 decision note로 하나씩 수렴한다.
4. [`03-slice2-spark-iceberg-shift.md`](03-slice2-spark-iceberg-shift.md)
   - Slice1의 state 전이를 Spark/Iceberg로 어떻게 다시 표현하는가?
   - 무엇이 그대로고(contract) 무엇이 바뀌나(엔진/저장소)?
   - question map에서 고른 질문을 state trace로 검증할 때 참고한다.

그 다음에 개별 의사결정으로 내려간다.

```text
scenario
-> question map
-> state trace
-> decision note
-> test contract
-> implementation
```

관련 개별 의사결정 노트:

- [`../reference-decisions/schema-drift.md`](../reference-decisions/schema-drift.md)
