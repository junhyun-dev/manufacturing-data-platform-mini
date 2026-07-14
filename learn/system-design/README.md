# System Design Notes

이 폴더는 `manufacturing-data-platform-mini`를 기능별로 보기 전에, 시스템을 **서비스 목적 -> 시나리오 -> 질문 지도 -> slice map -> 결정 -> 테스트 -> 구현** 순서로 이해하기 위한 학습 노트다.

기준 프로세스: 이 README의 Thinking Order와 `slices/TEMPLATE.ko.md`에 공개 repo용으로 정리한다.

핵심은 문서 번호가 아니라 사고 순서다. 앞으로 다른 프로젝트도 같은 방식으로 읽고 쓸 수 있게, 이 폴더의 기본 루프는 아래와 같다.

```text
service purpose charter
-> scenario seed
-> question map
-> question map audit / challenge
-> state trace / evidence
-> slice map
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
   - 질문이 국소적으로 좁아지면 [`08-area-question-bank.ko.md`](08-area-question-bank.ko.md)에서 보안/분산처리/재처리/장애/운영/claim 축을 다시 확인한다.
4. **Question map audit / challenge**
   - Claude/외부 benchmark 관점으로 빠진 질문, 과한 질문, 과장 위험을 찾는다.
   - 구현 범위는 늘리지 않고 질문 품질만 검토한다.
5. **State trace / evidence**
   - 질문이 실제 데이터 상태 전이 어디에서 생기는지 확인한다.
   - 기존 코드와 테스트가 이미 답한 contract를 확인한다.
6. **Slice map**
   - 한 build 단위에서 어떤 질문을 Core로 잡고 무엇을 Backlog로 뺐는지 얇게 묶는다.
   - 상세 상태를 복사하지 않고 code/test/verification log를 링크한다.
7. **Reference decision**
   - 질문 하나를 골라 options/tradeoff/decision/test로 수렴한다.
8. **Implementation**
   - decision의 test contract를 먼저 검증하고 코드를 붙인다.

## Root Documents

루트에는 전체 진입점만 둔다. 특정 slice나 상세 영역 문서는 하위 폴더로 내려간다.

0. [`00-service-purpose-charter.md`](00-service-purpose-charter.md)
   - 이 프로젝트가 왜 존재하는지, 누가 어떤 질문을 위해 쓰는지, 어떤 상태를 만들어야 하는지 고정한다.
   - 모든 scenario/question map의 상위 anchor다.
1. [`00a-plain-project-map.md`](00a-plain-project-map.md)
   - Spark/Iceberg 전에 이 프로젝트 자체를 쉽게 보는 입문 지도.
   - 파일이 들어와서 어떤 상태와 증거가 남는지 먼저 이해한다.
2. [`live-study-notes.md`](live-study-notes.md)
   - 정식 decision이 되기 전, 채팅하며 이해한 내용을 쌓는 실시간 공부장.
   - 충분히 정리된 내용만 `scenarios/`나 `reference-decisions/`로 승격한다.
3. [`08-area-question-bank.ko.md`](08-area-question-bank.ko.md)
   - 보안, 분산처리, 재처리, 장애, 품질, 운영, claim boundary 등 영역별 질문 은행.
   - 특정 slice를 구현하기 전에 관련 질문을 넓게 뽑고 Core/Demo/Backlog/Unknown으로 내릴 때 사용한다.
   - 상세 질문은 [`question-bank/`](question-bank/) 아래에 영역별로 나뉘어 있다. 처음 읽을 때는 [`question-bank/00-plain-language-guide.ko.md`](question-bank/00-plain-language-guide.ko.md)부터 본다.

## Folder Layout

| 폴더 | 역할 |
|---|---|
| [`scenarios/`](scenarios/) | 문제 상황과 walkthrough |
| [`source-contracts/`](source-contracts/) | 입력 파일/row/schema/source identity 계약 |
| [`question-bank/`](question-bank/) | 영역별 상세 질문 은행 |
| [`slices/`](slices/) | build 단위의 얇은 질문/범위/evidence 지도 |

## Folder Axis Rule

기본 원칙은 `by stage/type`이다.

```text
scenario        = 문제 상황
source contract = 입력 약속
question-bank   = 질문 축
slice           = 이번 build 범위
decision        = 특정 선택의 tradeoff
```

새 기술 문서가 늘어날 때도 새 top-level topic folder를 바로 만들지 않는다.

```text
좋음:
  slices/<slice-name>/
    00-slice-map.ko.md
    supporting-primer.md
    version-pin.md
    audit-notes.md

피함:
  airflow/
  kafka/
  streaming/
  spark-iceberg-v2/
```

즉 top-level은 계속 stage/type 중심으로 유지하고, 큰 slice의 상세 supporting docs는 해당 slice 아래로 중첩한다.

## Terminology Note

이 문서의 용어는 표준 용어와 프로젝트 내부 용어가 섞여 있다. 기술/데이터 엔지니어링 용어는 표준 표현을 우선하고, 내부 명명은 방법론 층에만 제한한다.

| 용어 | 일반성 | 이 프로젝트에서의 뜻 |
|---|---|---|
| scenario | 일반적 | 문제가 생기는 사용자/운영 상황 |
| data contract / source contract | data contract는 일반적, source contract는 입력 쪽으로 좁힌 표현 | 입력 데이터가 지켜야 하는 schema, grain, identity 약속 |
| ADR / decision note | 일반적 | 한 설계 선택의 options, tradeoff, decision 기록 |
| vertical slice / slice | 일반적 | end-to-end로 작게 자른 build 단위 |
| walking skeleton | 일반적 | 전체 구조를 아주 얇게 관통하는 실행 가능한 뼈대 |
| question map | 내부 명명 | discovery, requirements elicitation, design review 질문을 한곳에 펼친 지도 |
| slice map | 내부 명명 | 이번 build에서 Core/Backlog/Unknown을 자른 얇은 index |
| claim boundary | 내부 명명 | RFC의 non-goals나 Definition of Done의 제외 범위를 이 프로젝트식으로 부르는 말 |
| verification log | 내부 명명 | release checklist/run evidence에 가까운 실행 기록 |

## Slice Maps

- [`slices/README.ko.md`](slices/README.ko.md)
  - 시나리오와 구현 사이에서, 이번 build가 어떤 질문을 Core/Backlog로 잘랐는지 보는 얇은 index.
- [`slices/TEMPLATE.ko.md`](slices/TEMPLATE.ko.md)
  - 새 slice를 시작하거나 완료된 slice를 정리할 때 쓰는 7-section template.
- [`slices/spark-iceberg-partition-overwrite/00-slice-map.ko.md`](slices/spark-iceberg-partition-overwrite/00-slice-map.ko.md)
  - Spark/Iceberg partition overwrite slice의 질문 -> 설계 -> 구현 -> 검증 링크 지도.
- [`slices/02-airflow-wrapper-command-contract.ko.md`](slices/02-airflow-wrapper-command-contract.ko.md)
  - Airflow wrapper command contract slice의 질문 -> 설계 -> 구현 -> 검증 링크 지도.
- [`slices/05-kafka-raw-ingestion.ko.md`](slices/05-kafka-raw-ingestion.ko.md)
  - Kafka ingestion 질문을 넓게 펼친 뒤 첫 raw-landing build 범위를 자른 지도.
  - Kafka 4.3.1 + confluent-kafka 2.15.0 Test 0은 검증됐고, K1 raw landing은 다음 구현 단계다.

## Scenario Walkthroughs

- [`scenarios/00-scenario-seed.md`](scenarios/00-scenario-seed.md)
  - question map을 만들기 위한 scenario seed.
  - 애초에 어떤 상황에서 어떤 문제가 생기는가?
- [`scenarios/01-rerun-same-business-date.md`](scenarios/01-rerun-same-business-date.md)
  - 같은 `business_date`를 다시 처리할 때 append/skip/overwrite/merge 중 무엇을 선택하는가?
  - #1 ACID, #2 write semantics, #10 idempotency를 하나의 시나리오로 이해한다.
- [`scenarios/02-operator-debugging-wrong-gold.md`](scenarios/02-operator-debugging-wrong-gold.md)
  - gold 숫자가 이상할 때 operator가 source/run/quality/lineage evidence를 어떤 순서로 확인하는가?
  - 기존 catalog/lineage claim을 실제 RCA walkthrough로 exercise한다.
- [`scenarios/03-kafka-machine-event-ingestion.md`](scenarios/03-kafka-machine-event-ingestion.md)
  - 제조 설비 event를 파일 마감 전에 받아 replay 가능한 raw landing으로 넘기려면 Kafka가 필요한가?
  - event identity, partition ordering, offset commit, failure/replay 질문을 여는 design-only 시나리오다.

## Large Slice Supporting Docs

- [`slices/spark-iceberg-partition-overwrite/README.md`](slices/spark-iceberg-partition-overwrite/README.md)
  - Spark/Iceberg partition overwrite slice의 supporting docs 진입점.

## Source Contracts

- [`source-contracts/README.md`](source-contracts/README.md)
  - 입력 파일/row/schema/source identity 계약의 진입점.

그 다음에 개별 의사결정으로 내려간다.

```text
scenario
-> question map
-> question map audit
-> state trace
-> slice map
-> decision note
-> test contract
-> implementation
```

관련 개별 의사결정 노트:

- [`../reference-decisions/schema-drift.md`](../reference-decisions/schema-drift.md)
- [`../reference-decisions/gold-grain.md`](../reference-decisions/gold-grain.md)
- [`../reference-decisions/iceberg-write-semantics.md`](../reference-decisions/iceberg-write-semantics.md)
