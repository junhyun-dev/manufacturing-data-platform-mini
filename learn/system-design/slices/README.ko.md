# Slice Maps

상태: 질문 기반 설계 흐름을 한눈에 보는 얇은 index

이 폴더는 시나리오와 구현 사이의 중간 지도다.

```text
scenario
-> slice map
-> reference decision
-> code / tests
-> verification log
-> blog / resume claim
```

## 역할 구분

| 문서 종류 | 역할 |
|---|---|
| `scenarios/` | 어떤 상황에서 문제가 생기는가 |
| `question-bank/` | 어떤 질문 축을 넓게 가져올 수 있는가 |
| `slices/` | 이번 build에서 어떤 질문을 Core로 잡고 무엇을 Backlog로 뺐는가 |
| `reference-decisions/` | 특정 결정 하나의 선택지와 tradeoff |
| `VERIFICATION_LOG.md` | 실제 실행 명령과 검증 결과의 source of truth |

## Anti-Drift Rule

slice map은 content container가 아니다.

```text
테스트 개수, CLI 결과, runtime 상태:
  VERIFICATION_LOG.md에만 둔다.

구현 세부:
  code/test 파일을 링크한다.

긴 decision reasoning:
  reference-decisions/ 또는 기존 system-design 문서를 링크한다.
```

slice map의 고유 가치는 아래 세 가지다.

```text
1. slice thesis
2. Core/Demo/Backlog/Unknown 질문 분류
3. 다음 질문
```

## Supporting Docs Rule

작은 slice는 이 폴더에 `NN-name.ko.md` 한 파일로 둔다.

supporting 문서가 여러 개로 커지는 slice는 새 top-level 기술 폴더를 만들지 않고, slice 하위 폴더로 묶는다.

```text
slices/<slice-name>/
  00-slice-map.ko.md
  primer.md
  version-pin.md
  audit-notes.md
```

Spark/Iceberg partition overwrite slice는 이 규칙을 따른다.

## Slices

Template:

- [`TEMPLATE.ko.md`](TEMPLATE.ko.md)
  - 새 slice를 시작하거나, 이미 구현한 slice를 얇은 index로 정리할 때 쓰는 7-section 템플릿.

Current slices:

1. [`spark-iceberg-partition-overwrite/00-slice-map.ko.md`](spark-iceberg-partition-overwrite/00-slice-map.ko.md)
   - same `business_date` correction을 Iceberg partition overwrite + snapshot evidence로 표현한 slice.
2. [`02-airflow-wrapper-command-contract.ko.md`](02-airflow-wrapper-command-contract.ko.md)
   - Airflow가 business logic을 갖지 않고 같은 lakehouse CLI를 호출한다는 wrapper contract slice.
3. [`03-airflow-spark-iceberg-runtime.ko.md`](03-airflow-spark-iceberg-runtime.ko.md)
   - local Airflow가 Spark/Iceberg skeleton CLI를 trigger한다는 runtime wrapper slice.
4. [`04-lakehouse-to-iceberg-publish.ko.md`](04-lakehouse-to-iceberg-publish.ko.md)
   - 기존 JSON lakehouse run의 successful gold CSV를 local Iceberg current table로 publish하는 slice.
5. [`05-kafka-raw-ingestion.ko.md`](05-kafka-raw-ingestion.ko.md)
   - Kafka가 필요한 시나리오에서 producer/consumer/offset/raw landing 질문을 Core로 자르기 위한 design-only slice.
