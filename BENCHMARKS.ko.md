# BENCHMARKS 한국어판

원문: [`BENCHMARKS.md`](BENCHMARKS.md)

## 목적

이 문서는 프로젝트가 어떤 공개 reference pattern을 참고했는지, 그리고 무엇을 의도적으로 제외했는지 기록한다.

원칙:

```text
official docs = 규칙
OSS = 구조 감각
private/company code = 운영 경험 패턴만
```

공개 repo의 코드는 새로 작성하며, 데이터는 synthetic data만 사용한다.

## 공식 패턴

| Reference | 가져온 것 |
|---|---|
| Databricks Medallion | bronze/silver/gold layering |
| Airflow Best Practices | DAG는 얇은 wrapper, business_date/data_interval 사용 |
| Apache Iceberg | partition, schema evolution, snapshot 사고방식 |
| OpenLineage | run/job/dataset vocabulary, input -> output lineage |
| dbt generic tests | `not_null`, `unique`, `accepted_values` 같은 test shape |
| Great Expectations / Soda | expected vs actual + status 형태 |

도구 자체를 모두 dependency로 넣지 않고, 해당 도구들이 표현하는 model을 작은 코드로 구현한다.

## OSS 구조 패턴

| Reference | 배운 점 | 제외한 점 |
|---|---|---|
| OpenMetadata / DataHub | dataset identity, version/run, lineage parent links | graph DB, UI, ownership/tags |
| DVC / lakeFS | content hash 기반 dataset versioning | branching, atomic commit |
| dbt | model -> tests -> docs discipline | full compiler, adapters |
| Kedro / Dagster | pure node와 IO/orchestration 분리 | asset graph runtime, UI |

가장 중요한 차용은 pure transform과 IO 분리다. 이게 있어야 나중에 Spark로 바꾸는 일이 rewrite가 아니라 engine swap이 된다.

## JD mapping

핵심 claim은 다음과 같이 정리된다.

| JD keyword | 이 repo의 evidence | 상태 |
|---|---|---|
| Lakehouse / medallion | bronze -> silver -> gold | implemented |
| Data Mart | gold daily metrics | implemented |
| EAV / multi-format intake | mapping config -> EAV -> gold | implemented |
| Data quality | dbt-style checks + reconciliation | implemented |
| Schema drift | previous successful run과 schema hash 비교 | implemented |
| Idempotency / backfill | prior success 재사용 | implemented |
| Lineage | run/layer parent links | partial |
| Catalog | datasets / dataset_versions | implemented |
| Airflow | CLI wrapper DAG | partial (local `dags test` runtime 검증; production scheduler/worker 미구현) |
| Spark / Iceberg | 설계 primitive만 있음 | backlog |

## Anti-benchmark

의도적으로 제외한 것:

- full Databricks clone
- real Spark/Iceberg runtime
- streaming Kafka pipeline
- governance/RBAC
- lineage graph backend
- vector DB/RAG demo

이 제외 목록이 중요하다. 그래야 작은 slice가 과장된 플랫폼 claim으로 보이지 않는다.

## CORE vs OPTIONAL

CORE:

- medallion
- EAV
- quality
- catalog/lineage
- Spark/Iceberg translation backlog

OPTIONAL:

- AI dataset QA
- RAG/vectorDB/LLM preprocessing

optional은 특정 면접에서 필요할 때만 붙인다.
