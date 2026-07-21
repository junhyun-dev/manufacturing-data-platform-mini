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
| Airflow | CLI wrapper DAG | partial (lakehouse CLI와 Spark/Iceberg skeleton은 local `dags test` runtime 검증; Spark/Iceberg wrapper는 development `standalone` scheduler/LocalExecutor도 검증; production deployment는 claim하지 않음) |
| Spark / Iceberg | 단일 gold Iceberg table, `business_date` partition overwrite, snapshot evidence | partial (full Spark medallion pipeline 아님) |
| Kafka K1 | bounded producer/consumer, immutable JSONL+manifest, offset/recovery/replay/quarantine | partial (local broker 1개/partition 1개; continuous streaming 아님) |
| Kafka K1.5 landing -> batch | provenance 보존 결정적 CSV로 기존 quality/gold/Iceberg 재사용, 같은 입력은 skip | partial (bounded local bridge; direct sink/streaming 아님) |
| Spark engine swap (S7) | K1.5 canonical CSV에서 silver/gold를 Spark로 재표현, Python parity 검증, quality 통과분만 partition overwrite publish | partial (한 slice; full medallion rewrite·cluster Spark 아님) |

## 산업 플랫폼 reference — 압력 -> 이 repo의 결정

2026-07-21에 공식 제품 문서로 조사했다. 벤더 기능 나열이 아니라 **서비스 압력**이 핵심이며, 각 행은 이 repo의 결정으로 끝난다.

| Reference (1차 출처) | 해결하는 서비스 압력 | 그쪽의 계약 | 이 repo의 결정 |
|---|---|---|---|
| **AWS IoT SiteWise Edge gateway** — [docs](https://docs.aws.amazon.com/iot-sitewise/latest/userguide/gateways.html) | 현장 네트워크가 끊겨도 수집은 멈추면 안 된다 | "인터넷 단절 중에도 수집·처리를 계속하고, 연결이 복구되면 클라우드와 동기화" | **계약만 COPY**(단절 → 로컬 durable → 복구 후 sync)해 *제안 시나리오*로. Greengrass/Siemens Edge 런타임과 실제 OPC-UA/MQTT 연결은 **AVOID**. |
| **Azure IoT Operations data flows** — [docs](https://learn.microsoft.com/en-us/azure/iot-operations/connect-to-cloud/overview-dataflow) | 전달 도중 destination·네트워크가 불가용 | 전달 실패 시 **source 메시지를 ack하지 않고** broker 큐에 남겨 재시도, disk 버퍼 지원 | **안전한 처리 순서만 COPY** — K1도 landing이 durable해지기 전에 consumer offset을 전진시키지 않는다. 구현이나 delivery guarantee가 같다는 뜻은 아니다. 단위 변환은 EAV에 있지만 reference-data enrichment는 미구현이다. |
| **Cognite Data Fusion contextualization** — [docs](https://docs.cognite.com/cdf/integration/concepts/contextualization) | 같은 설비인데 시스템마다 ID가 다르다 | 소스별 리소스를 하나의 모델로 매핑해 "동일 엔티티가 같은 식별자를 갖게" 한 뒤 실제 관계대로 연결 | **제안 문제로 유지** — EAV는 컬럼과 단위를 표준화하지만, 여러 source ID를 하나의 canonical asset으로 해소하지는 않는다. ML 매칭·3D/P&ID·asset hierarchy 제품화는 **AVOID**. |
| **HighByte Unified Namespace** — [page](https://www.highbyte.com/intelligence-hub/unified-namespace) *(벤더 주장)* | 단절된 시스템의 data silo | 일관된 추상 구조 하나로 산업 데이터 제공 — **벤더 포지셔닝이며 독립 표준 정의가 아님** | 이 규모에서 UNS 도입은 **AVOID**. asset·topic 명명 일관성이라는 교훈만 취한다. |

네 소스는 하나의 공통 패턴이 아니라 두 lane을 보여준다. AWS/Azure는 **전달 실패와 단절 중 연속성**, Cognite/HighByte는 **소스 간 식별과 명명**의 필요성을 보여준다. 이 repo에는 각각과 관련된 K1 durability ordering과 EAV 컬럼·단위 표준화가 있지만, edge buffer와 cross-source asset identity는 아직 구현하지 않았다(`ROADMAP.ko.md` Phase 3 참조).

이 lane에서 구현하지 않았고 주장하지도 않는 것: OPC UA / MQTT / ROS 2 / DDS 연동, edge gateway 하드웨어나 product 수준 offline buffer, asset hierarchy, digital twin, 이상탐지, 예지보전, closed-loop 제어.

## Anti-benchmark

의도적으로 제외한 것과 **그 이유**:

| 제외 대상 | 왜 제외했나 |
|---|---|
| full Databricks clone | 이 프로젝트는 플랫폼 복제가 아니라 loop 하나를 얇게 관통하는 게 목적이다. |
| full Spark/Iceberg medallion runtime | S7에서 한 slice의 engine swap만 증명했다. 전 layer 재작성은 slice 범위를 넘는다. |
| continuous Kafka/Flink streaming pipeline | bounded local Kafka K1/K1.5만 구현했다. long-running consumer·window/watermark·streaming sink는 주장하지 않는다. |
| governance/RBAC | 거버넌스 메타데이터 모델은 인정하되 콘솔은 별도 제품 영역이다. |
| lineage graph backend | run/parent 레코드로 저장한다. 브라우저블 그래프는 표현 계층이지 loop가 아니다. |
| vector DB/RAG demo | OPTIONAL — 면접이 실제로 요구할 때만. 지금은 dataset 품질·버전 규율로만 설명한다. |

이 제외 목록과 **이유**가 함께 있어야 작은 slice가 과장된 플랫폼 claim으로 보이지 않는다.

## CORE vs OPTIONAL

CORE:

- medallion
- EAV
- bounded Kafka K1 / K1.5 landing -> batch bridge
- quality
- catalog/lineage
- local Spark/Iceberg publish + S7 Spark machine-event batch (full medallion rewrite는 여전히 backlog)

OPTIONAL:

- AI dataset QA
- RAG/vectorDB/LLM preprocessing

optional은 특정 면접에서 필요할 때만 붙인다.
