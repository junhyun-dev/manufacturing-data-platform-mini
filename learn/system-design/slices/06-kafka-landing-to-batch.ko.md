# 06. Kafka Landing -> Batch Bridge Slice (K1.5)

상태: implemented / local Kafka-to-batch and downstream Iceberg publish verified

> 이 문서는 K1의 immutable accepted landing을 기존 batch 경로에 잇는 얇은 slice index다.
> 상세 결정은 [`../../reference-decisions/kafka-landing-to-batch-adapter.md`](../../reference-decisions/kafka-landing-to-batch-adapter.md),
> 최신 테스트 수와 실행 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)가 source of truth다.

## 1. Slice Thesis

```text
한 business_date의 accepted Kafka JSONL을 결정적 batch input으로 바꾸고,
Spark Structured Streaming 없이 기존 quality/gold/Iceberg 경로를 재사용한다.
```

닫는 경로:

```text
Kafka record
-> immutable accepted JSONL + manifest (K1)
-> deterministic adapter CSV + provenance manifest (K1.5)
-> 기존 Python bronze/silver/gold + quality + JSON catalog
-> 기존 local Spark/Iceberg gold publish
```

## 2. Primary Scenario

```text
운영자가 이미 Kafka coordinate evidence와 함께 event를 landing 했다.
운영자는 business_date 하나를 처리하고,
어떤 accepted Kafka record가 batch에 들어갔는지 확인하고,
quality-passed gold를 얻고,
같은 accepted set을 다시 돌려도 trusted 결과가 두 배가 되지 않기를 원한다.
```

배경: [`05-kafka-raw-ingestion.ko.md`](05-kafka-raw-ingestion.ko.md) · [`04-lakehouse-to-iceberg-publish.ko.md`](04-lakehouse-to-iceberg-publish.ko.md)

## 3. Core Questions

| Core question | 채택한 계약 |
|---|---|
| 한 adapter run은 무엇인가? | 명시적 `business_date` 하나. 첫 row에서 추론하지 않는다. |
| 어떤 입력이 자격이 있는가? | manifest와 coordinate/status/`event_id`/key/timestamp가 일치하는 accepted envelope만. |
| source identity는 무엇인가? | canonical CSV의 SHA-256. Kafka provenance가 포함되어 identity에 반영된다. |
| 결정적 순서는? | `(topic, partition, offset)` 정렬 + 고정 header + 고정 `\n`. |
| 어떤 grain이 다리를 건너는가? | accepted event 1개 = bronze 1 row. silver/gold grain은 불변. |
| rerun은? | 같은 accepted set -> 같은 version/hash -> pipeline `status=skipped`. |
| invalid/tampered input은? | pipeline 호출 **전에** 실패한다. trusted state를 만들지도 전진시키지도 않는다. |

## 4. Backlog / Not Claimed

```text
continuous consumer service
Spark Structured Streaming (K2)
direct Kafka -> Iceberg sink
multi-partition/rebalance
Schema Registry / Avro
column-level Kafka lineage
cryptographic end-to-end payload integrity
concurrent adapter writer
production 운영
```

## 5. Evidence

- [`../../../src/manufacturing_data_platform/kafka_ingestion/batch_adapter.py`](../../../src/manufacturing_data_platform/kafka_ingestion/batch_adapter.py)
- [`../../../tests/test_kafka_batch_adapter.py`](../../../tests/test_kafka_batch_adapter.py)
- [`../../../scripts/verify_kafka_k1_5.sh`](../../../scripts/verify_kafka_k1_5.sh)
- [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)

## 6. Next Questions

```text
Airflow가 bounded replay/backfill과 downstream publish만 소유하는가?
Spark Structured Streaming(K2)은 window/latency 압력이 실제로 명명될 때만 필요한가?
failure-state forensics가 K2보다 먼저 와야 하는 다음 압력인가?
```
