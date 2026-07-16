# Kafka Landing To Batch Adapter (K1.5)

ADR Status: Implemented
검토 상태: Codex reviewed / local runtime verified

> code/test/local Kafka runtime과 downstream Spark/Iceberg publish를 독립 검증했다.
> 최신 테스트 수와 실행 결과는 `VERIFICATION_LOG.md`가 source of truth다.

## Context

K1은 accepted Kafka event를 immutable JSONL + manifest로 남긴다. 기존 batch pipeline은
CSV 파일을 읽고 `hash_file(source)`를 source identity로 쓰며, 같은 `(dataset, business_date,
source_hash)` 재실행을 `status=skipped`로 처리한다.

두 자산을 잇는 방법은 여러 가지지만, K1.5의 압력은 하나다.

```text
운영자가 한 business_date의 accepted Kafka record만 골라
어떤 coordinate가 batch에 들어갔는지 설명할 수 있고,
같은 accepted set을 다시 돌려도 trusted 결과가 두 배가 되지 않아야 한다.
```

Spark Structured Streaming이나 direct Kafka->Iceberg sink는 이 압력을 풀기 위해 필요하지 않다.

## Decision

```text
adapter input  = immutable accepted.jsonl (manifest와 교차 검증 통과한 것만)
adapter output = content-addressed immutable version + provenance.json
adapter identity = SHA-256(canonical CSV) = 기존 pipeline의 source_hash
downstream = 기존 run_lakehouse_pipeline(catalog_backend="json")
```

세부 계약:

| 결정 | 내용 | 이유 |
|---|---|---|
| 한 번의 run | 명시적 `business_date` 하나. 첫 row에서 추론하지 않는다. | 추론하면 잘못된 partition을 조용히 처리한다. |
| 입력 자격 | `accepted.jsonl` envelope 중 coordinate/status/`event_id`/key/timestamp가 sibling `manifest.json`과 일치하는 것만. duplicate/quarantine 파일은 batch input이 아니다. | quarantine된 record가 trusted 집계에 들어가면 안 된다. |
| source identity | canonical CSV의 SHA-256. CSV는 business 컬럼 + `event_id`/`schema_version`/Kafka coordinate를 포함한다. | provenance가 바뀌면 identity가 바뀐다. |
| 결정적 순서 | `(topic, partition, offset)` 정렬 + 고정 header + 고정 `\n`. | filesystem 탐색 순서와 manifest `created_at`이 hash에 영향을 주면 안 된다. |
| grain | accepted event 하나가 bronze 한 row. 기존 silver natural key와 gold grain은 그대로. | 이 slice에서 silver/gold를 재설계하지 않는다. |
| rerun | 같은 accepted set -> 같은 version/`source_hash` -> pipeline `status=skipped`. | 기존 idempotency 계약을 그대로 재사용한다. |
| 실패 | invalid/tampered input이면 pipeline 호출 전에 실패한다. | trusted lakehouse state를 만들거나 전진시키면 안 된다. |

### 왜 pipeline 호출 전에 실패해야 하는가

`run_lakehouse_pipeline`은 `ensure_sample_manufacturing_csv(raw_path)`로 시작한다. 존재하지
않는 경로를 넘기면 **synthetic sample CSV를 생성해서** 그럴듯한 성공 run을 만든다. 또한 선택된
row가 0이면 gold는 zero-row placeholder를 만든다. 따라서 adapter는 date 선택이 비었거나 landing
정합성이 깨졌으면 **pipeline을 호출하지 않고** 실패해야 한다. 이 false-green 경로가 이 ADR의
핵심 failure 계약이다.

## Failure states

| Failure point | Result | Recovery |
|---|---|---|
| envelope가 manifest와 불일치 | adapter 실패, lakehouse 미호출 | landing evidence를 고치거나 조사 |
| 요청한 date에 accepted event 없음 | adapter 실패, synthetic sample/zero-row gold 없음 | 올바른 date를 주거나 K1을 먼저 실행 |
| staging write 중 실패 | version 미생성 | staging 제거 후 재시도 (같은 hash로 수렴) |
| rename 후 재실행 | 같은 source_hash version 존재 | 내용 비교 후 `status=reused` |
| 기존 version과 내용 불일치 | adapter 실패 | 손상된 version을 조사 (덮어쓰지 않는다) |

## Boundaries

- **Fingerprint 경계**: K1 fingerprint는 원본 Kafka record bytes를 hash한다. normalize된 accepted
  envelope는 그 bytes를 그대로 재현하지 않으므로, K1.5는 fingerprint를 **보존해서 전달**하고 두
  파일에 모두 보이는 필드만 교차 검증한다. cryptographic payload-integrity chain은 주장하지 않는다.
  business metric만 조작하면 탐지되지 않는다. 다만 canonical CSV가 달라지므로 **identity가 달라지고**
  기존 trusted run을 재사용하지 않는다.
- **Filesystem 경계**: staging -> fsync -> 같은 local filesystem에서 rename. K1과 동일하게 local
  Linux filesystem에서만 검증했다. power-loss durability, NFS/object store, concurrent writer는
  주장하지 않는다.
- **Lineage 경계**: Kafka 컬럼은 source/bronze와 source hashing에 남는다. silver/gold는 기존 컬럼만
  project한다. column-level Kafka lineage는 주장하지 않는다.
- **Identity와 physical provenance 경계**: 같은 event/coordinate 집합은 batch grouping과 무관하게
  같은 CSV hash를 만든다. 하지만 grouping이 바뀌면 physical manifest path가 달라지므로, 같은 output
  root의 기존 version과 provenance가 다를 때는 stale provenance를 재사용하지 않고 consistency error로
  멈춘다.
- **Partition 경계**: K1.5는 한 `business_date`에서 topic/partition 조합 하나만 받는다. multi-partition
  landing은 ordering/rebalance 범위가 검증될 때까지 pipeline 호출 전에 거부한다.

## Alternatives

| Option | Why not K1.5 |
|---|---|
| Spark Structured Streaming | 이 압력에 없는 continuous/window 운영 surface를 끌어온다. |
| direct Kafka -> Iceberg sink | 기존 quality/catalog gate를 우회한다. |
| accepted.jsonl을 pipeline이 직접 읽게 변경 | 기존 CSV source 계약과 idempotency를 재설계해야 한다. |
| landing 시점에 CSV도 같이 쓰기 | K1 landing/offset 의미를 바꾸고 immutable 계약을 오염시킨다. |
| adapter identity = manifest batch_id 조합 | batch grouping이 바뀌면 identity가 흔들린다. |

## Claim boundary

말할 수 있는 것:

```text
한 business_date의 immutable accepted Kafka landing을 결정적 batch input으로 bridge했다.
event_id와 topic/partition/offset provenance를 source/bronze evidence에 보존했다.
기존 quality/gold pipeline과 source-hash rerun 계약을 재사용했다.
같은 accepted set 재실행이 trusted 결과를 두 배로 만들지 않는다.
```

말할 수 없는 것:

```text
continuous streaming pipeline
Spark Structured Streaming
direct Kafka-to-Iceberg sink
end-to-end exactly-once
column-level lineage
cryptographic end-to-end integrity
concurrent adapter writer correctness
production Kafka/Spark/Iceberg operation
```

## Evidence

- `src/manufacturing_data_platform/kafka_ingestion/batch_adapter.py`
- `tests/test_kafka_batch_adapter.py`
- `scripts/verify_kafka_k1_5.sh`
- `scripts/kafka_k1_5_verification.py`
- `VERIFICATION_LOG.md`
