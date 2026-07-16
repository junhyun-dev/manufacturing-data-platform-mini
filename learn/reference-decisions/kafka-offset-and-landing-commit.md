# Kafka Offset and Raw-Landing Commit Boundary

ADR Status: Implemented
상태: accepted local K1 decision

## Context

Kafka offset commit과 local JSONL file write를 하나의 transaction으로 묶을 수 없다.
offset을 먼저 commit하면 file write 실패 시 event를 잃고, file을 먼저 쓰면 commit 전
crash 시 같은 record가 다시 온다.

## Decision

K1은 at-least-once delivery를 선택하고 아래 순서를 사용한다.

```text
poll bounded records
-> validate/classify
-> write accepted/duplicate/quarantine JSONL + manifest to staging
-> fsync
-> atomic rename to immutable batch directory
-> synchronously commit next offset
```

commit 전 crash는 재전달로 복구한다. 재시도 시 manifest에서 Kafka coordinate와
fingerprint를 읽어 이미 durable한 record는 다시 accepted하지 않고 commit만 진행한다.

Landing index는 별도 mutable DB가 아니라 immutable batch manifest에서 재구성한다.
이 선택은 local small-data/single-writer scope에 맞춘 것이다.

## Failure states

| Failure point | Result | Recovery |
|---|---|---|
| staging write 전/중 | current batch 없음 | temp 제거 후 같은 offset 재처리 |
| atomic rename 후, offset commit 전 | durable batch 있음, offset 미진행 | coordinate reuse 후 offset commit |
| offset commit 후 | durable batch와 group progress 일치 | 다음 offset부터 처리 |
| invalid payload | quarantine batch로 durable | poison record의 다음 offset commit 가능 |

## Durability and recovery boundaries

- **Offset commit 계약**: Kafka offset은 compacted topic이나 transaction 때문에 gap이 생길 수 있으므로 연속성을 요구하지 않는다. K1 runtime은 `consumer.poll()`이 반환한 record를 누락 없이 증가 순서로 `land_records`에 넘기고, 마지막으로 처리한 record의 `offset + 1`을 commit한다. `land_records`는 증가 순서를 검사하지만 임의로 record를 뺀 subset까지 안전하게 만들어 주지는 않는다.
- **Filesystem 경계**: Python 문서상 같은 filesystem에서 성공한 `os.replace`는 atomic이고 `os.fsync`는 file descriptor의 write를 disk로 강제한다. 이 repo는 local Linux filesystem에서 이 순서만 검증했다. directory fsync까지 포함한 power-loss durability나 NFS/object-store portability는 주장하지 않는다.
- **주입 실패의 성격**: `SimulatedCrashAfterLanding`은 `os.replace`와 fsync 이후에 던지는 in-process 예외다. durable landing 뒤·commit 전 지점의 논리적 복구와 accepted-set 불변을 검증하지만, SIGKILL이나 전원 손실에 대한 crash consistency는 검증하지 않는다.

## Alternatives

| Option | Why not K1 |
|---|---|
| offset commit 후 file write | write 실패 시 silent loss |
| Kafka transaction | local filesystem sink와 원자적으로 묶이지 않음 |
| external state DB | K1 운영 surface와 two-phase inconsistency가 커짐 |
| Spark checkpoint | Spark Structured Streaming을 도입해야 하며 K1 pressure에 과함 |

## Claim boundary

말할 수 있는 것:

```text
at-least-once Kafka delivery에서 landing-before-commit 순서를 구현했다.
landing/commit 사이 crash를 주입했고, 재전달이 accepted set을 늘리지 않음을 검증했다.
bounded replay는 normal consumer-group progress를 바꾸지 않는다.
```

말할 수 없는 것:

```text
Kafka와 filesystem 사이 exactly-once transaction
concurrent landing writer correctness
multi-partition atomic batch
production recovery/retention operation
```

## Evidence

- `src/manufacturing_data_platform/kafka_ingestion/landing.py`
- `src/manufacturing_data_platform/kafka_ingestion/runtime.py`
- `tests/test_kafka_ingestion.py`
- `scripts/kafka_k1_verification.py`
- `VERIFICATION_LOG.md`

## References

- Apache Kafka 4.3.1 `KafkaConsumer`: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
- confluent-kafka 2.15.0 API: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
- Python `os.replace` / `os.fsync`: https://docs.python.org/3/library/os.html#os.replace , https://docs.python.org/3/library/os.html#os.fsync
