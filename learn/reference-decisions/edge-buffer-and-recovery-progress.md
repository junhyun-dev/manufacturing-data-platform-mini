# Edge Buffer And Recovery Progress (S8)

ADR Status: Implemented
검토 상태: accepted-closed — Codex 독립 재검증 완료 (2026-07-21)

> code/test/local Kafka runtime으로 검증했고 Codex review의 H1/H2/M1/M2를 반영한 뒤
> 독립 재검증을 통과했다. 최신 테스트 수와 실행 결과는 `VERIFICATION_LOG.md`가 source of truth다.

## Context

현장과 중앙 사이 링크가 끊기면 수집을 멈출 수 없고, 복구 후에는 "무엇이 빠졌는지"를 말할 수
있어야 한다. 산업 플랫폼 문서 조사(`BENCHMARKS.md` §6)에서 AWS/Azure가 이 압력을 직접 다뤘고,
Azure의 "전달이 끝나기 전에는 source를 ack하지 않는다"는 K1의 *durable landing 전 offset commit
금지*와 유사한 안전 ordering이다.

S8의 압력은 하나다.

```text
단절 구간을 로컬에 durable하게 모으고, 복구 후 그 구간이 "전부" 중앙에 반영됐는지
증명할 수 있어야 한다. 증명되기 전에는 trusted batch 결과를 만들면 안 된다.
```

## Decision

```text
edge 순서 identity = (edge_source_id, boot_session_id, sequence_no)
business identity  = 기존 strict v1 event_id (payload 계약 불변)
transport evidence = (topic, partition, offset)
완결성 판정        = sealed 1..N의 모든 event_id가 중앙 accepted set에 존재하는가
```

세부 계약:

| 결정 | 내용 | 이유 |
|---|---|---|
| envelope | 기존 v1 payload를 **감싸는** edge envelope. payload에 필드를 추가하지 않는다 | source contract를 오염시키지 않는다 |
| durable append | staging → file fsync → atomic rename → parent dir fsync 이후에만 buffered로 본다 | 半쓰기 상태를 진행으로 착각하지 않는다 |
| progress | **immutable 파일 자체가 progress**다. 별도 mutable cursor를 두지 않는다 | cursor와 파일이 불일치할 여지를 없앤다 |
| seal | `expected_last_sequence`로 한 세션을 봉인. 봉인 없이는 완결성을 선언할 수 없다 | "없음"과 "유실"을 구분하는 유일한 근거 |
| 완결성 | offset 연속성이 **아니라** event_id 집합으로 판정 | K1은 정상 offset gap을 허용한다. 두 space는 무관하다 |
| 승격 gate | 미완결이면 `run_bridge` 호출 **전에** 실패. adapter/lakehouse 산출물을 만들지 않는다 | 부분 복구가 trusted state를 전진시키면 안 된다 |
| 반복 replay | transport evidence는 늘 수 있으나 accepted 집합·`source_hash`·trusted 결과는 불변 | producer 재전송이 사업 결과를 바꾸면 안 된다 |
| 식별자 | path-safe만 허용 | 경로 탈출 방지 |

### 왜 mutable cursor를 두지 않는가

"어디까지 보냈다"를 별도 파일로 관리하면 파일 실체와 cursor가 어긋나는 순간 어느 쪽이 진실인지
알 수 없다. S8은 **persist된 entry 집합 자체**를 progress로 삼는다. 이는 K1이 landing manifest를
progress로 삼고 별도 상태 DB를 두지 않은 결정과 같은 계열이다.

### 채택한 bounded 가정: `event_id`

```text
event_id는 machine-event v1에서 전역적으로 고유하고 불변인 business-event identity다.
같은 event_id에 다른 payload가 오는 것은 정정이 아니라 producer contract 위반이다.
```

"재전송이 accepted 집합을 늘리지 않는다"는 이 가정 위에서만 성립한다. **K1은 같은 `event_id`의
payload 동등성을 검사하지 않으므로 S8도 payload-equivalence 검증을 주장하지 않는다.** 서로 다른
물리 event에 같은 `event_id`가 재사용되면 coverage가 과대 보고되지만, 이는 v1 contract 위반이며
S8의 탐지 범위 밖이다.

### 세션 scope는 가정이 아니라 검증되는 불변식이다

S8은 한 세션에 machine 1개 · business_date 1개만 허용한다. 이를 문서상의 단순화로 두지 않고
**seal에 `machine_id`/`business_date`를 유도·기록**하고, (a) 혼합 값이 있으면 seal을 거부하며,
(b) 요청한 `business_date`가 sealed 세션 날짜와 다르면 **어떤 산출물도 만들기 전에** 승격을
거부한다. 그렇지 않으면 봉인 구간의 일부만 담긴 batch가 "완결"로 발행될 수 있다.

### 왜 offset 연속성으로 판정하지 않는가

K1은 compaction/transaction 등으로 생기는 **정상 offset gap**을 이미 허용한다(K1 offset 계약 참조).
edge sequence와 Kafka offset은 서로 다른 space라, 재전송 시 같은 event가 다른 offset에 앉는다.
실제 runtime evidence에서도 edge sequence `[1,2,3]`이 offset `[0,1,4]`에 대응했다.

## Failure states

| Failure point | Result | Recovery |
|---|---|---|
| 같은 좌표 · 같은 bytes | idempotent reuse | 정상 |
| 같은 좌표 · 다른 bytes | conflict 오류 | 손상된 spool 조사(덮어쓰지 않음) |
| 같은 event_id가 다른 sequence | 거부 | 생산 측 중복 조사 |
| seal 시 구간 누락 | seal 거부 | 누락 sequence를 먼저 append |
| seal 후 append / 다른 값으로 재seal | 거부 | 봉인된 세션은 불변 |
| 복구 미완결 상태의 승격 시도 | `run_bridge` 호출 전 차단, 산출물 없음 | 남은 sequence를 replay |
| 완결 후 반복 replay | duplicate evidence만 증가 | accepted/`source_hash`/gold 불변 |

## Boundaries

- **Durability 경계**: local Linux filesystem의 fsync + same-filesystem atomic rename까지만 검증했다. power-loss, NFS/object store, concurrent writer는 주장하지 않는다.
- **시뮬레이션 경계**: 실제 edge 하드웨어·게이트웨이·OT 프로토콜이 아니라, broker 부재 구간을 로컬 spool로 **모사**한 것이다.
- **범위 경계**: edge_source 1개 · boot_session 1개 · machine 1개 · business_date 1개 · topic/partition/consumer 1개 · single writer.
- **연속성 경계**: continuous service가 아니다. 한 번의 봉인된 세션을 bounded하게 복구한다.

## Alternatives

| Option | Why not S8 |
|---|---|
| mutable cursor 파일로 progress 관리 | 파일 실체와 불일치 위험(위 참조). |
| Kafka offset 연속성으로 완결성 판정 | offset gap이 정상이라 오탐/미탐이 발생. |
| payload v1에 sequence 필드 추가 | source contract를 깨고 K1 계약을 오염시킨다. |
| 실제 MQTT/OPC-UA broker 도입 | 이 압력을 푸는 데 불필요하고 scope를 폭발시킨다. |
| 미완결이어도 batch를 돌리고 나중에 정정 | trusted state가 먼저 전진해 "무음 손실"을 만든다. |

## Claim boundary

말할 수 있는 것:

```text
bounded local edge-recovery 시뮬레이션을 구현했다: immutable sealed spool,
실제 local Kafka broker를 통한 재전송, 미완결 시 downstream 차단,
완결·반복 replay에서 accepted 집합과 trusted 결과가 늘지 않음을 검증했다.
필수 수식어: synthetic · local · bounded · simulation · single machine/session/partition.
```

말할 수 없는 것:

```text
산업 IoT/자율공장 플랫폼 구축·운영
실제 edge gateway 또는 product 수준 offline buffer
OPC UA / MQTT / ROS 2 / DDS 연동
continuous·대규모 real-time streaming
power-loss-safe 또는 분산 durability
multi-partition ordering/rebalance correctness
production Kafka/Spark/Airflow 운영, end-to-end exactly-once
digital twin · 이상탐지 · 예지보전 · 기계 제어
```

## Evidence

- `src/manufacturing_data_platform/edge_recovery.py`
- `tests/test_edge_recovery.py`
- `scripts/verify_edge_recovery.sh`, `scripts/edge_recovery_verification.py`
- `learn/system-design/source-contracts/03-edge-recovery-envelope.md`
- `VERIFICATION_LOG.md`
