# 03. Edge Recovery Envelope v1 (S8)

상태: Implemented / accepted-closed — Codex 독립 재검증 완료 (2026-07-21)

역할: 단절 구간을 로컬에 모을 때 쓰는 **envelope 계약**. 기존 machine-event v1 payload는
바꾸지 않고 **감싸기만** 한다.

관련: [`02-kafka-machine-event-v1.md`](02-kafka-machine-event-v1.md) ·
[`../../reference-decisions/edge-buffer-and-recovery-progress.md`](../../reference-decisions/edge-buffer-and-recovery-progress.md)

## 1. Envelope

```text
format_version   : 1
edge_source_id   : path-safe non-empty identifier   ([A-Za-z0-9._-]+)
boot_session_id  : path-safe non-empty identifier   ([A-Za-z0-9._-]+)
sequence_no      : integer >= 1
event            : 기존 strict machine-event v1 payload (변경 없음)
```

canonical bytes = 위 5개 키를 `sort_keys` + 최소 구분자로 직렬화한 UTF-8. **wall-clock 값은
canonical bytes에 절대 들어가지 않는다** → 같은 내용이면 항상 같은 fingerprint.

## 2. Identity 분리

| Identity | 무엇을 가리키나 | 누가 정하나 |
|---|---|---|
| `(edge_source_id, boot_session_id, sequence_no)` | edge 순서·완결성 | edge 수집기 |
| `event_id` | business event 정체 | producer (v1 계약) |
| `(topic, partition, offset)` | Kafka transport 위치 | broker |

**완결성은 `event_id` 집합으로 판정한다. Kafka offset 연속성으로 판정하지 않는다** — K1은 정상
offset gap을 허용하고, 재전송 시 같은 event가 다른 offset에 앉는다.

### 채택한 bounded 가정 (`event_id`)

```text
event_id는 machine-event v1에서 전역적으로 고유하고 불변인 business-event identity다.
같은 event_id에 다른 payload가 오는 것은 "정정"이 아니라 producer contract 위반이다.
```

이 가정 위에서만 "재전송이 accepted 집합을 늘리지 않는다"가 성립한다. **K1은 같은 `event_id`의
payload 동등성을 검사하지 않으므로, S8도 payload-equivalence 검증을 주장하지 않는다.** 서로 다른
물리 event에 같은 `event_id`가 재사용되면 coverage가 과대 보고되지만, 그것은 v1 source contract
위반이며 S8의 탐지 범위 밖이다.

## 3. Spool 레이아웃

```text
<spool_root>/edge_source_id=<id>/boot_session_id=<id>/
    seq=00000000000000000001/entry.json     # 내용 = canonical bytes 그 자체
    seq=00000000000000000002/entry.json
    session_seal.json                        # 봉인 manifest
```

`entry.json`은 canonical envelope **그 자체**다. 읽을 때 파일명을 신뢰하지 않고 내용을 다시
canonicalize해 fingerprint와 `sequence_no`가 디렉터리명과 일치하는지 확인한다.

## 4. 거부 규칙

```text
같은 좌표 + 다른 canonical bytes        -> conflict
같은 event_id가 다른 sequence           -> conflict
path-safe 아닌 식별자                    -> 거부
seal 시 1..N 중 누락                     -> seal 거부
seal 후 append / 다른 값으로 재seal      -> 거부
같은 좌표 + 같은 bytes                   -> idempotent reuse (정상)
```

## 5. Seal

```text
expected_last_sequence : 이 세션이 끝난 지점
machine_id             : 세션에서 유도·기록된 단일 machine (혼합이면 seal 거부)
business_date          : 세션에서 유도·기록된 단일 날짜 (혼합이면 seal 거부)
완결 조건               : 1..expected_last_sequence 전 구간이 spool에 존재
seal manifest           : sequence_no / event_id / fingerprint 목록
```

**세션 scope는 검증되는 불변식이다.** `machine_id`/`business_date`를 seal에 유도·기록하고,
혼합 값이 있으면 seal을 거부한다. 승격 시 요청한 `business_date`가 sealed 날짜와 다르면
**어떤 산출물도 만들기 전에** 거부한다. load/재seal 시에는 seal의 `format_version`, 두 식별자,
`sealed_event_count`, 정확한 sequence/fingerprint 선언을 모두 재검증하고, **누락뿐 아니라 추가된
entry도 거부**한다(조용히 걸러내지 않는다).

봉인이 없으면 "아직 안 온 것"과 "유실"을 구분할 수 없으므로 **완결 선언 자체가 불가능**하다.

## 6. Claim boundary

```text
증명함  : local Linux filesystem에서 fsync + atomic rename 순서, 봉인된 한 세션의
          완결성 판정, 미완결 시 downstream 차단, 반복 replay 시 accepted 불변
증명 안 함: 실제 edge 하드웨어/게이트웨이, OPC UA·MQTT·ROS 2·DDS, power-loss durability,
          NFS/object store, concurrent writer, multi-partition, continuous service,
          production 운영
```

S8 단순화: edge_source 1 · boot_session 1 · machine 1 · business_date 1 · topic/partition 1 ·
single writer.
