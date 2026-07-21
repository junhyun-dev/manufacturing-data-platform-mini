# 08. Edge/Cloud Recovery Slice (S8)

상태: Implemented / accepted-closed — Codex 독립 재검증 완료 (2026-07-21)

> 단절 구간을 로컬에 봉인해 모으고, 복구 후 그 구간이 전부 중앙에 반영됐을 때만 기존 batch를
> 돌리는 얇은 slice index다. 상세 결정은
> [`../../reference-decisions/edge-buffer-and-recovery-progress.md`](../../reference-decisions/edge-buffer-and-recovery-progress.md),
> 최신 실행 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)가 source of truth다.

## 1. Slice Thesis

```text
broker가 없는 동안 한 edge 세션을 immutable spool에 모아 봉인하고,
재연결 후 기존 K1 landing으로 replay하며,
봉인된 sequence 구간이 완전히 반영된 뒤에만 기존 K1.5 batch/gold 경로를 허용한다.
```

새 streaming 플랫폼이 아니라 **failure/recovery slice**다.

## 2. Primary Scenario

[`../scenarios/05-industrial-telemetry-recovery.md`](../scenarios/05-industrial-telemetry-recovery.md)

## 3. Core Questions

| Core question | 채택한 계약 |
|---|---|
| edge 순서는 무엇으로 식별하나? | `(edge_source_id, boot_session_id, sequence_no)` — Kafka 좌표와 분리 |
| "없음"과 "유실"을 어떻게 구분하나? | `expected_last_sequence`로 봉인. 봉인 없이는 완결 선언 불가 |
| durable progress는 무엇인가? | immutable entry 파일 자체. 별도 mutable cursor 없음 |
| 언제 buffered로 보나? | canonical bytes를 fsync + atomic rename 한 뒤 |
| replay 중복은 어떻게 흡수하나? | `event_id`. transport 증거는 늘어도 accepted 집합은 불변 |
| downstream은 언제 전진하나? | 봉인 구간 전부가 accepted일 때만. 미완결이면 `run_bridge` 호출 전 차단 |
| 완결성을 offset으로 판정하나? | **아니다.** K1은 정상 offset gap을 허용하고 두 space는 무관하다 |

## 4. 검증한 상태 전이

```text
broker 없음 상태에서 1..3 append -> seal(expected_last_sequence=3)
-> 재연결 후 1..2만 replay : accepted 2, missing [3], 승격 차단(산출물 없음)
-> 1..3 replay (새 offset) : accepted 3, missing [], 복구 완결
-> K1.5 processed, quality 통과
-> 1..3 재replay (새 offset): accepted 3 유지, K1.5 skipped, source_hash 불변
```

실제 runtime에서 edge sequence `[1,2,3]`이 Kafka offset `[0,1,4]`에 대응했다 — 두 space가
다르다는 직접 증거다.

## 5. Backlog / Not Claimed

```text
실제 edge gateway·하드웨어, product 수준 offline buffer
OPC UA / MQTT / ROS 2 / DDS 연동
continuous service, event-time/watermark, Flink/Structured Streaming
power-loss durability, NFS/object store, concurrent writer
multi machine/session/partition, rebalance ordering
production/HA/scale 운영, end-to-end exactly-once
digital twin · 이상탐지 · 예지보전 · 기계 제어
```

## 6. Evidence

- [`../../../src/manufacturing_data_platform/edge_recovery.py`](../../../src/manufacturing_data_platform/edge_recovery.py)
- [`../../../tests/test_edge_recovery.py`](../../../tests/test_edge_recovery.py)
- [`../../../scripts/verify_edge_recovery.sh`](../../../scripts/verify_edge_recovery.sh)
- [`../source-contracts/03-edge-recovery-envelope.md`](../source-contracts/03-edge-recovery-envelope.md)
- [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)

## 7. Next Questions

```text
late/out-of-order telemetry와 sequence gap을 별도 slice로 다룰 것인가?
여러 세션·여러 edge_source로 넓힐 실제 압력이 있는가?
two-system(landing commit ↔ spool seal) 원자성은 failure-state slice에서 다룰 것인가?
```
