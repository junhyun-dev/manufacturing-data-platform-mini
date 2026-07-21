# 05. Scenario — edge/cloud 단절 후 재연결 replay

상태: **Proposed** — 설계 후보일 뿐 구현되지 않았다. code/test/runtime evidence 없음.

> 이 문서는 다음 bounded slice **후보**를 시나리오 언어로 고정하기 위한 얇은 문서다.
> implementation package가 아니며, 구현·검증 전까지 어떤 claim도 만들지 않는다.
> 근거: [`../../reference-evidence/audit-inputs/2026-07-21-industrial-platform-direction/claude-audit.md`](../../reference-evidence/audit-inputs/2026-07-21-industrial-platform-direction/claude-audit.md)

## 1. 왜 이 시나리오인가

공식 산업 플랫폼 문서는 두 문제 lane을 보여준다(`BENCHMARKS.md` §6). AWS와 Azure는
**링크·destination 실패 중 데이터 연속성**을, Cognite와 HighByte는 **소스 간 식별과 명명**을
다룬다. 이 시나리오는 그중 첫 번째 lane만 선택한다.

- AWS IoT SiteWise Edge: 인터넷 단절 중에도 수집을 계속하고 복구 시 동기화.
- Azure IoT Operations: 전달이 실패하면 **source 메시지를 ack하지 않고** 큐에 남겨 재시도.

두 번째와 K1은 구현이나 delivery guarantee가 같지는 않지만, **durable한 downstream 결과가
생기기 전에 progress를 전진시키지 않는다**는 안전 원칙이 유사하다. 따라서 이 시나리오는
K1 계약을 단절 경계까지 확장할 수 있는지 검증하는 후보다.

## 2. Actor / Trigger

```text
Actor  : plant data operator (현장 데이터 담당)
Trigger: 현장 수집 지점과 중앙 처리 사이 링크가 일정 시간 끊겼다가 복구된다.
```

## 3. Desired outcome

```text
단절 구간의 데이터가 유실되지 않는다.
재연결 후 같은 구간이 중복 반영되지 않는다.
복구가 끝나기 전에는 trusted 결과(gold/current state)가 전진하지 않는다.
운영자가 "무엇이 언제 비었고 언제 채워졌는지" 설명할 수 있다.
```

## 4. Invariants (이 slice가 지켜야 할 것)

```text
edge buffer가 durable해지기 전에 edge 수집 진행 포인터를 전진시키지 않는다.
최초 복구는 누락된 고유 event 수만큼 accepted set을 늘린다.
같은 복구 구간을 다시 replay하면 accepted set이 더 늘지 않는다(identity로 흡수).
품질 검사를 통과하지 못한 복구분은 trusted state를 바꾸지 않는다.
복구 전후의 gold는 같은 grain·합계 계약을 유지한다.
```

## 5. Failure / recovery variants

```text
- 복구 도중 재실패        -> 마지막 durable 지점부터 재개, 중복은 identity로 흡수
- 단절 구간이 비어 있음    -> "빈 구간"과 "유실"을 구분해 evidence로 남긴다
- 복구분이 품질 실패       -> Iceberg write·success pointer 모두 막고 실패 evidence만 남긴다
- 부분 복구 후 정정 입력   -> 기존 partition overwrite/정정 계약(S3/S7)으로 흡수
```

## 6. 가장 작은 evidence (구현 시)

```text
단절을 주입한다 -> 로컬 buffer에 쌓인다 -> 복구한다 ->
누락된 고유 event만 accepted set에 추가되고 비었던 구간이 채워진다 ->
같은 구간을 다시 replay해도 accepted set이 더 늘지 않음을 test로 고정한다.
```

기존 자산 재사용: K1의 landing-before-commit·bounded replay, `source_hash` idempotency,
기존 quality gate, S3/S7의 partition overwrite. 첫 proof는 새 외부 의존성 없이 파일/큐 경계를
시뮬레이션할 수 있다. 이는 실제 edge gateway나 protocol runtime을 검증한다는 뜻이 아니다.

구현 전 Core 질문: 어떤 identity와 sequence(`event_id`, session/boot id, sequence number 등)로
"원래 event가 없었음"과 "전달 중 유실됨"을 구분할 것인가? 이 답 없이는 gap recovery를
claim할 수 없다.

## 7. Non-goals (이 시나리오에서 하지 않는 것)

```text
실제 OPC UA / MQTT / ROS 2 / DDS 연동
edge gateway 하드웨어, product 수준 offline buffer
continuous/event-time streaming, watermark, Flink / Spark Structured Streaming
asset hierarchy / Unified Namespace / digital twin
이상탐지 · 예지보전 · closed-loop 제어
production / HA / cluster 운영, 실시간 SLA
```

## 8. 상태

```text
Proposed. 구현 package 없음. code/test/runtime evidence 없음.
다음 게이트: Codex가 이 시나리오를 채택할지 판단 -> 채택 시 별도 slice/decision/test contract 설계.
```
