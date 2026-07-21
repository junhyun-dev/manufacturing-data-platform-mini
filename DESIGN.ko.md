# DESIGN 한국어판

원문: [`DESIGN.md`](DESIGN.md)

## 0. 목적

이 프로젝트는 합성 제조 데이터를 믿고 쓸 수 있는 지표로 바꾸고, 그 숫자가 어느
입력·실행·품질 판정에서 나왔는지 설명하고 재현할 수 있게 하는 local 데이터 플랫폼이다.
NoSQL/MongoDB catalog, dataset version manifest, data quality, lineage를 작은 실행 가능한
slice로 증명하는 것에서 시작해 bounded Kafka와 Spark/Iceberg 경로까지 확장했다.

큰 원칙:

```text
JD / benchmark
-> real-service scenario
-> state changes
-> required metadata
-> tables/files/functions/API
-> small executable slice
-> tests/docs
```

## 1. v0 scope

v0의 핵심은 catalog gate다.

포함:

- file ingest
- MongoDB catalog 등록
- version manifest
- `GET /datasets`
- `GET /datasets/{id}`

보류:

- graph lineage
- ownership/tags governance
- branching/atomic commit
- event stream
- auth/multitenancy

## 2. 서비스 골격

외부 OSS에서 무거운 구조를 그대로 복사하지 않고 필요한 패턴만 가져온다.

| Pattern | 적용 |
|---|---|
| resource-oriented API | `/datasets`, `/datasets/{id}` |
| env/config separation | env > `.env` > defaults |
| dependency via Docker Compose | MongoDB |
| schema evolution marker | document에 `schema_version` |

worker queue는 v0에 넣지 않는다. catalog loop를 먼저 작게 끝낸다.

## 3. 데이터 모델

핵심 결정은 `dataset`과 `dataset_version`을 분리하는 것이다.

```text
dataset = 논리적 데이터셋 정의
dataset_version = 특정 적재/스냅샷
```

예:

```json
{
  "dataset_id": "temp_sensor",
  "latest_version": "v3",
  "schema": [{"name": "timestamp", "type": "datetime"}]
}
```

```json
{
  "dataset_id": "temp_sensor",
  "version": "v3",
  "source_hash": "sha256(...)",
  "schema_hash": "sha256(...)",
  "row_count": 1000,
  "stats": {"null_counts": {"humidity": 12}}
}
```

이 분리가 reproducibility와 audit의 기반이다.

## 4. API contract

v0 core:

- `POST /datasets/{id}/ingest`
- `GET /datasets`
- `GET /datasets/{id}`

v0.5:

- `GET /datasets/{id}/extract?version=&columns=`

## 5. Phase 2 lakehouse slice

Phase 2는 catalog spine 위에 medallion pipeline을 붙인다.

```text
bronze = raw copy + manifest
silver = typed/normalized/deduped rows
gold = daily metrics at declared grain
quality = publish gate
catalog/lineage = run과 parent-child layer 기록
```

설계상 중요한 점:

- transform은 pure function
- write는 IO only
- quality result shape은 `{name, status, expected, actual, detail}`
- schema drift는 warn policy
- idempotency는 successful prior run 재사용

## 6. EAV mini slice

wide file이 여러 형태로 들어와도 config-driven mapping으로 표준화한다.

```text
wide CSV
-> mapping config
-> EAV long
-> pivot/aggregate gold
```

EAV row의 grain:

```text
entity_id + business_date + attribute + source_file_id
```

이 slice의 의미는 "multi-format intake를 pipeline code 변경 없이 처리한다"는 것이다.

## 7. Done 기준

작게 만들기의 완료 기준:

- CLI로 end-to-end 실행 가능
- tests가 quality/idempotency/schema drift를 검증
- README/DESIGN/ROADMAP claim이 code와 맞음
- 구현하지 않은 production 기능은 backlog로 명시

## 8. Kafka K1 bounded raw ingestion

Kafka K1은 CSV pipeline을 대체하지 않는 별도 source path이며 continuous streaming
service가 아니다.

```text
strict synthetic event v1
-> local Kafka topic (partition 1개, key=machine_id)
-> bounded consumer
-> accepted / duplicate / quarantine JSONL + manifest
-> fsync + atomic rename
-> manual next-offset commit
```

`event_id`는 business identity, `(topic, partition, offset)`은 transport evidence,
consumer-group committed offset은 progress다. K1은 at-least-once를 선택한다.
landing 뒤 commit 전 crash가 나면 같은 coordinate가 다시 오고, immutable manifest를
재사용해 accepted set을 늘리지 않은 채 commit한다.

검증 범위는 local broker 1개/partition 1개의 bounded ingestion, recovery, replay,
quarantine이다. continuous operation, multi-partition rebalance, multi-broker HA,
end-to-end exactly-once, Spark Structured Streaming, direct Iceberg streaming write는 미구현이다.

## 9. K1.5 landing-to-batch와 S7 Spark engine swap

K1.5는 Kafka landing을 direct streaming sink가 아니라 기존 batch spine에 연결한다.

```text
accepted JSONL + manifest
-> 결정적 canonical CSV + provenance
-> 기존 quality / gold / Iceberg 경로
```

adapter가 입력 계약과 CSV `source_hash`를 소유하고 기존 pipeline이 같은 hash를 재실행
멱등성에 사용한다. Spark는 raw Kafka JSONL을 다시 해석하지 않는다.

S7은 한 날짜의 transform engine만 Spark로 교체하면서 기존 계약을 유지한다.

```text
K1.5 canonical CSV + source_hash
-> Python과 parity를 맞춘 Spark silver/gold
-> 기존 quality suite
-> quality 통과 시 overwritePartitions() publish
```

설계 경계:

- `source_hash`, `run_id`, Iceberg `snapshot_id`는 서로 다른 identity다.
- quality 실패는 Iceberg write와 successful-run pointer 전진을 모두 막는다.
- 같은 source 재실행은 no-op이고, 정정은 대상 날짜 partition만 교체한다.
- 범위는 local bounded batch 하나와 Iceberg gold table 하나다. continuous streaming,
  cluster Spark, full Spark medallion rewrite, production Airflow는 증명하지 않는다.

## 10. S8 edge/cloud 단절 복구

S8은 단절된 edge 세션 하나를 모사해, **부분 복구 상태에서는 trusted downstream이 전진할 수
없음**을 증명한다.

```text
broker 없음 -> immutable 로컬 spool에 1..N append(fsync + atomic rename)
-> expected_last_sequence로 seal -> 재연결 후 기존 K1 landing으로 replay
-> event_id 기준 완결성 판정 -> 완결일 때만 기존 K1.5 batch/gold
```

코드보다 먼저 고정한 계약:

- identity 3분리: `(edge_source_id, boot_session_id, sequence_no)`(edge 순서) · `event_id`(business) · `(topic, partition, offset)`(transport).
- 완결성은 `event_id` 집합으로 판정한다. **Kafka offset 연속성으로 판정하지 않는다** — K1은 정상 offset gap을 허용한다.
- durable progress는 immutable entry 집합 자체다. 별도 mutable cursor를 두지 않는다.
- 봉인이 없으면 "아직 안 옴"과 "유실"을 구분할 수 없어 완결 선언 자체가 불가능하다.
- 미완결이면 `run_bridge` 호출 **전에** 실패해 adapter/lakehouse 산출물을 만들지 않는다.
- 반복 replay는 transport 증거만 늘리고 accepted 집합·`source_hash`·trusted gold는 바꾸지 않는다.

경계: local Linux filesystem 위의 synthetic·local·bounded·단일 machine/session/partition
시뮬레이션이다. edge gateway, OPC UA/MQTT/ROS 2/DDS, power-loss durability, concurrent writer,
production 운영은 아니다. 상세는 `learn/reference-decisions/edge-buffer-and-recovery-progress.md`.
