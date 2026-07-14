# DESIGN 한국어판

원문: [`DESIGN.md`](DESIGN.md)

## 0. 목적

이 프로젝트는 NoSQL/MongoDB catalog, dataset version manifest, data quality, lineage를 작은 실행 가능한 slice로 증명한다.

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
