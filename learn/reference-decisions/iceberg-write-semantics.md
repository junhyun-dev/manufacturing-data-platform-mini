# Iceberg Write Semantics 의사결정 노트

ADR Status: Implemented
상태: accepted local walking-skeleton decision
프로젝트: `manufacturing-data-platform-mini`

> **Scope status:** local single-gold-table Spark/Iceberg walking skeleton은 구현됐다.
> full Spark medallion rewrite, production lakehouse operation, concurrent writer handling, retention/rollback은 아직 Backlog다.
> 최신 runtime/test 결과는 [`../../VERIFICATION_LOG.md`](../../VERIFICATION_LOG.md)가 source of truth다.

이 노트는 Slice2에서 "table에 write를 어떻게 commit하나"를 다룬다. `02-slice2-question-map.md`의 #1(ACID) + #2(write semantics) + #10(idempotency)을 하나로 수렴한다. 이게 Slice2의 심장이다.

관련 문서/코드:

- [`../system-design/04-slice2-spark-iceberg-shift.md`](../system-design/04-slice2-spark-iceberg-shift.md) (§3.1 run_id≠snapshot_id, §3.2 layer 모델)
- [`../system-design/02-slice2-question-map.md`](../system-design/02-slice2-question-map.md)
- [`schema-drift.md`](schema-drift.md) (같은 포맷의 exemplar)
- `src/manufacturing_data_platform/pipeline/lakehouse.py` (`find_existing_successful_run`, `persist_catalog`)

## 1. 시나리오

같은 `business_date`의 manufacturing CSV가 **다시** 들어온다.

```text
- retry: 앞 run이 중간에 실패해서 다시 돌린다.
- backfill: 과거 날짜를 다시 처리한다.
- correction: 원본이 틀려서 정정된 파일로 다시 처리한다.
```

Slice1은 이걸 "이미 성공한 (dataset_id, business_date, source_hash) run이 있으면 **skip**"으로 막았다. Slice2에서는 bronze/silver/gold가 Iceberg table이다. 그럼 write를 어떻게 commit하나?

## 2. 문제

Iceberg table에 write하는 방식에 따라 결과가 달라진다.

위험:

- 매번 **append**하면 같은 날짜 재처리 시 gold row가 중복된다.
- table 전체 **overwrite**하면 다른 `business_date` partition까지 날아간다.
- 파일명/run_id 기준으로는 "같은 입력인지"를 보장할 수 없다.
- Slice1의 **skip**은 중복은 막지만, *정정된 데이터로 다시 돌리는 것*까지 막아버린다.
- run과 commit을 같은 것으로 착각하면 lineage가 틀어진다. (한 run이 silver/gold 두 commit을 만든다.)

## 3. 선택지

| 선택지 | 장점 | 비용 / 위험 |
|---|---|---|
| append-only | 단순, 이력 다 남음 | 재처리 시 중복, dedup을 downstream으로 미룸 |
| whole-table overwrite | table이 항상 깨끗 | 다른 날짜 partition 손실, 병렬 불가 |
| **partition atomic overwrite** (해당 business_date만) | 그 날짜만 원자적 교체, 다른 날짜 안전 | partition 경계를 잘 잡아야 함 |
| MERGE INTO (upsert) | late-arriving row를 정밀 반영 | key 기반, 복잡 — v0엔 과함 |
| skip existing (Slice1) | 중복 0, 재실행 안전 | 정정 재처리 불가 |

## 4. 상용/OSS의 의사결정

참고 pattern:

- **Iceberg / Delta**는 재처리를 "조건/partition 단위 atomic overwrite"로 다룬다 (Delta `replaceWhere`, Iceberg dynamic partition overwrite / `REPLACE PARTITIONS`). 매 commit이 하나의 **snapshot**이다.
- **dbt** incremental의 `insert_overwrite`도 같은 사고: partition 단위로 갈아끼운다.
- 공통 일반 결정:

```text
재처리는 append가 아니라 "그 조각만 원자적으로 교체"로 다룬다.
교체해도 이전 결과는 snapshot으로 남아 재현/비교가 가능하다.
commit(snapshot)과 pipeline run은 다른 단위다.
```

## 5. Tradeoff

이 프로젝트 v0의 결정: **skip 기본 + 정정 시 partition atomic overwrite**, 그리고 **run이 snapshot을 참조**.

| 얻는 것 | 비용 / tradeoff |
|---|---|
| 같은 source_hash 재실행은 여전히 no-op (Slice1 contract 유지) | 두 경로(skip / overwrite)를 코드가 구분해야 함 |
| 정정된 source는 그 날짜 partition만 원자적으로 교체 (다른 날짜 안전) | partition 경계를 business_date로 고정 |
| overwrite해도 이전 gold가 snapshot으로 남아 비교/재현 가능 (time travel demo) | snapshot이 쌓임 — retention/expire는 v0 backlog |
| run metadata가 snapshot id를 참조해 lineage가 정확 | run당 여러 snapshot id를 기록해야 함 |

핵심 구분:

```text
skip                = 같은 내용(source_hash 동일) 재실행 -> 아무 것도 안 함
partition overwrite = 정정된 내용(source_hash 다름) 재실행 -> 그 business_date partition만 교체
```

## 6. Row / File / Record Trace

| 순간 | table/record | key fields | 예시 | 의미 |
|---|---|---|---|---|
| 1차 처리 | gold_daily_metrics | business_date, gold_snapshot_id | `D, snap=S1` | 최초 gold commit |
| catalog | lakehouse_runs | run_id, gold_snapshot_id | `R1 -> S1` | run이 snapshot 참조 |
| 같은 파일 재실행 | (skip) | source_hash | `hash 동일 -> skipped` | no-op, S1 그대로 |
| 정정 파일 재실행 | gold_daily_metrics | business_date=D partition | `overwrite D -> snap=S2` | 그 날짜만 교체 |
| catalog | lakehouse_runs | run_id, gold_snapshot_id | `R2 -> S2` | 새 run, 새 snapshot 참조 |
| history | table snapshots | S1, S2 | `S1(이전), S2(현재)` | 재처리 전후 비교 가능(demo) |

## 7. State Changes

```text
source CSV(business_date=D) 도착
-> source_hash 계산
-> (dataset_id, D, source_hash) 성공 run 조회
   -> 있으면: skip, prior run_id 반환                 (Slice1 그대로)
   -> 없으면: 처리 진행
      -> transform_silver / transform_gold (순수)
      -> silver_events: D partition atomic overwrite  -> silver_snapshot_id
      -> gold_daily_metrics: D partition atomic overwrite -> gold_snapshot_id
      -> quality checks
      -> lakehouse_runs.run_id 기록 + silver_snapshot_id / gold_snapshot_id 참조
```

## 8. 살아남아야 하는 정보

```text
dataset_id
business_date
source_hash                 (idempotency key — snapshot이 대체하지 않음)
run_id                      (파이프라인 실행 단위)
bronze_snapshot_id
silver_snapshot_id
gold_snapshot_id            (run이 참조하는 table commit들)
quality checks
run status
```

이유: `source_hash`는 "같은 내용인가"를, snapshot id들은 "이 run이 어떤 table 상태를 만들었나"를 말한다. 둘을 함께 남겨야 skip/overwrite 판단과 lineage가 동시에 선다.

## 9. Tables / Columns / Files

```text
source_archive/               원본 CSV 그대로 (immutable)
warehouse/
  bronze_events               Iceberg table (raw-preserving)
  silver_events               Iceberg table (typed/deduped), partition by business_date
  gold_daily_metrics          Iceberg table (mart), partition by business_date
lakehouse_runs (mongo/json)
  run_id, dataset_id, business_date, source_hash,
  silver_snapshot_id, gold_snapshot_id, quality, status
```

## 10. Functions / APIs (초안 — 구현 전)

```text
find_existing_successful_run(dataset_id, business_date, source_hash)   # Slice1 재사용, skip 판단
write_partition_overwrite(table, df, business_date)                    # 신규: 그 partition만 atomic 교체
current_snapshot_id(table)                                            # 신규: commit 후 snapshot id 읽기
persist_run(run_id, silver_snapshot_id, gold_snapshot_id, ...)         # lakehouse_runs에 참조 기록
```

transform_silver / transform_gold는 **순수 함수 유지** (rows/DataFrame -> DataFrame). write만 Iceberg로.

## 11. 설계 판단 (Copy / Simplify / Avoid)

Copy:

- partition 단위 atomic overwrite (재처리 = 그 조각만 교체).
- `source_hash`를 idempotency key로 유지.
- run이 snapshot id를 **참조**(대체 아님).
- 재처리 전후 snapshot이 남는다.

Simplify:

- MERGE INTO 대신 partition 전체 overwrite (v0 데이터 작음).
- retention/expire 없이 snapshot 그냥 쌓이게 둔다.
- Iceberg catalog는 로컬 file 기반(hadoop) 하나만.

Avoid (v0):

- whole-table overwrite.
- production rollback / RESTORE 운영.
- 동시 writer / concurrent commit 처리.
- late-arriving row upsert(MERGE).

## 12. My Project v0 (local contract)

```text
같은 source_hash 재실행이면:
  skip, prior run_id 반환, 새 snapshot 없음.

정정된 source(다른 source_hash) 재실행이면:
  해당 business_date partition만 atomic overwrite.
  그 partition의 gold row 수 = 재계산값 (중복 없음).
  다른 business_date partition은 안 바뀜.
  새 gold_snapshot_id 생성, 이전 snapshot은 table history에 남음.
  lakehouse_runs.run_id가 새 silver/gold snapshot id를 참조.

run_id != snapshot_id:
  한 run이 silver/gold snapshot을 각각 만들고, run이 그것들을 참조한다.
```

## 13. Test Contract (먼저 작성 — playbook Test-First)

```text
[idempotent skip]
given business_date=D의 gold가 이미 있음 (gold_snapshot=S1)
when 같은 source_hash로 다시 실행
then status=skipped
and  gold snapshot은 여전히 S1 (새 commit 없음)

[correction -> partition atomic overwrite]
given business_date=D, gold_snapshot=S1
when 정정된 파일(다른 source_hash)로 실행
then D partition의 gold row 수 == 재계산값 (중복 없음)
and  다른 business_date partition은 그대로
and  새 gold_snapshot_id S2, S2 != S1
and  lakehouse_runs의 새 run_id가 S2를 참조

[time travel demo]
given S1(재처리 전 gold), S2(재처리 후 gold) 둘 다 table history에 존재
when VERSION AS OF S1로 읽음
then 재처리 전 gold를 그대로 재현 (운영 복구 주장 아님, 비교/재현만)
```

Slice1의 golden test(input 5 -> silver 3 -> gold 1 -> quality pass/pass/warn)는 Iceberg 위에서도 재현되어야 한다.

## 14. Claim Boundary

정직하게 말할 수 있는 것:

```text
같은 business_date 재처리를 Iceberg partition atomic overwrite로 다뤄 중복 없이 교체했다.
source_hash idempotency로 같은 입력은 skip, 정정 입력은 partition만 교체한다.
run metadata가 Iceberg snapshot id를 참조해 lineage를 잇는다.
재처리 전후 snapshot이 남아 이전 결과를 재현/비교할 수 있음을 test로 확인했다.
```

말하면 안 되는 것:

```text
snapshot이 pipeline run을 대체한다 (틀림 — 참조 관계다).
production rollback / restore를 운영한다.
concurrent writer / 대규모 분산 commit을 다룬다.
MERGE 기반 실시간 upsert를 한다.
```

## 15. 면접 답변 (30-60초)

> Slice2에서 같은 날짜 재처리를 어떻게 다룰지가 핵심이었습니다. Slice1에서는 같은 source_hash면 skip했는데, 정정된 데이터로 다시 돌려야 할 때가 있어서, Iceberg에서는 그 business_date partition만 atomic overwrite하도록 했습니다. 그러면 그 날짜만 원자적으로 교체되고 다른 날짜는 안전하며, 이전 결과는 snapshot으로 남아 재현/비교가 됩니다. 중요한 건 run과 snapshot을 구분한 점입니다. run_id는 우리 파이프라인 실행 단위로 남기고, 거기에 silver/gold snapshot id를 참조로 기록해 lineage를 이었습니다. snapshot이 run을 대체하는 게 아니라 run이 snapshot을 참조하는 구조입니다. time travel은 운영 복구 기능으로 과장하지 않고, 재처리 전후 비교를 확인하는 정도로만 뒀습니다.

## 16. 다음에 같이 볼 질문

바로 구현하지 말고, 아래를 같이 정한다.

1. default를 skip으로 둘까, "정정 파일이면 자동 overwrite"까지 default에 넣을까? (안전 vs 편의)
2. `bronze_events`는 append-only인가, silver/gold처럼 partition overwrite인가? (raw-preserving의 정확한 의미)
3. partition을 `business_date` 하나로 둘까, `plant_id`도 추가할까? (pruning 이득 vs small files)
4. dynamic partition overwrite가 이 pyspark/iceberg 버전에서 실제로 되나 — 아니면 명시적 DELETE+INSERT? (**walking skeleton에서 확인할 것**)
5. quality fail이면 gold commit을 막을까(fail run), commit하고 warn만 남길까?
