# Source Contracts

상태: source input contract index

이 폴더는 pipeline이 받는 입력 단위, row grain, required columns, source identity를 정리한다.

## Contracts

1. [`01-manufacturing-csv.md`](01-manufacturing-csv.md)
   - v0 manufacturing-style batch CSV file의 입력 계약.
   - source row grain, required columns, natural key, `source_hash`, `schema_hash`를 정리한다.

## Boundary

현재 repo의 public evidence는 synthetic manufacturing-style/tabular source에 맞춘다.

```text
real company/customer schemas
private internal paths
ROS2/MCAP/session/sensor source contracts
```

위 항목들은 현재 source contract가 아니다.
