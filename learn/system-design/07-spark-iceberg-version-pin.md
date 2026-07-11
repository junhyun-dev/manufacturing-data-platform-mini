# 07. Spark/Iceberg Version Pin

상태: implementation gate / design-only
프로젝트: `manufacturing-data-platform-mini`

> **STATUS: design-only.** 이 문서는 Spark/Iceberg walking skeleton 구현 전에 버전, jar, catalog 설정을 고정하기 위한 gate다. 아직 이 repo에는 Spark/Iceberg 구현 코드가 없다.

## 1. Why This Gate Exists

Spark/Iceberg skeleton에서 가장 깨지기 쉬운 부분은 Python 코드가 아니다.

깨지기 쉬운 부분은 아래 조합이다.

```text
PySpark version
-> Spark minor version
-> Scala binary version
-> Iceberg runtime jar artifact
-> Java version
-> SparkSession catalog/extension config
```

이 조합이 어긋나면 code path를 보기 전에 SparkSession 시작, table create, overwrite, metadata table read에서 실패한다.

그래서 구현 전에 먼저 하나의 conservative pin을 고정한다.

## 2. Reference Basis

확인 기준:

- Apache Spark 3.5.8 docs: https://spark.apache.org/docs/3.5.8/
  - Spark 3.5.8 문서는 Java 8/11/17, Scala 2.12/2.13, Python 3.8+를 지원한다고 설명한다.
- Apache Iceberg Spark Getting Started: https://iceberg.apache.org/docs/latest/spark-getting-started/
  - Iceberg latest는 1.11.0이고, Spark가 Iceberg operations에서 가장 feature-rich engine이라고 설명한다.
  - Spark catalog 설정은 `spark.sql.extensions`, `spark.sql.catalog.local`, `type=hadoop`, `warehouse` 조합을 사용한다.
- Apache Iceberg Multi-Engine Support: https://iceberg.apache.org/multi-engine-support/
  - Spark 3.5는 maintained 상태이고 latest Iceberg support는 1.11.0이다.
  - Spark 3.5 runtime jar는 `iceberg-spark-runtime-3.5_2.12` / `iceberg-spark-runtime-3.5_2.13` 계열로 제공된다.
- Maven Central artifact: https://central.sonatype.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12
  - `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0` artifact가 존재한다.

Local facts checked on 2026-07-11:

```text
Python: 3.10.12
Java: OpenJDK 17.0.19
PyPI pyspark versions include: 3.5.8, 3.5.7, 3.5.6, ...
```

## 3. Pinned Candidate

이번 walking skeleton의 기본 pin:

| Layer | Pin |
|---|---|
| Python | 3.10.12 local |
| Java | OpenJDK 17.0.19 local |
| PySpark | `pyspark==3.5.8` |
| Spark minor | `3.5` |
| Scala binary suffix | `2.12` |
| Iceberg | `1.11.0` |
| Iceberg runtime coordinate | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0` |
| Catalog | local hadoop catalog |
| Warehouse | `/tmp/manufacturing-mini-iceberg-warehouse` |
| Evidence output | `/tmp/manufacturing-mini-iceberg-evidence` |

Why Spark 3.5.x instead of Spark 4.1:

```text
Spark 4.1 is supported by Iceberg 1.11.0, but it is newer and uses Scala 2.13.
For this portfolio walking skeleton, the goal is not newest-runtime adoption.
The goal is a stable local demonstration of partition overwrite + snapshot evidence.

Spark 3.5.x is maintained by Iceberg, works with local Java 17, and has a clearly available 3.5_2.12 runtime jar.
```

Why `3.5.8`:

```text
PyPI currently exposes pyspark 3.5.8.
Apache Spark 3.5.8 docs exist.
Iceberg's Spark 3.5 runtime artifact is tied to the Spark minor line, not the patch number.
```

Risk:

```text
The Scala suffix must match the Spark distribution actually used by PySpark.
This pin assumes the PyPI Spark 3.5.x runtime is compatible with the 3.5_2.12 Iceberg runtime jar.
Test 0 must verify this by creating and reading a trivial Iceberg table.
```

## 4. SparkSession Config Draft

The skeleton should build SparkSession with this config:

```python
ICEBERG_RUNTIME_COORDINATE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"

spark = (
    SparkSession.builder
    .appName("manufacturing-mini-iceberg-skeleton")
    .master("local[2]")
    .config("spark.jars.packages", ICEBERG_RUNTIME_COORDINATE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .getOrCreate()
)
```

Fallback if `spark.jars.packages` fails:

```text
1. Resolve/download the exact jar locally.
2. Replace `spark.jars.packages` with `spark.jars=<local jar path>`.
3. Keep the same catalog and extension config.
4. Record the fallback in verification output.
```

## 5. Table and Write Contract

Target table:

```text
local.db.gold_daily_metrics
```

DDL shape:

```sql
CREATE NAMESPACE IF NOT EXISTS local.db;

CREATE TABLE IF NOT EXISTS local.db.gold_daily_metrics (
  business_date STRING,
  plant_id STRING,
  line_id STRING,
  product_code STRING,
  units_produced BIGINT,
  defect_count BIGINT,
  defect_rate DOUBLE
)
USING iceberg
PARTITIONED BY (business_date);
```

Write API:

```python
corrected_df.writeTo("local.db.gold_daily_metrics").overwritePartitions()
```

Avoid:

```text
SQL INSERT OVERWRITE without explicitly controlling dynamic partition overwrite mode.
```

Reason:

```text
The skeleton must prove D partition is replaced while D2 partition is preserved.
A whole-table overwrite can pass a one-date test but fail the real contract.
```

## 6. Environment Gate Command

Before implementing the full skeleton:

```bash
python -V
java -version
python -m pip install "pyspark==3.5.8"
```

Then Test 0 must start Spark with Iceberg config and run:

```text
create namespace
create trivial Iceberg table
insert one row
read one row
read snapshots metadata table
```

If this fails, the failure should be recorded with one of these concrete reasons:

```text
pyspark unavailable
Java incompatible
Iceberg runtime jar unavailable
Spark/Scala/Iceberg jar mismatch
catalog/warehouse config failed
metadata table syntax failed
```

## 7. Implementation Boundary

Allowed after this gate passes:

```text
single gold table walking skeleton
business_date partition overwrite
snapshot id evidence
run_id -> snapshot_id mapping
same source_hash rerun -> no new snapshot
different source_hash same business_date -> new snapshot
```

Still not allowed as a public claim:

```text
full lakehouse
production Spark pipeline
full medallion Spark rewrite
Iceberg rollback system
concurrent writer handling
Airflow runtime orchestration of Spark
Spark-based quality suite
```

## 8. Next Step

Next file/code target:

```text
requirements-spark.txt
src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py
tests/test_spark_iceberg_skeleton.py
```

The first implementation commit should only prove Test 0 and the partition overwrite contract from `06-spark-iceberg-walking-skeleton-plan.md`.
