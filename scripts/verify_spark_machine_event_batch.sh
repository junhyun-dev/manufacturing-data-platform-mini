#!/usr/bin/env bash
set -euo pipefail

# Bounded S7 verification: K1.5 canonical landing -> local Spark batch -> Iceberg.
#
# Needs pyspark + the Iceberg runtime (requirements-spark.txt). No Kafka broker is
# required: the script builds in-process K1 landings with the real landing writer.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/manufacturing-mini-spark-machine-event-batch}"
PYTHON_BIN="${PYTHON_BIN:-python}"

if ! "$PYTHON_BIN" -c "import pyspark" >/dev/null 2>&1; then
  echo "pyspark not available for '$PYTHON_BIN'; install requirements-spark.txt or set PYTHON_BIN." >&2
  echo "runtime-not-run: Spark/Iceberg unavailable." >&2
  exit 3
fi

PYTHONPATH="$REPO_ROOT/src" exec "$PYTHON_BIN" \
  "$REPO_ROOT/scripts/spark_machine_event_batch_verification.py" \
  --output-dir "$OUTPUT_DIR" \
  --clean
