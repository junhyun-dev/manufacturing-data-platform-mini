#!/usr/bin/env bash
set -euo pipefail

# Bounded S9 verification: sealed edge session -> local Kafka replay -> recovery gate
# -> existing Spark/Iceberg publish -> retry no-op.
#
# Reuses the existing pinned assets: scripts/run_with_local_kafka.sh owns the broker,
# and the S7 module owns Spark/Iceberg. Nothing is copied here.
# Phase 1 runs with NO broker (that is the disconnection). All artifacts stay under /tmp.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/manufacturing-mini-s9-verification}"
PYTHON_BIN="${PYTHON_BIN:-python}"
SCRIPT="$REPO_ROOT/scripts/recovered_telemetry_publish_verification.py"

if ! "$PYTHON_BIN" -c "import pyspark" >/dev/null 2>&1; then
  echo "pyspark not available for '$PYTHON_BIN'; install requirements-spark.txt or set PYTHON_BIN." >&2
  echo "runtime-not-run: Spark/Iceberg unavailable." >&2
  exit 3
fi

echo "== S9 phase 1/3: spool and seal while disconnected (no broker running) =="
if pgrep -f 'kafka\.Kafka' >/dev/null 2>&1; then
  echo "a Kafka broker process is already running; phase 1 must run disconnected" >&2
  exit 1
fi
PYTHONPATH="$REPO_ROOT/src" "$PYTHON_BIN" "$SCRIPT" \
  --phase spool --output-dir "$OUTPUT_DIR" --clean

echo
echo "== S9 phase 2/3: replay through the local broker; partial publish must be blocked =="
STATE_ROOT="${STATE_ROOT:-/tmp/manufacturing-mini-s9-broker}" \
  "$REPO_ROOT/scripts/run_with_local_kafka.sh" "$SCRIPT" \
  --phase broker --output-dir "$OUTPUT_DIR"

echo
echo "== S9 phase 3/3: recovery-gated Spark/Iceberg publish, then retry =="
PYTHONPATH="$REPO_ROOT/src" "$PYTHON_BIN" "$SCRIPT" --phase publish --output-dir "$OUTPUT_DIR"

echo
echo "S9 recovery-gated publish verification passed; evidence: $OUTPUT_DIR/s9_verification.json"
