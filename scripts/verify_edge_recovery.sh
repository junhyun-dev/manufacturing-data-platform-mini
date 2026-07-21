#!/usr/bin/env bash
set -euo pipefail

# Bounded S8 verification: disconnected edge spool -> local Kafka replay -> K1.5 gate.
#
# Phase 1 runs with NO broker process (that is the point of the disconnection).
# Phase 2 reuses the shared pinned local-Kafka runbook; it is not copied here.
# Phase 3 uses the project .venv because the K1.5 batch path needs its dependencies.
# All runtime artifacts stay under /tmp.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/manufacturing-mini-s8-edge-recovery}"
PYTHON_BIN="${PYTHON_BIN:-$REPO_ROOT/.venv/bin/python}"
SCRIPT="$REPO_ROOT/scripts/edge_recovery_verification.py"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python interpreter not found: $PYTHON_BIN" >&2
  exit 1
fi

echo "== S8 phase 1/3: spool while disconnected (no broker running) =="
rm -rf "$OUTPUT_DIR"
if pgrep -f 'kafka\.Kafka' >/dev/null 2>&1; then
  echo "a Kafka broker process is already running; phase 1 must run disconnected" >&2
  exit 1
fi
PYTHONPATH="$REPO_ROOT/src" "$PYTHON_BIN" "$SCRIPT" --phase spool --output-dir "$OUTPUT_DIR"

echo
echo "== S8 phase 2/3: reconnect and replay through the local broker =="
# run_with_local_kafka.sh starts/stops the pinned broker and appends --bootstrap-servers.
STATE_ROOT="${STATE_ROOT:-/tmp/manufacturing-mini-s8-broker}" \
  "$REPO_ROOT/scripts/run_with_local_kafka.sh" "$SCRIPT" \
  --phase broker --output-dir "$OUTPUT_DIR"

echo
echo "== S8 phase 3/3: K1.5 promotion gate (processed -> skipped) =="
PYTHONPATH="$REPO_ROOT/src" "$PYTHON_BIN" "$SCRIPT" --phase promote --output-dir "$OUTPUT_DIR"

echo
echo "S8 edge recovery verification passed; evidence: $OUTPUT_DIR/edge_recovery_verification.json"
