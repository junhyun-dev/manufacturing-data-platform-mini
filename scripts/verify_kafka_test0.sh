#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export STATE_ROOT="${STATE_ROOT:-/tmp/manufacturing-mini-kafka-test0}"
EVIDENCE_PATH="${EVIDENCE_PATH:-$STATE_ROOT/evidence.json}"

exec "$REPO_ROOT/scripts/run_with_local_kafka.sh" \
  "$REPO_ROOT/scripts/kafka_test0_roundtrip.py" \
  --output "$EVIDENCE_PATH"
