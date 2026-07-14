#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export STATE_ROOT="${STATE_ROOT:-/tmp/manufacturing-mini-kafka-k1-broker}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/manufacturing-mini-kafka-k1-evidence}"

exec "$REPO_ROOT/scripts/run_with_local_kafka.sh" \
  "$REPO_ROOT/scripts/kafka_k1_verification.py" \
  --output-dir "$EVIDENCE_ROOT"
