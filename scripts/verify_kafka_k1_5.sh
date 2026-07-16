#!/usr/bin/env bash
set -euo pipefail

# Bounded K1.5 bridge verification.
#
# Consumes the immutable K1 landing produced by ./scripts/verify_kafka_k1.sh.
# No broker and no Kafka client are needed: the adapter reads immutable files only.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LANDING_DIR="${LANDING_DIR:-/tmp/manufacturing-mini-kafka-k1-evidence/raw}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/manufacturing-mini-kafka-k1-5-evidence}"
BUSINESS_DATE="${BUSINESS_DATE:-2026-06-29}"
PYTHON_BIN="${PYTHON_BIN:-$REPO_ROOT/.venv/bin/python}"

if [[ ! -d "$LANDING_DIR" ]]; then
  echo "K1 landing not found at $LANDING_DIR" >&2
  echo "Run ./scripts/verify_kafka_k1.sh first." >&2
  exit 1
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python interpreter not found: $PYTHON_BIN" >&2
  exit 1
fi

PYTHONPATH="$REPO_ROOT/src" exec "$PYTHON_BIN" \
  "$REPO_ROOT/scripts/kafka_k1_5_verification.py" \
  --landing-dir "$LANDING_DIR" \
  --business-date "$BUSINESS_DATE" \
  --output-dir "$EVIDENCE_ROOT" \
  --clean
