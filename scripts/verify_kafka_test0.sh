#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_VERSION="4.3.1"
SCALA_VERSION="2.13"
KAFKA_ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_SHA512="C7D7B2318CB51AA0C61D3246A51C349210073C5C9B754947EF965A439F2F939E8600F204E134A75AC31FAF3829C9370960EF7C6A9886C8A1DBF0339A21F4C54C"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"

DOWNLOAD_DIR="${DOWNLOAD_DIR:-/tmp/manufacturing-mini-kafka-downloads}"
DIST_ROOT="${DIST_ROOT:-/tmp/manufacturing-mini-kafka-dist}"
STATE_ROOT="${STATE_ROOT:-/tmp/manufacturing-mini-kafka-test0}"
KAFKA_VENV="${KAFKA_VENV:-/tmp/manufacturing-mini-kafka-venv}"
BROKER_ADDRESS="${BROKER_ADDRESS:-127.0.0.1:19092}"
CONTROLLER_ADDRESS="${CONTROLLER_ADDRESS:-127.0.0.1:19093}"

ARCHIVE_PATH="$DOWNLOAD_DIR/$KAFKA_ARCHIVE"
KAFKA_HOME="$DIST_ROOT/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
CONFIG_PATH="$STATE_ROOT/server.properties"
BROKER_LOG="$STATE_ROOT/broker.log"
EVIDENCE_PATH="$STATE_ROOT/evidence.json"
PYTHON_BIN="$KAFKA_VENV/bin/python"
KAFKA_PID=""

cleanup() {
  if [[ -n "$KAFKA_PID" ]] && kill -0 "$KAFKA_PID" >/dev/null 2>&1; then
    kill -INT -- "-$KAFKA_PID" >/dev/null 2>&1 || true
    for _ in $(seq 1 20); do
      if ! kill -0 "$KAFKA_PID" >/dev/null 2>&1; then
        wait "$KAFKA_PID" >/dev/null 2>&1 || true
        return
      fi
      sleep 1
    done
    kill -TERM -- "-$KAFKA_PID" >/dev/null 2>&1 || true
    sleep 2
    kill -KILL -- "-$KAFKA_PID" >/dev/null 2>&1 || true
    wait "$KAFKA_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mkdir -p "$DOWNLOAD_DIR" "$DIST_ROOT"
if [[ ! -f "$ARCHIVE_PATH" ]]; then
  echo "Downloading $KAFKA_URL"
  curl --fail --location --retry 3 --output "$ARCHIVE_PATH" "$KAFKA_URL"
fi

actual_sha512="$(sha512sum "$ARCHIVE_PATH" | awk '{print toupper($1)}')"
if [[ "$actual_sha512" != "$KAFKA_SHA512" ]]; then
  echo "Kafka archive SHA-512 mismatch" >&2
  echo "expected: $KAFKA_SHA512" >&2
  echo "actual:   $actual_sha512" >&2
  exit 1
fi
echo "Kafka archive checksum verified: $KAFKA_ARCHIVE"

if [[ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
  tar -xzf "$ARCHIVE_PATH" -C "$DIST_ROOT"
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  python3 -m venv "$KAFKA_VENV"
fi
"$PYTHON_BIN" -m pip install -r "$REPO_ROOT/requirements-kafka.txt"

rm -rf "$STATE_ROOT"
mkdir -p "$STATE_ROOT"

"$PYTHON_BIN" - "$KAFKA_HOME/config/server.properties" "$CONFIG_PATH" "$STATE_ROOT/logs" "$BROKER_ADDRESS" "$CONTROLLER_ADDRESS" <<'PY'
from pathlib import Path
import sys

source = Path(sys.argv[1])
target = Path(sys.argv[2])
log_dirs = sys.argv[3]
broker = sys.argv[4]
controller = sys.argv[5]

updates = {
    "controller.quorum.bootstrap.servers": controller,
    "listeners": f"PLAINTEXT://{broker},CONTROLLER://{controller}",
    "advertised.listeners": f"PLAINTEXT://{broker},CONTROLLER://{controller}",
    "log.dirs": log_dirs,
}
found = set()
output = []
for line in source.read_text().splitlines():
    key = line.split("=", 1)[0]
    if key in updates:
        output.append(f"{key}={updates[key]}")
        found.add(key)
    else:
        output.append(line)

missing = set(updates) - found
if missing:
    raise SystemExit(f"Kafka server.properties is missing expected keys: {sorted(missing)}")
target.write_text("\n".join(output) + "\n")
PY

cluster_id="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
"$KAFKA_HOME/bin/kafka-storage.sh" format --standalone \
  -t "$cluster_id" \
  -c "$CONFIG_PATH"

echo "Starting Kafka $KAFKA_VERSION at $BROKER_ADDRESS"
setsid "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG_PATH" >"$BROKER_LOG" 2>&1 &
KAFKA_PID=$!

started=false
for _ in $(seq 1 60); do
  if ! kill -0 "$KAFKA_PID" >/dev/null 2>&1; then
    echo "Kafka exited before becoming ready. Recent log:" >&2
    tail -n 100 "$BROKER_LOG" >&2 || true
    exit 1
  fi
  if "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BROKER_ADDRESS" \
    --list >/dev/null 2>&1; then
    started=true
    break
  fi
  sleep 1
done

if [[ "$started" != "true" ]]; then
  echo "Kafka did not become ready at $BROKER_ADDRESS. Recent log:" >&2
  tail -n 120 "$BROKER_LOG" >&2 || true
  exit 1
fi

echo "Kafka broker is ready"
PYTHONPATH="$REPO_ROOT/src" "$PYTHON_BIN" \
  "$REPO_ROOT/scripts/kafka_test0_roundtrip.py" \
  --bootstrap-servers "$BROKER_ADDRESS" \
  --output "$EVIDENCE_PATH"

echo "Kafka Test 0 passed; evidence: $EVIDENCE_PATH"
