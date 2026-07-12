#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_VENV="${AIRFLOW_VENV:-/tmp/manufacturing-mini-airflow-venv}"
AIRFLOW_HOME="${AIRFLOW_HOME:-/tmp/manufacturing-mini-airflow-standalone-home}"
WAREHOUSE="${WAREHOUSE:-/tmp/manufacturing-mini-airflow-standalone-iceberg-warehouse}"
EVIDENCE_DIR="${EVIDENCE_DIR:-/tmp/manufacturing-mini-airflow-standalone-iceberg-evidence}"
LOG_FILE="${LOG_FILE:-/tmp/manufacturing-mini-airflow-standalone.log}"
RUN_ID="${RUN_ID:-standalone_iceberg_$(date -u +%Y%m%d_%H%M%S)}"

AIRFLOW_BIN="$AIRFLOW_VENV/bin/airflow"
PYTHON_BIN="$AIRFLOW_VENV/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Airflow venv python: $PYTHON_BIN" >&2
  echo "Create it with: python -m venv $AIRFLOW_VENV" >&2
  exit 1
fi

cd "$REPO_ROOT"

echo "Installing Airflow, project, and Spark dependencies into $AIRFLOW_VENV"
"$PYTHON_BIN" -m pip install -r requirements-airflow.txt
"$PYTHON_BIN" -m pip install -r requirements.txt -r requirements-spark.txt

rm -rf "$AIRFLOW_HOME" "$WAREHOUSE" "$EVIDENCE_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER="$REPO_ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="$REPO_ROOT/src"
export PATH="$AIRFLOW_VENV/bin:$PATH"

echo "Starting Airflow standalone; log: $LOG_FILE"
setsid airflow standalone >"$LOG_FILE" 2>&1 &
AIRFLOW_PID=$!

cleanup() {
  if kill -0 "$AIRFLOW_PID" >/dev/null 2>&1; then
    kill -INT "-$AIRFLOW_PID" >/dev/null 2>&1 || true
    for _ in $(seq 1 20); do
      if ! kill -0 "$AIRFLOW_PID" >/dev/null 2>&1; then
        wait "$AIRFLOW_PID" >/dev/null 2>&1 || true
        return
      fi
      sleep 1
    done
    kill -TERM "-$AIRFLOW_PID" >/dev/null 2>&1 || true
    sleep 2
    kill -KILL "-$AIRFLOW_PID" >/dev/null 2>&1 || true
    wait "$AIRFLOW_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

started=false
for _ in $(seq 1 60); do
  if ! kill -0 "$AIRFLOW_PID" >/dev/null 2>&1; then
    echo "Airflow standalone exited early. Recent log:" >&2
    tail -n 80 "$LOG_FILE" >&2 || true
    exit 1
  fi

  http_code="$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/ || true)"
  if [[ "$http_code" == "200" ]]; then
    started=true
    break
  fi
  sleep 2
done

if [[ "$started" != "true" ]]; then
  echo "Airflow standalone did not return HTTP 200 on 127.0.0.1:8080. Recent log:" >&2
  tail -n 120 "$LOG_FILE" >&2 || true
  exit 1
fi

echo "Airflow API server returned HTTP 200"
"$AIRFLOW_BIN" dags list | grep -E "manufacturing_(lakehouse_daily|iceberg_skeleton)"
"$AIRFLOW_BIN" dags list-import-errors > /tmp/manufacturing-mini-airflow-import-errors.txt
if ! grep -q "No data found" /tmp/manufacturing-mini-airflow-import-errors.txt; then
  echo "DAG import errors were reported:" >&2
  cat /tmp/manufacturing-mini-airflow-import-errors.txt >&2
  exit 1
fi

"$AIRFLOW_BIN" dags unpause manufacturing_iceberg_skeleton >/tmp/manufacturing-mini-airflow-unpause.txt
"$AIRFLOW_BIN" dags trigger manufacturing_iceberg_skeleton \
  --run-id "$RUN_ID" \
  --conf "{\"warehouse\":\"$WAREHOUSE\",\"output_dir\":\"$EVIDENCE_DIR\",\"clean\":true}" \
  --output json >/tmp/manufacturing-mini-airflow-trigger.json

echo "Triggered manufacturing_iceberg_skeleton run_id=$RUN_ID"

"$PYTHON_BIN" - "$AIRFLOW_HOME/airflow.db" "$RUN_ID" <<'PY'
import sqlite3
import sys
import time

db_path = sys.argv[1]
run_id = sys.argv[2]
dag_id = "manufacturing_iceberg_skeleton"

observed = []
for _ in range(60):
    with sqlite3.connect(db_path) as con:
        dag_row = con.execute(
            "select state from dag_run where dag_id = ? and run_id = ?",
            (dag_id, run_id),
        ).fetchone()
        task_row = con.execute(
            "select state from task_instance where dag_id = ? and run_id = ?",
            (dag_id, run_id),
        ).fetchone()

    dag_state = dag_row[0] if dag_row else "<missing>"
    task_state = task_row[0] if task_row else "<no-task-yet>"
    pair = (dag_state, task_state)
    if not observed or observed[-1] != pair:
        observed.append(pair)
        print(f"state: dag={dag_state} task={task_state}", flush=True)

    if dag_state in {"success", "failed"}:
        break
    time.sleep(5)

if not observed or observed[-1] != ("success", "success"):
    raise SystemExit(f"Expected final state success/success, observed {observed!r}")

print("state_transition:", " -> ".join(f"{dag}/{task}" for dag, task in observed))
PY

"$PYTHON_BIN" - "$EVIDENCE_DIR" <<'PY'
import json
import sys
from pathlib import Path

evidence_dir = Path(sys.argv[1])
run_snapshot_map = json.loads((evidence_dir / "run_snapshot_map.json").read_text())
current_gold = json.loads((evidence_dir / "current_gold.json").read_text())
snapshot_comparison = json.loads((evidence_dir / "snapshot_comparison.json").read_text())

assert run_snapshot_map["partition_overwrite_assertions"]["snapshot_increment"] == 1
assert run_snapshot_map["partition_overwrite_assertions"]["same_source_created_snapshot"] is False
assert run_snapshot_map["partition_overwrite_assertions"]["target_partition_row_count"] == 1
assert len(current_gold["rows"]) == 2
assert snapshot_comparison["corrected_snapshot_count"] == 2

print("evidence files: run_snapshot_map.json, current_gold.json, snapshot_comparison.json")
print("snapshot_increment: 1")
print("same_source_created_snapshot: false")
print("current_gold_rows:", len(current_gold["rows"]))
PY

echo "Airflow standalone scheduler verification passed."
