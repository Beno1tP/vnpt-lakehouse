#!/usr/bin/env bash
# Task 3 step 3.2: run the full DAG backfill (bronze -> validate -> silver -> validate_silver) twice, unattended,
# and prove idempotency: the set of distinct per-day report lines must be identical after pass 1 and pass 2.
# Start:  nohup bash tools/backfill_twice.sh > ~/backfill-overnight.log 2>&1 &
# Read:   tail -30 ~/backfill-overnight.log
# Results (counts only) in ~/data/vnpt-lab/evidence/task3-backfill-<UTC time>/, outside Git.
set -u
cd "$(dirname "$0")/.."
OUT=$HOME/data/vnpt-lab/evidence/task3-backfill-$(date -u +%Y%m%dT%H%MZ)
mkdir -p "$OUT"
AF=(kubectl -n airflow exec deploy/airflow-api-server -c api-server --)
stamp() { echo "$(date -u +%H:%M:%S) UTC ($(TZ=Asia/Ho_Chi_Minh date +%H:%M:%S) ICT)"; }

wait_idle() {  # REPROCESS=completed: old runs already show success, so wait for "nothing queued/running" twice in a row
  sleep 120
  local idle=0 active
  for _ in $(seq 1 150); do
    active=$("${AF[@]}" airflow dags list-runs chatbot_bronze -o plain | grep -cE " (queued|running) " || true)
    echo "[backfill] $(stamp) pass $1: active runs $active"
    if [ "$active" -eq 0 ]; then idle=$((idle + 1)); [ "$idle" -ge 2 ] && return 0; else idle=0; fi
    sleep 60
  done
  return 1
}

for pass in 1 2; do
  echo "[backfill] $(stamp) pass $pass: start"
  make --no-print-directory backfill REPROCESS=completed || { echo "[backfill] pass $pass: backfill create failed"; exit 1; }
  wait_idle "$pass" || { echo "[backfill] pass $pass: still active after 150 min, stopping"; exit 1; }
  make --no-print-directory dag-report > "$OUT/dag-report-pass$pass.txt"
  make --no-print-directory dag-runs > "$OUT/dag-runs-pass$pass.txt"
  failed=$("${AF[@]}" airflow dags list-runs chatbot_bronze -o plain | grep backfill__ | grep -c " failed " || true)
  echo "[backfill] $(stamp) pass $pass: done, failed backfill runs = $failed"
  sed -E 's/^ *[0-9]+ //' "$OUT/dag-report-pass$pass.txt" | sort -u > "$OUT/lines-pass$pass.txt"
  # E63: compare only the per-day DATA lines; the report also prints log noise and a growing file counter
  grep -E '^\[(landing|bronze|validate|silver|validate_silver)\]' "$OUT/lines-pass$pass.txt" > "$OUT/data-pass$pass.txt"
done

silver_ok=$(grep -c "validate_silver\] 28/28" "$OUT/data-pass2.txt" || true)
restarts=$(kubectl -n airflow get pods -l component=scheduler \
  -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="scheduler")].restartCount}')
echo "[backfill] distinct 'validate_silver 28/28' lines: $silver_ok (expect 18 data days); scheduler restarts: $restarts"
echo "[backfill] per-day data lines: pass 1 $(wc -l < "$OUT/data-pass1.txt"), pass 2 $(wc -l < "$OUT/data-pass2.txt")"
if diff -q "$OUT/data-pass1.txt" "$OUT/data-pass2.txt" >/dev/null; then
  echo "[backfill] IDEMPOTENT: pass 2 produced exactly the same per-day numbers as pass 1"
else
  echo "[backfill] DIFFERENT per-day data lines between the passes:"; diff "$OUT/data-pass1.txt" "$OUT/data-pass2.txt" | head -20
fi
echo "[backfill] $(stamp) finished; evidence in $OUT"
