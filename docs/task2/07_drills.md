# Task 2 — Failure drills

Purpose: show that the pipeline recovers by itself from the failures it will meet, **without duplicate data**.
Day used: **2024-07-24** (largest day: 12,970 chat rows, 6,485 predict rows; the bronze driver runs ~20 s, long enough
to hit). Each drill re-queues that day's existing run (`REPROCESS=completed`), waits until the bronze pod is
`Running`, then breaks it. Proof of "no duplicates": the run's own `validate` task = exact per-day counts (23/23).

| # | Drill | Command | Expected recovery | Theory | Result |
|---|---|---|---|---|---|
| 1 | Kill the **driver** pod mid-run | `make drill-driver` | SparkApplication `FAILED` → `bronze` task fails → Airflow **retry** after `retry_delay` (5 min) → new SparkApplication (new random suffix, no `AlreadyExists`) → `COMPLETED`; `validate` 23/23 | One retry layer (A4): Spark app `restartPolicy: Never`, Airflow `retries=2`. Idempotent overwrite → a half-written first try cannot leave duplicates (Iceberg commit is atomic: no commit = no change) | ✅ 2026-10-26: driver deleted ~02:35:36 UTC → `bronze` attempt 3 `Task failed` (no rows line) → retry **02:40:44** (= 5 min `retry_delay`) → attempt 4 `12970 / 12970`, `6485 / 6485` in 22 s → `validate` success (exact counts: no duplicates) → `notify`; run `success` |
| 2 | Delete an **executor** pod mid-run | `make drill-executor` | Spark driver sees the executor lost, asks K8s for a new executor pod, re-runs the lost tasks; **same** Airflow attempt succeeds, no retry | Spark task-level fault tolerance: lineage lets lost partitions be recomputed; the driver (not Airflow) handles executor loss | |
| 3 | Restart the **Airflow scheduler** mid-run | `make drill-scheduler` | The task process (LocalExecutor, inside the scheduler pod) dies; the Spark driver keeps running; the new scheduler finds the task without heartbeat → retry; `reattach_on_restart` finds the existing driver by its labels (dag/task/run) instead of submitting a second app → success, one load | Separation of orchestration and compute: Spark state lives in K8s, not in Airflow; labels make the work findable after a crash | |
| 4 | Backfill twice | `make backfill REPROCESS=completed` ×2 | identical per-day counts | idempotency | ✅ 2026-10-26: 18 days identical, 23/23 ×18 |
| 5 | Forced failure → alert (2.6) | bad date param | one Telegram message, no data contents | | ⏳ 2.6 |

How to read a drill afterwards:
```
make dag-runs | grep 2024-07-24
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- airflow tasks states-for-dag-run chatbot_bronze backfill__2024-07-24T00:00:00+00:00
make dag-report | grep 2024-07-24
```
