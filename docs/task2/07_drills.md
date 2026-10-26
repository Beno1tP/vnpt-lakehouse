# Task 2 — Failure drills

Purpose: show that the pipeline recovers by itself from the failures it will meet, **without duplicate data**.
Day used: **2024-07-24** (largest day: 12,970 chat rows, 6,485 predict rows; the bronze driver runs ~20 s, long enough
to hit). Each drill re-queues that day's existing run (`REPROCESS=completed`), waits until the bronze pod is
`Running`, then breaks it. Proof of "no duplicates": the run's own `validate` task = exact per-day counts (23/23).

| # | Drill | Command | Expected recovery | Theory | Result |
|---|---|---|---|---|---|
| 1 | Kill the **driver** pod mid-run | `make drill-driver` | SparkApplication `FAILED` → `bronze` task fails → Airflow **retry** after `retry_delay` (5 min) → new SparkApplication (new random suffix, no `AlreadyExists`) → `COMPLETED`; `validate` 23/23 | One retry layer (A4): Spark app `restartPolicy: Never`, Airflow `retries=2`. Idempotent overwrite → a half-written first try cannot leave duplicates (Iceberg commit is atomic: no commit = no change) | ✅ 2026-10-26: driver deleted ~02:35:36 UTC → `bronze` attempt 3 `Task failed` (no rows line) → retry **02:40:44** (= 5 min `retry_delay`) → attempt 4 `12970 / 12970`, `6485 / 6485` in 22 s → `validate` success (exact counts: no duplicates) → `notify`; run `success` |
| 2 | Delete an **executor** pod mid-run | `make drill-executor` | Spark driver sees the executor lost, asks K8s for a new executor pod, re-runs the lost tasks; **same** Airflow attempt succeeds, no retry | Spark task-level fault tolerance: lineage lets lost partitions be recomputed; the driver (not Airflow) handles executor loss | ✅ 2026-10-26: executor deleted mid-run → driver log `Executor lost: 1 (epoch 0)`, `Lost executor 1 on 10.244.0.97: The executor with id 1 was deleted by a user…` → same attempt (5) finished 12,970 / 6,485 in 20 s; **no attempt 6** (no Airflow retry); validate success |
| 3 | Restart the **Airflow scheduler** mid-run | `make drill-scheduler` | The task process (LocalExecutor, inside the scheduler pod) dies; the Spark driver keeps running; the new scheduler finds the task without heartbeat → retry; `reattach_on_restart` finds the existing driver by its labels (dag/task/run) instead of submitting a second app → success, one load | Separation of orchestration and compute: Spark state lives in K8s, not in Airflow; labels make the work findable after a crash | ❌→fix 2026-10-26 (E41): scheduler restarted during bronze; old pod (RollingUpdate, 10 s grace) finished bronze, started validate, was killed → validate failed with `retries=0` → **run failed**; data OK (bronze complete, overwrite). Fix: grace 120 s + validate retries=1. ✅ **Rerun after the fix (revision 6):** scheduler restarted during bronze → bronze 03:46:30–03:46:53, validate 03:48:09–03:48:32 (75 s gap = scheduler hand-over), notify → run **success** |
| 4 | Backfill twice | `make backfill REPROCESS=completed` ×2 | identical per-day counts | idempotency | ✅ 2026-10-26: 18 days identical, 23/23 ×18 |
| 6 | **Lose the catalog database** (2.5b) | `make backup-now` → `make drill-lose-metastore CONFIRM=yes` → `make sparkapp APP=validate DATE=2024-07-24` (must FAIL) → `make restore-metastore TS=<ts>` → validate again | During the loss: HMS starts with an **empty** schema; Spark: table not found, although every Parquet/metadata file is still in MinIO. After the restore: same tables, `23/23` | The catalog only stores pointers (table → current `metadata.json`); losing it loses the *names*, not the data. RPO = time since the last backup: commits made after it are not visible after the restore (their `metadata.json` exist but the pointer is older) → drill 7 | |
| 7 | **Re-register a table from its files** (2.5b, next) | `CALL lake.system.register_table(…)` with the newest `metadata.json` | the table comes back with its latest commit, even without a catalog backup | Iceberg tables are self-describing: the metadata tree on S3 is the source of truth, the catalog is an index | |
| 5 | Forced failure → alert (2.6) | trigger `alert_smoke` | one ⏰ (deadline 30 s) + one ❌ (failure, final try), metadata only | | ✅ 2026-10-26: both received in Alert group; texts fixed in E44 |

How to read a drill afterwards:
```
make dag-runs | grep 2024-07-24
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- airflow tasks states-for-dag-run chatbot_bronze backfill__2024-07-24T00:00:00+00:00
make dag-report | grep 2024-07-24
```
