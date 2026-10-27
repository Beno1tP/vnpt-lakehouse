# Task 2 — Validation

## 1. Expected counts per event day (step 2.0, decision D3)

Computed with Spark SQL on the Task 1 Bronze tables (decision D3, before `make down`), independent of the landing simulator's pandas split; stored in `tests/expected/bronze_daily.json`. Day = date as written in `CREATED_TIME`; `bot_predict` by its own `CREATED_TIME`.

| Day | chatlog rows | bot_predict rows | unmatched predict |
|---|---|---|---|
| 2024-07-02 | 12 | 6 | 0 |
| 2024-07-03 | 2 | 1 | 0 |
| 2024-07-04 | 14 | 7 | 0 |
| 2024-07-05 | 4 | 2 | 0 |
| 2024-07-06 | 18 | 9 | 0 |
| 2024-07-08 | 12 | 6 | 0 |
| 2024-07-09 | 86 | 43 | 0 |
| 2024-07-10 | 14 | 7 | 0 |
| 2024-07-11 | 10 | 5 | 0 |
| 2024-07-15 | 36 | 18 | 0 |
| 2024-07-16 | 156 | 78 | 0 |
| 2024-07-17 | 94 | 48 | 0 |
| 2024-07-18 | 186 | 93 | 0 |
| 2024-07-19 | 40 | 20 | 0 |
| 2024-07-22 | 6 | 3 | 0 |
| 2024-07-23 | 12 | 6 | 0 |
| 2024-07-24 | 12,970 | 6,485 | 0 |
| 2024-07-25 | 7,552 | 3,892 | 116 |
| **Total (18 days)** | **21,224** | **10,729** | **116** (= Task 1) |

Observed 2026-10-25: 0 parse failures (no NULL day); `unmatched_same_day` = `unmatched_any_day` = 116, all on 2024-07-25, so a per-day anti-join gives the same number as the global one. Empty days (skipped, D2 = A): **07-07, 07-12, 07-13, 07-14, 07-20, 07-21**. 07-24 + 07-25 hold 20,522 of 21,224 chat rows (96.7 %).

## 2. Results

| Check | Expected | Actual | Date |
|---|---|---|---|
| One day via SparkApplication (2.2) | 2024-07-02: 12 / 6 | 12 / 6, validate 10/10 | 2026-10-25 |
| Big days 2024-07-24 / 07-25 (2.2e) | 12,970 / 6,485 · 7,552 / 3,892 (116 unmatched) | equal, validate 10/10 each | 2026-10-25 |
| Rerun same day → `overwrite`, total unchanged | added = deleted = 12 / 6 | 16/16 with `--check-idempotency` | 2026-10-25 |
| Task 1 `make acceptance` after `bronze_ingest.py` change | PASSED | 17/17, 23/23, PASSED, exit 0 | 2026-10-25 |
| Backfill 07-02 → 07-25 (18 loaded, 6 skipped) | sums = totals above | 18 days rows = expected, 23/23 each; 6 days skipped; 25 runs success | 2026-10-26 |
| Backfill ×2 → identical counts per day | same rows, 23/23 | identical on 18 days × 2 tables, 23/23 ×18 (backfill_id 1 and 2) | 2026-10-26 |
| From zero (`minikube delete` → `make k8s-up` → `make k8s-demo`), run 2 after E52 | 18 days loaded = expected, 6 skipped, 0 failed, scheduler 0 restarts | landing 18 days × 2 OK (21,224 / 10,729); 36 bronze lines = expected; 23/23 ×18; 0 failed; 0 restarts; demo 16m08s | 2026-10-27 |
| Rerun 07-24 after a restart (E53) by `tasks clear`, with resource watch (`04_process.md`) | 12,970 / 6,485, 23/23; node requests back to idle | 12,970 / 6,485, 23/23; requests 4036 → ~7236 → 4036 Mi; Spark pods deleted after the run | 2026-10-27 |

## 3. Automation: runs nobody triggered (decision C, 2026-10-27)

Scheduling is proven by runs the scheduler (or the K8s CronJob controller) created on its own. A scheduled run that finds **new** data cannot be shown: the data is from July 2024, so history is loaded by backfill (the same scheduler executes backfill runs, with the same dependencies, retries and alerts; only the dates are chosen by a person). Event-driven runs (file arrival → run) = option A, revisit in Task 3.

| Automation | Trigger | Evidence | Status |
|---|---|---|---|
| `chatbot_bronze` daily load | Airflow scheduler, `CronDataIntervalTimetable` 00:00 UTC | `scheduled__2026-10-27T00:00` (run_after 10-27, logical date 10-26), started 02:05:46Z right after the unpause, `success`; `check_landing` found no 2026-10-26 export → skipped (D2) | ✅ 2026-10-27 |
| `lake_maintenance` weekly | Airflow scheduler, Sunday 02:00 UTC | `scheduled__2026-10-25T02:00` (logical date 10-18), 02:21:37 → 02:22:08Z, `success`; with `catchup=False` only the latest missed week ran | ✅ 2026-10-27 |
| `pg-backup` nightly | K8s CronJob 01:00 UTC, `startingDeadlineSeconds` 12 h | CronJob created 2026-10-27T01:38:15Z (after that day's slot), `lastScheduleTime` empty, `suspend=false` → first run **2026-10-28 01:00 UTC**. Check: `kubectl -n lake get jobs \| grep pg-backup` → `Complete 1/1` | ⏳ pending 10-28 |
