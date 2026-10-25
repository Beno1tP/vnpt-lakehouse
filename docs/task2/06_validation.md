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
| Backfill 07-02 → 07-25 (18 loaded, 6 skipped) | sums = totals above | | |
| Backfill ×2 → identical counts per day | | | |
