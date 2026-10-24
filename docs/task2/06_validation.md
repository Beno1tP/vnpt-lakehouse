# Task 2 — Validation

## 1. Expected counts per event day (step 2.0, decision D3)

Computed independently of the job (DuckDB), stored in `tests/expected/bronze_daily.json`. Day = date as written in `CREATED_TIME`; `bot_predict` by its own `CREATED_TIME`.

| Day | chatlog rows | bot_predict rows | unmatched predict |
|---|---|---|---|
| (to fill) | | | |
| **Total** | **21,224** | **10,729** | **116** (must equal Task 1) |

## 2. Results

| Check | Expected | Actual | Date |
|---|---|---|---|
| One day via SparkApplication (2.2) | per-day table | | |
| Rerun same day → `overwrite`, total unchanged | | | |
| Task 1 `make acceptance` after `bronze_ingest.py` change | PASSED | | |
| Backfill 07-02 → 07-25 (18 loaded, 6 skipped) | sums = totals above | | |
| Backfill ×2 → identical counts per day | | | |
