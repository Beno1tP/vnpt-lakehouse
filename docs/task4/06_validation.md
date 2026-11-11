# Task 4 — Validation

## 1. Expected Gold values (step 4.0)

Source of truth: `dbt/seeds/expected_gold_uc*.csv` from `tools/expected_gold.py` (pandas). Definitions: `docs/metrics.md`. Every UC is checked with `traffic_class` filter **on** (real only) and **off** (all).

| UC | Metric | All traffic | Real only (no `/home`, no internal) |
|---|---|---|---|
| UC4 | messages per day (definition to confirm) | | |
| UC5 | understood / not understood per day | | |
| UC6 | top-N `final_intent` | | |
| UC7 | distinct users, last `window_days` vs previous | | |
| UC8 | distinct users per day (definition to confirm) | | |

## 2. Layer-to-layer counts

| Count | Silver (Spark) | Trino | StarRocks catalog | `fct_user_message` | Expected |
|---|---|---|---|---|---|
| client messages | 10,613 | 10,613 | **10,613** (`starrocks-init` as dbt_rw, 11-11; `gold.stg_user_message` via dbt 3-part source) | | 10,613 |
| with prediction | | | | | |

## 3. dbt build results

| Run | Models | Tests | Result |
|---|---|---|---|

## 4. Idempotency

| Check | Result |
|---|---|
| `dbt build` twice → identical `gold_uc*` | |
| `dim_sender` keys unchanged after `--full-refresh` of downstream models | |
| `gold_build` started by the Asset event (no manual trigger) | |

## 5. Step 4.2 checks (2026-11-11)

| Check | Result |
|---|---|
| `make s3-access-test` | 5 profiles × 6/6; `starrocks`: read lakehouse only (after E70) |
| `make starrocks-init` | 16/16 statements OK (catalog, database, users, grants incl. REFRESH, E72); dbt_rw reads 10,613 rows; bi_ro CREATE + INSERT → privilege error (E71: only an `Access denied` counts) |
| dbt 3-part source | `source('silver','user_message')` with `database: lake_iceberg` renders `lake_iceberg.silver.user_message`: `dbt run` created view `gold.stg_user_message`, count 10,613 → no `raw.*` views needed (A4 open point closed) |
| `dbt debug` | connection OK; `git [ERROR]` only (no git in the image on purpose: hub packages do not need it) → use `debug --connection` |
