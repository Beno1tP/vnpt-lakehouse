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
| client messages | | | | | 10,613 |
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
