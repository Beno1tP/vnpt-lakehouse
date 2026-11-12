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

## 6. Grants survive rebuilds (4.5 design check, 2026-11-11)

`make dbt CMD="run --select gold_uc5"` (drop + recreate), then `SHOW GRANTS FOR 'bi_ro'@'%'` → `GRANT SELECT ON ALL TABLES IN DATABASE gold` (stored as a wildcard) and `EXECUTE AS 'bi_ro'@'%'` → `SELECT count(*) FROM gold.gold_uc5` = 36: no grant step after dbt runs. dbt-starrocks 1.12.2 has no grant macros (repository tree read 11-11), so dbt's `grants:` config would not apply anyway.

## 7. Tag survives maintenance: drill on a scratch table (option C, 2026-11-12, pipeline mode)

Real tables untouched (Task 1 docs time-travel to untagged Bronze snapshots). `lake.scratch.tag_drill`: 3 inserts =
snapshots `4173304594530501193` (1 row), **`941002814889062600`** (2 rows, tag `drill_v1`, RETAIN 1 DAYS),
`5626552706193522083` (3 rows, main). `make maintenance TABLES=scratch.tag_drill SNAP_HOURS=0 RETAIN=1` (only that table
in the rendered arguments):

| Step | Result |
|---|---|
| expire_snapshots | `deleted_manifest_lists_count 1` (snapshot 1), `deleted_data_files_count 0` (its file is still referenced) |
| rewrite_data_files / rewrite_manifests | 3 files → 1, manifests 4 → 1: two new snapshots (`7189416442594192027`, `7658167447880813539` = main) |
| after | snapshots 4 = {tagged 9410…2600, 5626…2083, 7189…2027, 7658…3539}; rows 3 = before; oldest snapshot time travel: `gone (IllegalArgumentException)` |
| `scratch-check` | `drill_v1` → still `941002814889062600`, rows at tag 2, now 3: **PASS** |
| `scratch-drop` | table dropped with PURGE, namespace removed |

Theory: expire_snapshots removes by age and `retain_last` but never a snapshot a ref (tag/branch) points to while the
ref's retention lasts (365 d on the real `task*_v1` tags): the weekly `lake_maintenance` keeps every freeze tag.
Claude's prediction of 3 snapshots after was wrong (rewrite_manifests commits a snapshot too).
