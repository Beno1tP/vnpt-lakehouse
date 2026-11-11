# Task 4 — dbt lineage and query plans (step 4.6)

## 1. Lineage

`make dbt-docs` → `dbt docs generate --static` in a one-off pod (namespace gold-jobs, user dbt_rw) → one self-contained
HTML file (manifest + catalog embedded) copied to `~/data/vnpt-lab/evidence/task4-dbt-docs-<UTC>/lineage.html`
(outside Git; open it in a browser, no server needed). Shape (13 models, 6 seeds, 3 sources, 34 tests):

```
sources lake_iceberg.silver.{chat_message, nlu_prediction, user_message}     (3-part names, no raw.* views needed)
  → stg_chat_message, stg_nlu_prediction, stg_user_message                  (views; Silver gate assert_silver_counts)
  → dim_date (24 days) · dim_intent (3) · dim_sender (incremental, stable sender_key)
  → fct_prediction (10,729, PK + day partitions) · fct_user_message (10,613)
  → gold_uc4 · gold_uc5 · gold_uc6 (from fct_prediction) · gold_uc7 · gold_uc8 (BITMAP users, from dim_sender/fct_user_message)
seeds expected_gold_uc4…uc8 (pandas) → tests reconcile_uc4…uc8; scenario_labels → gold_uc4
```

## 2. Gold table vs external catalog (2026-11-11, gold mode)

```sql
EXPLAIN SELECT event_date, sum(not_understood_cnt) FROM gold.gold_uc5 WHERE traffic_class = 'real' GROUP BY event_date;
EXPLAIN SELECT event_date, count(*) FROM lake_iceberg.silver.nlu_prediction
        WHERE final_intent IN ('fallback','bot_khong_hieu') GROUP BY event_date;
```

| Query | Scan | Plan facts | Rows the planner expects |
|---|---|---|---|
| UC5 on Gold `gold_uc5` | internal OLAP table (columnar tablets on the BE) | `partitions=1/1`, `tabletRatio=1/1` | **9** (the `real` rows of 36 pre-aggregated rows) |
| Same question on Silver via `lake_iceberg` | Iceberg scan: HMS metadata + Parquet files from MinIO | no partition/tablet facts (external files) | **7,153** (estimate: an external table has no statistics, so the filter selectivity is a default guess; 10,729 rows are read) |

Gold = the aggregation is done once by `dbt build`; a dashboard query reads a few pre-computed rows from local
columnar storage. Querying Silver through the catalog works (same Iceberg files Trino reads) but reads and groups all
predictions on every query. That is the OLAP-vs-lake split of Task 3 (Trino explores) and Task 4 (StarRocks serves).

## 3. Bitmap distinct users (gold_uc8)

```sql
SELECT user_class, bitmap_union_count(users) AS exact_users, sum(active_users) AS sum_of_daily
FROM gold.gold_uc8 GROUP BY user_class;
```

| user_class | exact users (bitmap union over all days) | sum of daily distinct users | double counted |
|---|---|---|---|
| external | **87** | 106 | 19 |
| internal | **2** | 6 | 4 |
| total | **89** (= Task 1's distinct senders, = gold_uc7 on 07-25) | 112 (= 24 days × 4.67, metrics.md) | 23 |

Distinct counts are not additive: a person active on several days is counted once per day by a sum. A bitmap of the
stable integer `sender_key` per day can be unioned over ANY range of days and still counts each person once (UC7:
"một người chỉ được đếm một lần"). With 89 users `COUNT(DISTINCT)` on the fact table would be instant too; the bitmap
is the pattern that keeps pre-aggregated tables exact at production scale (01_knowledge.md A2).

## 4. Time travel by tag in StarRocks (2026-11-11, gold mode)

| Query (through `lake_iceberg`) | Result | Meaning |
|---|---|---|
| `SELECT count(*) FROM lake_iceberg.silver.nlu_prediction FOR VERSION AS OF 'task3_v1'` | 10,729 | StarRocks 3.5.21 resolves an Iceberg **tag** (not only a snapshot id) |
| `SELECT count(step) … FOR VERSION AS OF 'task3_v1'` | 0 | the old snapshot is read with the **current** table schema: `step` (added in 4.0a, after `task3_v1`) exists and is NULL for every old row (Iceberg schema evolution: columns are matched by field id, a missing field reads as NULL) |

Consequence for the freeze (4.6 stage 3): Gold can be rebuilt FROM a tag by pointing the staging views at
`FOR VERSION AS OF '<tag>'`; a tag taken before a schema change still answers with today's columns (NULL-filled).
