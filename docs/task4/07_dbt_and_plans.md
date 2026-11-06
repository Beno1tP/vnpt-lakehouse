# Task 4 — dbt lineage and query plans (step 4.6)

## 1. Lineage

`make dbt-docs` → screenshot of the lineage graph (`docs/task4/img/lineage.png`). Expected shape:

```
source lake_iceberg.silver.{chat_message, nlu_prediction, user_message} (or raw.* views)
  → stg_* → dim_date, dim_sender (incremental), dim_intent → fct_user_message → gold_uc4 … gold_uc8
seeds expected_gold_* → tests reconcile_uc*
```

## 2. Gold table vs external catalog

```sql
EXPLAIN SELECT event_date, sum(understood_cnt), sum(not_understood_cnt)
FROM gold.gold_uc5 WHERE traffic_class = 'real' GROUP BY event_date;

EXPLAIN SELECT event_date, sum(CASE WHEN final_intent NOT IN ('fallback','bot_khong_hieu') THEN 1 ELSE 0 END)
FROM lake_iceberg.silver.user_message WHERE NOT is_home AND NOT is_internal_sender GROUP BY event_date;
```

| Query | Scan node | Rows scanned | Time (`SET enable_profile = true`, then Query Profile) |
|---|---|---|---|
| `gold_uc5` | OlapScanNode | | |
| Silver via catalog | IcebergScanNode | | |

Explain (2–3 lines): internal table = local columnar tablets, pre-aggregated; external = metadata from HMS + Parquet from MinIO on every query.

## 3. Bitmap distinct users

```sql
SELECT BITMAP_UNION_COUNT(users) FROM gold.gold_uc8 WHERE event_date BETWEEN '2024-07-20' AND '2024-07-25';
SELECT sum(daily_users)            FROM gold.gold_uc8 WHERE event_date BETWEEN '2024-07-20' AND '2024-07-25';  -- wrong on purpose
```
Show both numbers: the gap is the double counting of users active on several days.
