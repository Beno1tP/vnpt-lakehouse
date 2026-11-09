# Task 4 — Knowledge: StarRocks Gold with dbt

Plan: `03_plan.md` (review of the input plan in §R). Versions/RAM/disk: `02_config.md`.

---

## A1. StarRocks architecture

| Part | What it does |
|---|---|
| **FE** (Frontend, Java) | SQL parsing, cost-based planning, metadata, cluster management. Leader + followers replicate metadata (BDB-JE); observers scale reads. **9030** = MySQL protocol (clients, dbt, Superset), 8030 = HTTP |
| **BE** (Backend, C++) | Stores data and executes queries: columnar storage, **vectorized** execution (SIMD), MPP pipeline across BEs. 8040 HTTP, 9060 Thrift |
| CN (Compute Node) | Stateless compute for external tables / shared-data mode; not used here |
| Shared-nothing vs shared-data | Shared-nothing: BEs keep data on local disk (this lab). Shared-data: data in S3, CNs cache it |
| Protocol | **MySQL wire protocol**: any MySQL client/driver works (`mysql -h … -P 9030 -u root`, `pymysql`) |
| Memory | FE: JVM heap from `JAVA_OPTS` in `fe.conf` (the shipped default is large, ~8 GB: **must** be lowered for a 1–1.5 Gi pod). BE: no heap; `mem_limit` in `be.conf` caps its own allocator (default 90 % of what it detects) |

**Why StarRocks for Gold (vs Trino):** Trino reads files on every query. StarRocks keeps Gold in its own storage with sort keys, pre-aggregated tables, materialized views and caches, so a dashboard of 10 charts × N users answers in milliseconds. Trino = explore the lake; StarRocks = serve dashboards. Both read the same Iceberg Silver.

## A2. Table design

| Concept | Meaning | Use here |
|---|---|---|
| Table types | **Duplicate** (keep every row), **Aggregate** (same key → merged with SUM/MAX/BITMAP_UNION…), **Unique** / **Primary Key** (upsert by key) | `fct_user_message`: Primary Key on `message_id`; `gold_uc*`: Aggregate or plain Duplicate tables built by dbt |
| Partitioning | Range/list (usually date) → pruning + per-partition replace | `event_date` |
| Bucketing | `DISTRIBUTED BY HASH(col) BUCKETS n` → tablets | 1 bucket (≈ 10 k rows; more buckets = many tiny tablets) |
| Replicas | Default 3; **with 1 BE set `default_replication_num = 1` in `fe.conf`** and `replication_num: 1` per model, or CREATE TABLE fails | both |
| Async MV | Refreshes from base tables (also external catalogs), can **rewrite queries** | alternative to dbt aggregates (leader Q4) |
| Bitmap | `BITMAP` + `BITMAP_UNION` = **exact** distinct counts that merge across days (Roaring). Needs an **integer** key | distinct users per period (UC7/UC8) |
| HLL | approximate distinct (~1–2 % error) | not needed |

**Why bitmap:** distinct users is **non-additive**: users(Mon) + users(Tue) ≠ users(Mon–Tue). A bitmap per day answers any period exactly: `BITMAP_UNION_COUNT(users)` over the days in range.
Honest note: with 89 senders and ~10 k rows `COUNT(DISTINCT)` on the fact table is instant too. The bitmap is built because it is the pattern that still works at production scale, and because it lets the pre-aggregated `gold_uc*` tables answer any date range without going back to the fact table.

**Integer sender key:** `SENDER_ID` is text (2 are emails). `dim_sender` assigns a **stable** `sender_key`: an existing sender keeps its key on every rebuild. Build it as an **incremental** model (new senders get `max(key) + row_number()`), never `row_number()` over all senders per run (keys would move and old bitmaps would point at the wrong people). Hash keys (`murmur_hash3_32`) avoid the table but can collide → approximate counts.

## A3. Reading Silver from StarRocks (external catalog)

```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS lake_iceberg PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hive",
  "hive.metastore.uris" = "thrift://hive-metastore.lake.svc.cluster.local:9083",
  "aws.s3.endpoint" = "http://minio.lake.svc.cluster.local:9000",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.enable_ssl" = "false",
  "aws.s3.region" = "us-east-1",
  "aws.s3.access_key" = "<read-only user starrocks>",
  "aws.s3.secret_key" = "<from Secret>"
);
SELECT count(*) FROM lake_iceberg.silver.user_message;
```
- Names are 3-part: `catalog.database.table`; `default_catalog` = internal tables.
- Created by the init Job (credentials from a Secret), never typed with keys in a shell.
- StarRocks can also **write** Iceberg (INSERT into an Iceberg catalog), so the MinIO user must be read-only (same reason as Trino, Task 3 A3).
- Iceberg `timestamp` (NTZ, Task 3 D8) → StarRocks `DATETIME`: no zone shift.
- The catalog caches metadata; after a Silver write, a query may see the previous snapshot for a short time. `REFRESH EXTERNAL TABLE lake_iceberg.silver.user_message` before `dbt build` makes it deterministic.

## A4. dbt

| Concept | Meaning |
|---|---|
| Model | A `SELECT` in a `.sql` file; dbt wraps it in `CREATE TABLE/VIEW AS` (materialization) |
| `ref()` / `source()` | Dependencies → DAG and **lineage graph** |
| Materializations | view, table, incremental (`unique_key`), materialized_view |
| Tests | Generic: `unique`, `not_null`, `accepted_values`, `relationships`; **singular**: SQL returning failing rows (reconciliation vs expected) |
| Seeds | Small CSVs → tables: **expected values** become seeds, so reconciliation is a normal test |
| `dbt build` | seeds + models + tests in DAG order; a failing test skips downstream models |
| Docs | `dbt docs generate` → lineage graph and column docs |
| Profiles | `profiles.yml` with `env_var('STARROCKS_PASSWORD')`; never a literal password |

`dbt-starrocks` per-model config: `materialized`, `table_type` (`DUPLICATE`/`AGGREGATE`/`PRIMARY`), `keys`, `partition_by`, `distributed_by`, `buckets`, `properties: {"replication_num": "1"}`.

**Three-part source names:** dbt sources are `schema.table`. If `dbt-starrocks` does not render `lake_iceberg.silver.user_message`, add a thin **raw view layer** in the internal catalog (`CREATE VIEW raw.user_message AS SELECT * FROM lake_iceberg.silver.user_message`, created by the init Job) and point sources at it. **Test this first** (step 4.2).

## A5. Gold model (Kimball, 4 steps)

1. Business process: a citizen messages the bot; the bot predicts an intent.
2. Grain: **one client message** (`fct_user_message`, one row per chat `id` with `is_client`).
3. Dimensions: `dim_date`, `dim_sender` (`sender_key`, `is_internal`), `dim_intent` (intent, group, `is_understood`); channel as a degenerate attribute.
4. Facts: message count (additive), confidence (non-additive → AVG), `is_understood`, flags `is_home`, `is_internal_sender`.

`gold_uc*` = one aggregate per use case at the grain the dashboard needs, **keeping the filter flags as dimensions** so Task 5's toggle still works on pre-aggregated tables. Add one derived column **`traffic_class`** (`real` / `home` / `internal`) so Superset needs one native filter instead of two booleans.

## A6. Orchestration with Assets (Airflow 3)

The laptop cannot hold Spark and StarRocks at the same time (`02_config.md` §2), so Gold is a **separate DAG `gold_build`** scheduled on an **Asset** that `validate_silver` emits (`outlets=[Asset("lake://silver/user_message")]`). Airflow queues the asset event; `gold_build` starts with a `starrocks_ready` sensor (mode `reschedule`, so it holds no worker slot while waiting) and runs when StarRocks is up (gold mode). This is also the production pattern: one DAG per layer, linked by data, not by time.
