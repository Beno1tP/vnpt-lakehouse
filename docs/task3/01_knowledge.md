# Task 3 — Knowledge: Silver on Spark, Trino on Iceberg

Plan: `03_plan.md`. Versions and budgets: `02_config.md`. Review of the input plan (what was changed and why): `03_plan.md` §R.

---

## A1. Silver: what changes from Bronze

| Bronze (Task 1–2) | Silver (Task 3) |
|---|---|
| Every column a string, as delivered | Typed: `event_ts` timestamp (no zone), `event_date` date, confidence `double`, IDs `string` |
| Partition `_ingest_date`. On K8s this **equals the event day** (landing splits by the day in `CREATED_TIME`; Bronze rule `event_day_matches_partition` enforces it, 23/23 per day). On Compose (Task 1) one partition holds all 18 days | Partition `event_date` (the day business questions use) |
| No duplicate `ID` inside a partition (Bronze rule) | One row per `ID` across the table (rule `unique id`) |
| No interpretation | Flags + business rules: `is_client` (`SOURCE='client'`), `is_home` (`TEXT='/home'`), `is_internal_sender` (the 2 email sender IDs), `final_intent` |
| Two tables | Plus a joined table: client message + its NLU result |

Rules (CLAUDE.md, `docs/task1/06_validation.md`):
- `final_intent`: `INTENT_NAME = 'fallback'` stays fallback; confidence < the row's `NLU_THRESHOLD` → `bot_khong_hieu`; else `COALESCE(UPDATED_INTENT, INTENT_NAME)`. `NLU_THRESHOLD` is **a column of the predict file** (always 0.8 in this data, `progress.md` §6), so use the row's value; do not hard-code 0.8 or pass a job parameter that could disagree with the source. `UPDATED_INTENT` is empty in this data, so only a unit test exercises the `COALESCE` branch.
- Never fill empty source columns; flag, don't delete (`/home` and testers stay in Silver, filtered in Gold/dashboard).
- Iceberg has **no cross-table transaction** (Task 1 drill B): before joining, Silver checks that both Bronze tables of the day carry the same `_batch_id`; otherwise fail (rerun Bronze).

### Timestamps: parse once, store without a zone

`CREATED_TIME` = `02-JUL-24 03.40.09.895455000 PM`: 9 fraction digits, no zone.
- Pattern `dd-MMM-yy hh.mm.ss.SSSSSSSSS a` already works: Task 2 Bronze rule `created_time_parseable` = 0 nulls on every day. Spark's formatter is case-insensitive (`JUL` parses), so no `initcap`. Spark keeps microseconds; the last 3 digits are dropped (documented Spark behaviour: nano-of-second is parsed, excess truncated).
- Store **`event_ts` as `TIMESTAMP_NTZ`** (`to_timestamp_ntz`, Spark ≥ 3.4) → Iceberg `timestamp` (without zone) → Trino `timestamp(6)`, StarRocks `DATETIME`. The wall-clock value as written is shown the same in every engine; no session-zone shift.
  If you write Spark's default `TIMESTAMP` (= with local zone), Iceberg stores `timestamptz` (UTC instant) and every reader converts it to its session zone: that is the "7 hours off" bug.
- Also store **`event_date DATE`** (= `to_date` of the NTZ value). Dates never shift. Dashboards group by `event_date`.
- The source zone (assumed `Asia/Ho_Chi_Minh`) goes into a table property / column comment, not into the values. Leader question F2.

### Writing Silver idempotently: overwrite partitions vs MERGE

| | `overwritePartitions()` (dynamic) | `MERGE INTO … ON id` |
|---|---|---|
| What it does | Replaces exactly the `event_date` partitions present in the batch | Updates matching IDs, inserts new ones |
| Rerun same day | Same table ✅ | Same table ✅ |
| Corrected re-export that **drops** a row | Row disappears ✅ (whole day replaced) | Row stays ❌ unless `WHEN NOT MATCHED BY SOURCE … DELETE` scoped to the day |
| Late event of day D inside the export of day D+1 | Would replace day D with only the late rows ❌ | Correct ✅ |
| Needs | nothing new (same as Bronze) | Iceberg SQL extensions; copy-on-write rewrites whole files |

In this pipeline one export = one event day (enforced by the Bronze rule above), so **overwrite by `event_date` is correct and simpler** (decision D7, `03_plan.md`). MERGE is the right tool when loads are incremental (CDC, late events); know it, demo it once in `explore`, do not build on it.

Dedup before writing anyway (`row_number() over (partition by ID order by _ingested_at desc) = 1`): a MERGE with duplicate source keys fails (`MERGE cardinality violation`), and an overwrite would keep both.

### Silver tables

| Table | Grain | Key columns |
|---|---|---|
| `silver.chat_message` | one chatlog row | `id`, `sender_id`, `source`, `is_client`, `text`, `is_home`, `is_internal_sender`, `input_channel`, `bot_id`, `event_ts`, `event_date`, `_batch_id` |
| `silver.nlu_prediction` | one predict row | `id`, `chat_id` (= `ID_CHATLOG`), `intent_name`, `updated_intent`, `confidence`, `nlu_threshold`, `final_intent`, `has_chat_row`, `event_ts`, `event_date` |
| `silver.user_message` | one **client** message (+ its prediction, left join) | chat columns + `final_intent`, `confidence`, `has_prediction` |

The 116 predict rows without a chat row stay in `nlu_prediction` with `has_chat_row = false` (flag, don't delete). Confirm the confidence column name with `DESCRIBE lake.bronze.bot_predict` before coding.

## A2. Trino: what it is and how it works

| Concept | Meaning |
|---|---|
| What | A distributed **SQL query engine**: stores nothing, reads data where it lives through *connectors* |
| Coordinator | Parses SQL, cost-based planning, splits work, tracks queries; UI/REST on 8080 |
| Workers | Run tasks on *splits* (pieces of files) in memory, stream pages between stages (pipelined; no disk shuffle by default) |
| Catalog | One `.properties` file per source; names are `catalog.schema.table` |
| Single node | Coordinator also schedules work on itself (`node-scheduler.include-coordinator=true`, 0 workers) |
| Failure model | A worker failure **fails the query** (client reruns). Fault-tolerant execution (`retry-policy=TASK` + exchange manager on S3) is for long ETL; not needed here |
| Memory | `query.max-memory-per-node` + `memory.heap-headroom-per-node` (default 30 % of heap) must fit inside `-Xmx`; heap ≈ 70–75 % of the pod limit |
| vs Spark | Trino: interactive SQL, seconds, many users. Spark: batch ETL, DataFrame code, per-task retries |

**How Trino reads our tables:** Trino's own Thrift client asks HMS (3.1.3 is fine) for the table's `metadata_location` → reads `metadata.json`, manifest list, manifests from MinIO → prunes files by partition and min/max column stats → reads only the needed Parquet column chunks. Same files Spark wrote, no copy.

Catalog file (native S3 file system; the legacy Hadoop `hive.s3.*` settings are removed in recent Trino):
```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore.lake.svc.cluster.local:9083
iceberg.security=read_only
fs.native-s3.enabled=true
s3.endpoint=http://minio.lake.svc.cluster.local:9000
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=${ENV:TRINO_S3_ACCESS_KEY}
s3.aws-secret-key=${ENV:TRINO_S3_SECRET_KEY}
```

## A3. Read-only, two layers

1. **MinIO user `trino`** with a read-only policy on `lakehouse/*` (pattern of `infra/k8s/lake/minio-users/`, gap H#9 from Task 2).
2. **`iceberg.security=read_only`** in the catalog: Trino refuses `INSERT/DELETE/DROP/ALTER/ANALYZE` itself, before touching S3 or HMS. Needed because HMS has **no authentication**: an S3 read-only user alone would not stop Trino from dropping a table *in the metastore* (metadata change, no S3 write).

Consequence: **`ANALYZE` is not possible from Trino** (it writes a Puffin statistics file and a new `metadata.json`). That is intended: Spark owns writes. Show `SHOW STATS FOR lake.silver.chat_message` instead (row count, null fraction, min/max come from Iceberg manifests without ANALYZE; distinct counts are what ANALYZE would add), and show the `Access Denied` of ANALYZE as the least-privilege proof.

## A4. Trino features to show

| Feature | SQL |
|---|---|
| Metadata tables | `SELECT * FROM lake.silver."chat_message$snapshots"`, `$files`, `$history`, `$partitions`, `$manifests` |
| Time travel | `… FOR VERSION AS OF <snapshot_id>`; `FOR TIMESTAMP AS OF TIMESTAMP '2026-11-07 10:00:00 Asia/Ho_Chi_Minh'` |
| Plans | `EXPLAIN` (logical/distributed), `EXPLAIN ANALYZE` (actual rows, CPU, memory, input bytes per operator) |
| Stats | `SHOW STATS FOR …` (A3) |
| Partition pruning | `EXPLAIN ANALYZE … WHERE event_date = DATE '2024-07-24'` vs no filter: fewer splits / input bytes |
| Query history | Web UI (port-forward 8080): stages, splits, memory, failed queries |
| Cross-check with Spark | `$snapshots` in Trino lists the same `snapshot_id`s as `lake.silver.chat_message.snapshots` in Spark: one table, two engines |
