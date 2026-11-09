# Task 3 — Explain plans and Trino exploration (step 3.5)

Run in query mode (`make mode-query`), from `sql/trino/explore.sql`. Paste the output under each heading, then explain it in 2–3 lines (what the number means, why).

## 1. Partition pruning

```sql
EXPLAIN ANALYZE SELECT final_intent, count(*) FROM lake.silver.user_message GROUP BY 1;
EXPLAIN ANALYZE SELECT final_intent, count(*) FROM lake.silver.user_message
WHERE event_date = DATE '2024-07-24' GROUP BY 1;
```

| Query | Splits | Input rows | Input bytes | Wall time |
|---|---|---|---|---|
| no filter | | | | |
| `event_date = 2024-07-24` | | | | |

Explain: the filter on the partition column removes whole files at planning time (manifest partition values), before any worker reads Parquet.

## 2. Metadata tables and the same snapshots as Spark

```sql
SELECT snapshot_id, committed_at, operation, summary['added-records'] FROM lake.silver."chat_message$snapshots" ORDER BY committed_at;
SELECT partition, record_count, file_count FROM lake.silver."chat_message$partitions";
SELECT file_path, record_count, file_size_in_bytes FROM lake.silver."chat_message$files";
```
Compare with Spark: `SELECT snapshot_id, operation FROM lake.silver.chat_message.snapshots`.

## 3. Time travel

```sql
SELECT count(*) FROM lake.silver.chat_message FOR VERSION AS OF <first snapshot_id>;
```

## 4. Statistics and read-only

```sql
SHOW STATS FOR lake.silver.chat_message;
ANALYZE lake.silver.chat_message;   -- expected: Access Denied (iceberg.security=read_only, D11)
```

## 5. MERGE demo (Spark, scratch table only — knowledge, not the pipeline: D7)

```sql
CREATE TABLE lake.scratch.chat_message_merge AS SELECT * FROM lake.silver.chat_message WHERE event_date = DATE '2024-07-02';
MERGE INTO lake.scratch.chat_message_merge t USING (...) s ON t.id = s.id
WHEN MATCHED AND s._ingested_at > t._ingested_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
DROP TABLE lake.scratch.chat_message_merge PURGE;
```
