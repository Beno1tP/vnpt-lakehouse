# Task 3 — Explain plans and Trino exploration (step 3.5)

Run order (times: UTC with ICT = UTC+7):
1. Pipeline mode: `make catalog CMD=info TABLE=bronze.chatlog` (Spark reports the table's current `metadata.json`).
2. `make mode-query` → `make trino-explore` (runs `sql/trino/explore.sql`, saves the output to `~/data/vnpt-lab/evidence/task3-explore-<UTC>.txt`) → the two single statements below (time travel by ID, `ANALYZE`) → `make mode-pipeline`.

Each section: the query, the result, 2–3 lines of explanation. Counts and metadata only.

## 1. Partition pruning

```sql
EXPLAIN ANALYZE SELECT final_intent, count(*) FROM lake.silver.user_message GROUP BY 1;
EXPLAIN ANALYZE SELECT final_intent, count(*) FROM lake.silver.user_message WHERE event_date = DATE '2024-07-24' GROUP BY 1;
```

| Query | Splits | Input rows | Input bytes | Wall time |
|---|---|---|---|---|
| no filter | | | | |
| `event_date = 2024-07-24` | | | | |

Explain: the filter on the partition column removes whole files at planning time (manifest partition values), before any worker reads Parquet.

## 2. Metadata tables (`$snapshots`, `$partitions`, `$files`)

| Item | Value |
|---|---|
| snapshots of silver.chat_message (first / last commit, ICT) | |
| first snapshot id | |
| partitions × files | |
| data files / rows / bytes | |

Explain: every Silver run = one Iceberg commit = one snapshot (two backfill passes + hand runs), visible without touching the data files.

## 3. One table, two engines

| Engine | Current `metadata.json` of bronze.chatlog | Snapshots |
|---|---|---|
| Spark (`make catalog CMD=info TABLE=bronze.chatlog`) | | |
| Trino (`"chatlog$metadata_log_entries"`) | | |

Explain: HMS stores one pointer per table (`metadata_location`); both engines resolve it to the same file, so they see the same snapshot.

## 4. Time travel

```sql
SELECT count(*) FROM lake.bronze.chatlog FOR VERSION AS OF 'task2_v1';            -- in explore.sql: expect 21224
SELECT count(*) FROM lake.silver.chat_message FOR VERSION AS OF <first_snapshot_id>;  -- run by hand with the id from §2
```

| Version | Rows | Meaning |
|---|---|---|
| tag `task2_v1` (bronze.chatlog) | | |
| first Silver snapshot | | |
| current Silver | 21,224 | |

## 5. Statistics and read-only

```sql
SHOW STATS FOR lake.silver.chat_message;   -- in explore.sql
ANALYZE lake.silver.chat_message;          -- by hand: expected Access Denied (iceberg.security=read_only, D11)
```

Explain: row count, null fraction and min/max come from Iceberg manifests without ANALYZE; distinct-value counts are what ANALYZE would add (it writes a Puffin file + new metadata, so it needs write access, which Trino deliberately lacks).

## 6. MERGE (knowledge only, not run)

D7 builds Silver with `overwritePartitions()` because one export = one event day. MERGE (`MERGE INTO t USING s ON t.id = s.id WHEN MATCHED … UPDATE … WHEN NOT MATCHED … INSERT`) is the tool for incremental loads (CDC, late events); it needs a deduplicated source (else `MERGE cardinality violation`) and, with copy-on-write, rewrites whole files per matched partition. A demo would need a Spark write job on a scratch table (new job code + image); optional, not part of the pipeline.
