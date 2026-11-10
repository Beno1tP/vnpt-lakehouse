# Task 3 — Explain plans and Trino exploration (step 3.5)

Run 2026-11-10 (times: UTC with ICT = UTC+7):
1. Pipeline mode: `make catalog CMD=info TABLE=bronze.chatlog` (Spark).
2. `make mode-query` → `make trino-explore` (01:44 UTC = 08:44 ICT; output `~/data/vnpt-lab/evidence/task3-explore-20261110T0144Z.txt`, 129 lines, outside Git) → time travel by id + `ANALYZE` by hand → `make mode-pipeline`.

Counts and metadata only.

## 1. Partition pruning

```sql
EXPLAIN ANALYZE SELECT final_intent, count(*) FROM lake.silver.user_message GROUP BY 1;
EXPLAIN ANALYZE SELECT final_intent, count(*) FROM lake.silver.user_message WHERE event_date = DATE '2024-07-24' GROUP BY 1;
```

| Query | Splits (files read) | Input rows | Input (logical / physical) |
|---|---|---|---|
| no filter | **18** | 10,613 | 176.70 kB / 571.84 kB |
| `event_date = 2024-07-24` | **1** | 6,485 | 107.66 kB / 296.72 kB |

Explain: `event_date` is the partition column, and each Iceberg manifest records the partition value of every data file. Trino drops the 17 files of other days **at planning time**, before opening any Parquet; only the 07-24 file is read. Without the filter every day's file is a split. On big tables this is the main reason to filter on the partition column (Gold/dashboard queries by date).

## 2. Metadata tables (`$snapshots`, `$partitions`, `$files`) of silver.chat_message

| Item | Value |
|---|---|
| snapshots | **38** = 2 hand runs (07-09, 07-25 on 11-09) + 18 days × 2 backfill passes |
| first commit | 2026-11-09 **17:11:46 ICT** (10:11:46 UTC), snapshot `7804354106737549217`, `append` = the first `make sparkapp APP=silver DATE=2024-07-09` |
| last commit | 2026-11-10 **00:12:32 ICT** (17:12:32 UTC) = end of backfill pass 2 (07-25) |
| partitions | 18 days, **1 file each** |
| data files / rows / bytes | 18 / 21,224 / 644,236 (~629 KiB) |

Explain: every Silver run of a day = one Iceberg commit = one snapshot; reruns add snapshots (`overwrite`), never rows. One compact file per day: no small-file problem yet (weekly maintenance would compact if it appeared).

## 3. One table, two engines (bronze.chatlog)

| Engine | Current `metadata.json` | Snapshots |
|---|---|---|
| Spark (`make catalog CMD=info`) | `…/chatlog/metadata/00079-8085977d-f329-473d-865a-a5f5b1a46e9a.metadata.json` | 60 |
| Trino (`"chatlog$metadata_log_entries"`, `$snapshots`) | **the same file** | **60** |

Explain: HMS stores one pointer per table (`metadata_location`); Spark and Trino resolve it to the same `metadata.json`, hence the same snapshot and the same rows. Spark wrote, HMS points, Trino reads: one copy of the data.

Time-zone note: Spark reported `last_commit=2026-11-09 17:11:44` = **UTC** (00:11:44 ICT, pass 2's last Bronze commit). The Spark SQL session zone is Asia/Ho_Chi_Minh, but `collect()` hands timestamps to Python, which formats them in the **pod's** zone (UTC). Trino printed its times in ICT because the script sets `SET TIME ZONE 'Asia/Ho_Chi_Minh'`. Same instants, different display zones: always label the zone.

## 4. Time travel

| Version | Rows | Meaning |
|---|---|---|
| bronze.chatlog `FOR VERSION AS OF 'task2_v1'` | **21,224** | the Task 2 freeze tag still reads the frozen state |
| refs of bronze.chatlog | `main` (BRANCH) → `5849815788944112570`; `task2_v1` (TAG) → `4460393597214113888` | `main` moved on (two backfills since the freeze), the tag stayed |
| silver.chat_message `FOR VERSION AS OF 7804354106737549217` (first snapshot) | **86**, only 2024-07-09 | the table as it was after the very first Silver run |
| silver.chat_message now | 21,224, 18 days | |

Explain: a snapshot keeps the list of files that formed the table at that commit; time travel reads that list. Old snapshots stay readable until `expire_snapshots` (weekly maintenance, 7 days / keep ≥ 5) removes them, except snapshots held by a tag.

## 5. Statistics and read-only

| `SHOW STATS FOR lake.silver.chat_message` | Value |
|---|---|
| row count | 21,224 |
| distinct values | **empty** for every column (that is what ANALYZE would add) |
| null fraction | `text` 0.00075 (≈ 16 rows), `session_id` **1.0** (empty in every chat row: a data fact), others 0 |
| `event_ts` min / max | 2024-07-02 15:39:17.676687 / 2024-07-25 11:01:53.468563: the wall clock as written, not shifted (D8) |
| booleans | min false, max true |

`ANALYZE lake.silver.chat_message` → **`Access Denied: Cannot ANALYZE (missing insert privilege)`** (query `20261110_014705_00012_g8vaz`).

Explain: row count, null fraction and min/max come from Iceberg manifests at no cost. ANALYZE would write a Puffin statistics file + a new `metadata.json`, so it needs write access, which Trino deliberately lacks (`iceberg.security=read_only`, D11). Spark owns writes.

## 6. MERGE (knowledge only, not run)

D7 builds Silver with `overwritePartitions()` because one export = one event day. MERGE (`MERGE INTO t USING s ON t.id = s.id WHEN MATCHED … UPDATE … WHEN NOT MATCHED … INSERT`) is the tool for incremental loads (CDC, late events); it needs a deduplicated source (else `MERGE cardinality violation`) and, with copy-on-write, rewrites whole files per matched partition. A demo would need a Spark write job on a scratch table (new job code + image); optional, not part of the pipeline.
