-- Task 3 step 3.5: how Trino reads the Iceberg tables. Read-only (D11). Run: make trino-explore (query mode).
-- Results and their explanation: docs/task3/07_explain_plans.md. Counts and metadata only, never row contents.
SET TIME ZONE 'Asia/Ho_Chi_Minh';

-- 1. Partition pruning: same query without / with a filter on the partition column event_date
EXPLAIN ANALYZE SELECT final_intent, count(*) FROM lake.silver.user_message GROUP BY 1;
EXPLAIN ANALYZE SELECT final_intent, count(*) FROM lake.silver.user_message
WHERE event_date = DATE '2024-07-24' GROUP BY 1;

-- 2. Metadata tables: snapshots, partitions, files of silver.chat_message
SELECT count(*) AS snapshots, min(committed_at) AS first_commit, max(committed_at) AS last_commit
FROM lake.silver."chat_message$snapshots";
SELECT snapshot_id AS first_snapshot_id, committed_at, operation
FROM lake.silver."chat_message$snapshots" ORDER BY committed_at LIMIT 1;
SELECT partition.event_date, record_count, file_count FROM lake.silver."chat_message$partitions" ORDER BY 1;
SELECT count(*) AS data_files, sum(record_count) AS rows, sum(file_size_in_bytes) AS bytes
FROM lake.silver."chat_message$files";

-- 3. One table, two engines: Trino's current metadata.json of bronze.chatlog = the file Spark reports
--    (make catalog CMD=info TABLE=bronze.chatlog prints metadata=…)
SELECT file AS current_metadata_file FROM lake.bronze."chatlog$metadata_log_entries" ORDER BY timestamp DESC LIMIT 1;
SELECT count(*) AS snapshots FROM lake.bronze."chatlog$snapshots";

-- 4. Time travel through the Task 2 freeze tag (expect 21224) and the refs that hold it
SELECT count(*) AS rows_at_tag_task2_v1 FROM lake.bronze.chatlog FOR VERSION AS OF 'task2_v1';
SELECT name, type, snapshot_id FROM lake.bronze."chatlog$refs" ORDER BY name;

-- 5. Statistics Iceberg keeps without ANALYZE (rows, nulls, min/max from manifests)
SHOW STATS FOR lake.silver.chat_message;
