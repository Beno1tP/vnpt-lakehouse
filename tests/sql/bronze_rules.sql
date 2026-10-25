-- Task 2 step 2.4 (gap #6): data-quality RULES that must hold for ANY day, unlike the exact reconciliation counts in
-- bronze_checks.sql + bronze_daily.json. Each block: "-- rule: <table>.<name>" + one query returning the NUMBER OF
-- VIOLATIONS for the partition ${ingest_date}; every rule expects 0. Counts only, never row contents.
-- Event day = date as written in CREATED_TIME (Oracle text, e.g. '02-JUL-24 03.40.09.895455000 PM').

-- rule: chatlog.partition_not_empty
SELECT CASE WHEN count(*) > 0 THEN 0 ELSE 1 END FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}';

-- rule: chatlog.id_not_null
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}' AND ID IS NULL;

-- rule: chatlog.id_unique
SELECT count(*) - count(DISTINCT ID) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}';

-- rule: chatlog.source_accepted_values
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}'
  AND (SOURCE IS NULL OR SOURCE NOT IN ('client', 'bot'));

-- rule: chatlog.created_time_parseable
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}'
  AND to_timestamp(CREATED_TIME, 'dd-MMM-yy hh.mm.ss.SSSSSSSSS a') IS NULL;

-- rule: chatlog.event_day_matches_partition
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}'
  AND to_date(to_timestamp(CREATED_TIME, 'dd-MMM-yy hh.mm.ss.SSSSSSSSS a')) <> _ingest_date;

-- rule: bot_predict.partition_not_empty
SELECT CASE WHEN count(*) > 0 THEN 0 ELSE 1 END FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}';

-- rule: bot_predict.id_not_null
SELECT count(*) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}' AND ID IS NULL;

-- rule: bot_predict.id_unique
SELECT count(*) - count(DISTINCT ID) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}';

-- rule: bot_predict.id_chatlog_not_null
SELECT count(*) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}' AND ID_CHATLOG IS NULL;

-- rule: bot_predict.created_time_parseable
SELECT count(*) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}'
  AND to_timestamp(CREATED_TIME, 'dd-MMM-yy hh.mm.ss.SSSSSSSSS a') IS NULL;

-- rule: bot_predict.event_day_matches_partition
SELECT count(*) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}'
  AND to_date(to_timestamp(CREATED_TIME, 'dd-MMM-yy hh.mm.ss.SSSSSSSSS a')) <> _ingest_date;

-- rule: both.one_batch
SELECT count(DISTINCT _batch_id) - 1 FROM (
  SELECT _batch_id FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}'
  UNION ALL
  SELECT _batch_id FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}');
