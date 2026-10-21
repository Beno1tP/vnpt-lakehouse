-- Bronze checks for one ingest date. Each block: "-- check: <table>.<key>" + one query returning ONE number.
-- <table>.<key> must exist in tests/expected/bronze.json. ${ingest_date} is replaced by the validator.
-- Counts only: never select row contents here.

-- check: chatlog.rows
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}';

-- check: chatlog.distinct_id
SELECT count(DISTINCT ID) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}';

-- check: chatlog.distinct_sender
SELECT count(DISTINCT SENDER_ID) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}';

-- check: chatlog.non_null_in_empty_cols
SELECT count(ADD_INFO) + count(SESSION_ID) + count(LOGSTT_ID) + count(AUDIO_URL) + count(PAGE_ID)
FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}';

-- check: chatlog.source_client
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}' AND SOURCE = 'client';

-- check: chatlog.source_bot
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}' AND SOURCE = 'bot';

-- check: chatlog.client_home
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}' AND SOURCE = 'client' AND TEXT = '/home';

-- check: chatlog.channel_livechat
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}' AND INPUT_CHANNEL = 'livechat';

-- check: chatlog.channel_platform
SELECT count(*) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}' AND INPUT_CHANNEL = 'platform';

-- check: bot_predict.rows
SELECT count(*) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}';

-- check: bot_predict.distinct_id
SELECT count(DISTINCT ID) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}';

-- check: bot_predict.distinct_sender
SELECT count(DISTINCT SENDER_ID) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}';

-- check: bot_predict.non_null_in_empty_cols
SELECT count(UPDATED_INTENT) + count(STATUS_DELETE) + count(STATUS_CONFIRM) + count(INTENT_MAP_CLICK_BUTTON)
FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}';

-- check: bot_predict.unmatched_to_chat
SELECT count(*)
FROM (SELECT ID_CHATLOG FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}') p
LEFT ANTI JOIN (SELECT ID FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}') c
ON p.ID_CHATLOG = c.ID;

-- check: both.distinct_batch_ids
-- Both tables must come from the same load (a crash between the two commits leaves them on different batches: drill B)
SELECT count(DISTINCT _batch_id) FROM (
  SELECT _batch_id FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}'
  UNION ALL
  SELECT _batch_id FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}'
);
