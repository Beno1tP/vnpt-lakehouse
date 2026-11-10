-- Task 3 step 3.2: data-quality RULES for Silver that must hold for ANY day (exact counts are in silver_daily.json).
-- Each block: "-- rule: <table>.<name>" + one query returning the NUMBER OF VIOLATIONS for event_date ${ingest_date};
-- every rule expects 0. Counts only, never row contents. Silver day D = Bronze partition D (landing split by the
-- date as written in CREATED_TIME).

-- rule: chat_message.partition_not_empty
SELECT CASE WHEN count(*) > 0 THEN 0 ELSE 1 END FROM lake.silver.chat_message WHERE event_date = DATE '${ingest_date}';

-- rule: chat_message.event_ts_not_null
SELECT count(*) FROM lake.silver.chat_message WHERE event_date = DATE '${ingest_date}' AND event_ts IS NULL;

-- rule: chat_message.event_date_is_date_of_event_ts
SELECT count(*) FROM lake.silver.chat_message WHERE event_date = DATE '${ingest_date}' AND to_date(event_ts) <> event_date;

-- rule: chat_message.id_unique_across_table
SELECT count(*) - count(DISTINCT id) FROM lake.silver.chat_message;

-- rule: chat_message.rows_equal_bronze
SELECT abs((SELECT count(*) FROM lake.silver.chat_message WHERE event_date = DATE '${ingest_date}')
         - (SELECT count(DISTINCT ID) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '${ingest_date}'));

-- rule: chat_message.flags_not_null
SELECT count(*) FROM lake.silver.chat_message WHERE event_date = DATE '${ingest_date}'
  AND (is_client IS NULL OR is_home IS NULL OR is_internal_sender IS NULL);

-- rule: nlu_prediction.id_unique_across_table
SELECT count(*) - count(DISTINCT id) FROM lake.silver.nlu_prediction;

-- rule: nlu_prediction.confidence_and_threshold_not_null
SELECT count(*) FROM lake.silver.nlu_prediction WHERE event_date = DATE '${ingest_date}'
  AND (confidence IS NULL OR nlu_threshold IS NULL);

-- rule: nlu_prediction.final_intent_follows_rule
SELECT count(*) FROM lake.silver.nlu_prediction WHERE event_date = DATE '${ingest_date}'
  AND final_intent <> CASE WHEN intent_name = 'fallback' THEN 'fallback'
                           WHEN confidence < nlu_threshold THEN 'bot_khong_hieu'
                           ELSE coalesce(updated_intent, intent_name) END;

-- rule: nlu_prediction.step_not_null
SELECT count(*) FROM lake.silver.nlu_prediction WHERE event_date = DATE '${ingest_date}' AND step IS NULL;

-- rule: user_message.step_iff_prediction
SELECT count(*) FROM lake.silver.user_message WHERE event_date = DATE '${ingest_date}'
  AND (step IS NULL) = has_prediction;

-- rule: nlu_prediction.rows_equal_bronze
SELECT abs((SELECT count(*) FROM lake.silver.nlu_prediction WHERE event_date = DATE '${ingest_date}')
         - (SELECT count(DISTINCT ID) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '${ingest_date}'));

-- rule: user_message.rows_equal_client_messages
SELECT abs((SELECT count(*) FROM lake.silver.user_message WHERE event_date = DATE '${ingest_date}')
         - (SELECT count(*) FROM lake.silver.chat_message WHERE event_date = DATE '${ingest_date}' AND is_client));

-- rule: user_message.one_row_per_message
SELECT count(*) - count(DISTINCT id) FROM lake.silver.user_message WHERE event_date = DATE '${ingest_date}';
