-- Task 3 step 3.4: every Silver number per event_date, computed by TRINO (independent of Spark's validator), in the
-- shape of tests/expected/silver_daily.json. Output: one row per (day, key, value); tools/trino_check.py compares it
-- with the pandas expectations. Counts only, never row contents. Run: make trino-check (query mode).
WITH c AS (
  SELECT event_date,
         count(*) AS chat_rows,
         count_if(is_client) AS client_rows,
         count_if(is_client AND is_home) AS client_home_rows,
         count_if(is_internal_sender) AS internal_sender_rows,
         count_if(is_internal_sender AND is_client) AS internal_sender_client_rows
  FROM lake.silver.chat_message GROUP BY event_date
), p AS (
  SELECT event_date, count(*) AS predict_rows, count_if(NOT has_chat_row) AS predict_without_chat_row
  FROM lake.silver.nlu_prediction GROUP BY event_date
), u AS (
  SELECT event_date, count(*) AS user_message_rows, count_if(has_prediction) AS user_message_with_prediction
  FROM lake.silver.user_message GROUP BY event_date
)
SELECT CAST(event_date AS varchar) AS day, k, v FROM c
CROSS JOIN UNNEST(
  ARRAY['chat_rows', 'client_rows', 'client_home_rows', 'internal_sender_rows', 'internal_sender_client_rows'],
  ARRAY[chat_rows, client_rows, client_home_rows, internal_sender_rows, internal_sender_client_rows]) AS t(k, v)
UNION ALL
SELECT CAST(event_date AS varchar), k, v FROM p
CROSS JOIN UNNEST(ARRAY['predict_rows', 'predict_without_chat_row'],
                  ARRAY[predict_rows, predict_without_chat_row]) AS t(k, v)
UNION ALL
SELECT CAST(event_date AS varchar), k, v FROM u
CROSS JOIN UNNEST(ARRAY['user_message_rows', 'user_message_with_prediction'],
                  ARRAY[user_message_rows, user_message_with_prediction]) AS t(k, v)
UNION ALL
SELECT CAST(event_date AS varchar), 'final_intent.' || final_intent, count(*)
FROM lake.silver.nlu_prediction GROUP BY event_date, final_intent
ORDER BY 1, 2
