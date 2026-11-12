-- Task 4 freeze / demo-check: every Gold table's row count AND its measure sums, against the expected values
-- (docs/metrics.md, tools/expected_gold.py, tests/expected/silver.json). One row per check: table, measure,
-- expected, actual. Counts and sums only. Run by `make gold-check` (StarRocks up, gold or demo-task4 mode).
SELECT 'dim_date', 'rows', 24, count(*) FROM gold.dim_date
UNION ALL SELECT 'dim_intent', 'rows', 3, count(*) FROM gold.dim_intent
UNION ALL SELECT 'dim_sender', 'rows', 89, count(*) FROM gold.dim_sender
UNION ALL SELECT 'dim_sender', 'max(sender_key)', 89, max(sender_key) FROM gold.dim_sender
UNION ALL SELECT 'fct_prediction', 'rows', 10729, count(*) FROM gold.fct_prediction
UNION ALL SELECT 'fct_prediction', 'real', 219, count(*) FROM gold.fct_prediction WHERE traffic_class = 'real'
UNION ALL SELECT 'fct_user_message', 'rows', 10613, count(*) FROM gold.fct_user_message
UNION ALL SELECT 'gold_uc4', 'rows', 36, count(*) FROM gold.gold_uc4
UNION ALL SELECT 'gold_uc4', 'sum(message_cnt)', 10729, sum(message_cnt) FROM gold.gold_uc4
UNION ALL SELECT 'gold_uc4', 'sum(message_cnt) step 406666', 235, sum(message_cnt) FROM gold.gold_uc4 WHERE step = '406666'
UNION ALL SELECT 'gold_uc5', 'rows', 36, count(*) FROM gold.gold_uc5
UNION ALL SELECT 'gold_uc5', 'sum(understood_cnt)', 10588, sum(understood_cnt) FROM gold.gold_uc5
UNION ALL SELECT 'gold_uc5', 'sum(not_understood_cnt)', 141, sum(not_understood_cnt) FROM gold.gold_uc5
UNION ALL SELECT 'gold_uc6', 'rows', 57, count(*) FROM gold.gold_uc6
UNION ALL SELECT 'gold_uc6', 'sum(message_cnt)', 10729, sum(message_cnt) FROM gold.gold_uc6
UNION ALL SELECT 'gold_uc6', 'sum(message_cnt) business', 10588, sum(message_cnt) FROM gold.gold_uc6 WHERE intent_group = 'business'
UNION ALL SELECT 'gold_uc7', 'rows', 48, count(*) FROM gold.gold_uc7
UNION ALL SELECT 'gold_uc7', 'cumulative_users on 2024-07-25', 89, sum(cumulative_users) FROM gold.gold_uc7 WHERE event_date = '2024-07-25'
UNION ALL SELECT 'gold_uc8', 'rows', 48, count(*) FROM gold.gold_uc8
UNION ALL SELECT 'gold_uc8', 'sum(new_users)', 89, sum(new_users) FROM gold.gold_uc8
UNION ALL SELECT 'gold_uc8', 'sum(active_users)', 112, sum(active_users) FROM gold.gold_uc8
UNION ALL SELECT 'gold_uc8', 'bitmap_union_count(users)', 89, bitmap_union_count(users) FROM gold.gold_uc8
