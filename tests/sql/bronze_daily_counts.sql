-- Task 2 step 2.0 (decision D3): expected counts per event day, from the validated Task 1 Bronze tables.
-- Day = date as written in CREATED_TIME (Oracle text, no time zone conversion).
-- Run (Task 1 stack up):
--   docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env exec -T spark \
--     /opt/spark/bin/spark-sql --master local[2] -f /opt/tests/sql/bronze_daily_counts.sql 2>/dev/null
-- Counts only, never row contents. Partition 2026-10-16 = the Task 1 load validated by make acceptance.

CREATE OR REPLACE TEMPORARY VIEW c AS
SELECT ID, to_date(to_timestamp(CREATED_TIME, 'dd-MMM-yy hh.mm.ss.SSSSSSSSS a')) AS day
FROM lake.bronze.chatlog WHERE _ingest_date = DATE '2026-10-16';

CREATE OR REPLACE TEMPORARY VIEW p AS
SELECT ID_CHATLOG, to_date(to_timestamp(CREATED_TIME, 'dd-MMM-yy hh.mm.ss.SSSSSSSSS a')) AS day
FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '2026-10-16';

-- one row per day: chatlog rows, predict rows,
-- unmatched_any_day  = predict rows with no chat row at all (sums to 116, as in Task 1),
-- unmatched_same_day = predict rows with no chat row in the SAME day (what a per-day validator will see)
CREATE OR REPLACE TEMPORARY VIEW daily AS
SELECT coalesce(cd.day, pd.day)          AS day,
       coalesce(cd.n, 0)                 AS chatlog_rows,
       coalesce(pd.n, 0)                 AS predict_rows,
       coalesce(pd.unmatched_any, 0)     AS unmatched_any_day,
       coalesce(pd.unmatched_same, 0)    AS unmatched_same_day
FROM (SELECT day, count(*) AS n FROM c GROUP BY day) cd
FULL OUTER JOIN (
  SELECT p.day,
         count(*)                                         AS n,
         count_if(ca.ID IS NULL)                          AS unmatched_any,
         count_if(cs.ID IS NULL)                          AS unmatched_same
  FROM p
  LEFT JOIN c ca ON p.ID_CHATLOG = ca.ID
  LEFT JOIN c cs ON p.ID_CHATLOG = cs.ID AND p.day = cs.day
  GROUP BY p.day) pd
ON cd.day = pd.day;

SELECT * FROM daily ORDER BY day;

-- totals: must be 21224 / 10729 / 116, with 0 rows on a NULL day (= 0 parse failures)
SELECT 'TOTAL' AS day, sum(chatlog_rows), sum(predict_rows), sum(unmatched_any_day), sum(unmatched_same_day),
       count(*) AS days_with_rows, count_if(day IS NULL) AS null_days
FROM daily;
