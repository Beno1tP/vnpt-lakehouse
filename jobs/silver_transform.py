"""Task 3 step 3.1 — Bronze -> Silver for ONE event day (idempotent: rerun replaces that day).

    spark-submit silver_transform.py --ingest-date 2024-07-24

Reads both Bronze partitions of the day, checks they come from one Bronze run (same _batch_id: Iceberg has no
cross-table transaction), then writes three Silver tables partitioned by event_date with overwritePartitions() (D7):
  silver.chat_message    one chatlog row: typed, deduplicated, flags is_client / is_home / is_internal_sender
  silver.nlu_prediction  one predict row: confidence + threshold as double, final_intent, has_chat_row, step
  silver.user_message    one client message + its prediction (left join), has_prediction
event_ts = TIMESTAMP_NTZ (wall clock as written, Vietnam local; D8); event_date = its date. final_intent uses the
row's NLU_THRESHOLD (D9). `step` = the bot scenario (predict STEP, Task 4 step 4.0a: UC4), added to existing
tables by schema evolution (ADD COLUMN, metadata only) before the first write that carries it.
Rules = tools/expected_silver.py (pandas), whose numbers the validator checks.
Prints counts only, never row contents.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date

CREATED_FMT = "dd-MMM-yy hh.mm.ss.SSSSSSSSS a"  # Oracle text, 9 fraction digits (Spark keeps microseconds)
SOURCE_TZ = "Asia/Ho_Chi_Minh"  # documentation only (table property): values are stored without a zone
TABLES = ("chat_message", "nlu_prediction", "user_message")
ADDED_COLUMNS = {  # schema evolution: columns added after the table was created (Task 4 4.0a)
    "nlu_prediction": {"step": "string"},
    "user_message": {"step": "string"},
}


def check_day(day: str) -> str:
    """Only a real calendar date reaches SQL (the value is interpolated into queries)."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        raise ValueError(f"--ingest-date must be YYYY-MM-DD, got {day!r}")
    date.fromisoformat(day)  # rejects 2024-02-30
    return day


def event_ts_expr(col: str = "CREATED_TIME") -> str:
    return f"to_timestamp_ntz({col}, '{CREATED_FMT}')"


def batch_check_sql(day: str) -> str:
    """Number of distinct Bronze runs behind day D across both tables (must be exactly 1)."""
    d = check_day(day)
    return (
        "SELECT count(DISTINCT _batch_id) FROM ("
        f"SELECT _batch_id FROM lake.bronze.chatlog WHERE _ingest_date = DATE '{d}' "
        f"UNION ALL SELECT _batch_id FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '{d}')"
    )


def chat_message_sql(day: str) -> str:
    d = check_day(day)
    ts = event_ts_expr()
    return f"""
SELECT ID AS id, BOT_ID AS bot_id, SENDER_ID AS sender_id, SOURCE AS source,
       SOURCE = 'client' AS is_client,
       TEXT AS text, coalesce(TEXT = '/home', false) AS is_home,
       coalesce(SENDER_ID LIKE '%@%', false) AS is_internal_sender,
       INPUT_CHANNEL AS input_channel, SESSION_ID AS session_id,
       {ts} AS event_ts, to_date({ts}) AS event_date,
       _batch_id, _ingest_date
FROM (SELECT *, row_number() OVER (PARTITION BY ID ORDER BY _ingested_at DESC) AS _rn
      FROM lake.bronze.chatlog WHERE _ingest_date = DATE '{d}')
WHERE _rn = 1"""


def nlu_prediction_sql(day: str) -> str:
    d = check_day(day)
    ts = event_ts_expr()
    return f"""
SELECT p.ID AS id, p.ID_CHATLOG AS chat_id, p.BOT_ID AS bot_id, p.SENDER_ID AS sender_id,
       p.INTENT_NAME AS intent_name, nullif(p.UPDATED_INTENT, '') AS updated_intent,
       CAST(p.INTENT_CONFIDENCE AS DOUBLE) AS confidence, CAST(p.NLU_THRESHOLD AS DOUBLE) AS nlu_threshold,
       CASE WHEN p.INTENT_NAME = 'fallback' THEN 'fallback'
            WHEN CAST(p.INTENT_CONFIDENCE AS DOUBLE) < CAST(p.NLU_THRESHOLD AS DOUBLE) THEN 'bot_khong_hieu'
            ELSE coalesce(nullif(p.UPDATED_INTENT, ''), p.INTENT_NAME) END AS final_intent,
       c.chat_ref IS NOT NULL AS has_chat_row, p.STEP AS step,
       {ts.replace("CREATED_TIME", "p.CREATED_TIME")} AS event_ts,
       to_date({ts.replace("CREATED_TIME", "p.CREATED_TIME")}) AS event_date,
       p._batch_id, p._ingest_date
FROM (SELECT *, row_number() OVER (PARTITION BY ID ORDER BY _ingested_at DESC) AS _rn
      FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '{d}') p
LEFT JOIN (SELECT DISTINCT ID AS chat_ref FROM lake.bronze.chatlog) c ON c.chat_ref = p.ID_CHATLOG
WHERE p._rn = 1"""


USER_MESSAGE_SQL = """
SELECT c.id, c.sender_id, c.text, c.is_home, c.is_internal_sender, c.input_channel, c.session_id,
       c.event_ts, c.event_date, p.id AS prediction_id, p.final_intent, p.confidence, p.step,
       p.id IS NOT NULL AS has_prediction, c._batch_id
FROM chat_message c LEFT JOIN nlu_prediction p ON p.chat_id = c.id
WHERE c.is_client"""


SUMMARY_SQL = """
SELECT
  (SELECT count(*) FROM chat_message) AS chat_rows,
  (SELECT count_if(is_client) FROM chat_message) AS client_rows,
  (SELECT count_if(is_client AND is_home) FROM chat_message) AS client_home_rows,
  (SELECT count_if(is_internal_sender) FROM chat_message) AS internal_sender_rows,
  (SELECT count_if(is_internal_sender AND is_client) FROM chat_message) AS internal_sender_client_rows,
  (SELECT count(*) FROM nlu_prediction) AS predict_rows,
  (SELECT count_if(NOT has_chat_row) FROM nlu_prediction) AS predict_without_chat_row,
  (SELECT count(*) FROM user_message) AS user_message_rows,
  (SELECT count_if(has_prediction) FROM user_message) AS user_message_with_prediction"""


def summary(spark) -> dict:
    """Counts in the shape of tests/expected/silver_daily.json (tools/expected_silver.py counts())."""
    out = spark.sql(SUMMARY_SQL).collect()[0].asDict()
    rows = spark.sql("SELECT final_intent, count(*) FROM nlu_prediction GROUP BY final_intent").collect()
    out["final_intent"] = {r[0]: r[1] for r in sorted(rows)}
    rows = spark.sql("SELECT step, count(*) FROM nlu_prediction GROUP BY step").collect()
    out["step"] = {r[0]: r[1] for r in sorted(rows)}
    return out


def add_columns_sql(table: str, existing: list[str]) -> list[str]:
    """ALTER statements for the ADDED_COLUMNS the table does not have yet (idempotent: [] once added)."""
    if table not in TABLES:
        raise ValueError(f"unknown Silver table {table!r}")
    have = {c.lower() for c in existing}
    return [
        f"ALTER TABLE lake.silver.{table} ADD COLUMN {col} {typ}"
        for col, typ in ADDED_COLUMNS.get(table, {}).items()
        if col.lower() not in have
    ]


def write(spark, df, table: str) -> None:
    full = f"lake.silver.{table}"
    if spark.catalog.tableExists(full):
        for stmt in add_columns_sql(table, spark.table(full).columns):
            spark.sql(stmt)  # Iceberg schema evolution: new column id, old data files read it as NULL
            print(f"[silver] schema evolution: {stmt}", flush=True)
        # replaces exactly the event_date partitions in df (D7); DataFrameWriterV2 matches columns by name
        df.writeTo(full).overwritePartitions()
    else:
        from pyspark.sql import functions as F

        (
            df.writeTo(full)
            .using("iceberg")
            .partitionedBy(F.col("event_date"))
            .tableProperty("format-version", "2")
            .tableProperty("source.timezone", SOURCE_TZ)  # event_ts is this zone's wall clock, stored without zone
            .create()
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest-date", required=True, help="YYYY-MM-DD: the Bronze day = the Silver event_date")
    day = check_day(ap.parse_args().ingest_date)

    from pyspark.sql import SparkSession  # imported here so the SQL builders are testable without Spark

    spark = SparkSession.builder.appName("silver_transform").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    batches = spark.sql(batch_check_sql(day)).collect()[0][0]
    if batches != 1:  # 0 = no Bronze rows for the day; >1 = tables from different Bronze runs (rerun bronze)
        sys.exit(f"[silver] {day}: Bronze has {batches} distinct _batch_id across the two tables, expected 1")

    chat = spark.sql(chat_message_sql(day)).cache()
    pred = spark.sql(nlu_prediction_sql(day)).cache()
    for name, df in (("chat_message", chat), ("nlu_prediction", pred)):
        if df.filter(f"event_ts IS NULL OR event_date <> DATE '{day}'").count():
            # overwritePartitions() would replace ANOTHER day's partition with these rows (D7 edge)
            sys.exit(f"[silver] {day}: {name} has rows with a null event_ts or another event_date")
    if pred.groupBy("chat_id").count().filter("count > 1 AND chat_id IS NOT NULL").count():
        sys.exit(f"[silver] {day}: a chat id has more than one prediction: user_message would multiply rows")
    chat.createOrReplaceTempView("chat_message")
    pred.createOrReplaceTempView("nlu_prediction")
    user = spark.sql(USER_MESSAGE_SQL)

    user.createOrReplaceTempView("user_message")
    import json

    print(f"[silver] {day} summary {json.dumps(summary(spark), sort_keys=True)}", flush=True)  # = silver_daily.json
    for table, df in (("chat_message", chat), ("nlu_prediction", pred), ("user_message", user)):
        write(spark, df, table)
        n = spark.table(f"lake.silver.{table}").where(f"event_date = DATE '{day}'").count()
        print(f"[silver] {table}: {n} rows in partition {day}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
