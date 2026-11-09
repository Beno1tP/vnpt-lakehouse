"""Task 3 step 3.2 — validate the Silver tables for one event day.

Exact counts vs tests/expected/silver_daily.json (pandas, tools/expected_silver.py; same keys as the summary that
silver_transform.py prints), day-generic rules from tests/sql/silver_rules.sql (violations must be 0), and the
column types that matter (event_ts must be timestamp_ntz: a zoned timestamp shifts by 7 h in readers, D8).
PASS/FAIL per check, counts only; exit 1 on any failure so the Airflow task fails.

    spark-submit validate_silver.py --ingest-date 2024-07-24 \
        --expected /opt/tests/expected/silver_daily.json --rules /opt/tests/sql/silver_rules.sql
"""

from __future__ import annotations

import argparse
import json
import sys

from validate_bronze import load_checks

TABLES = ("chat_message", "nlu_prediction", "user_message")
COUNTS_SQL = """
SELECT
  (SELECT count(*) FROM lake.silver.chat_message WHERE event_date = DATE '{d}') AS chat_rows,
  (SELECT count_if(is_client) FROM lake.silver.chat_message WHERE event_date = DATE '{d}') AS client_rows,
  (SELECT count_if(is_client AND is_home) FROM lake.silver.chat_message WHERE event_date = DATE '{d}')
    AS client_home_rows,
  (SELECT count_if(is_internal_sender) FROM lake.silver.chat_message WHERE event_date = DATE '{d}')
    AS internal_sender_rows,
  (SELECT count_if(is_internal_sender AND is_client) FROM lake.silver.chat_message WHERE event_date = DATE '{d}')
    AS internal_sender_client_rows,
  (SELECT count(*) FROM lake.silver.nlu_prediction WHERE event_date = DATE '{d}') AS predict_rows,
  (SELECT count_if(NOT has_chat_row) FROM lake.silver.nlu_prediction WHERE event_date = DATE '{d}')
    AS predict_without_chat_row,
  (SELECT count(*) FROM lake.silver.user_message WHERE event_date = DATE '{d}') AS user_message_rows,
  (SELECT count_if(has_prediction) FROM lake.silver.user_message WHERE event_date = DATE '{d}')
    AS user_message_with_prediction"""


def expected_for_day(doc: dict, day: str) -> dict:
    d = doc["days"].get(day)
    if d is None:
        sys.exit(f"[validate_silver] no expected values for {day}: not a data day")
    return d


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest-date", required=True, help="YYYY-MM-DD: the Silver event_date to check")
    ap.add_argument("--expected", required=True)
    ap.add_argument("--rules", required=True)
    args = ap.parse_args()
    from silver_transform import check_day

    day = check_day(args.ingest_date)
    with open(args.expected) as f:
        want = expected_for_day(json.load(f), day)

    from pyspark.sql import SparkSession  # imported here so the module imports without Spark

    spark = SparkSession.builder.appName("validate_silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    results: list[bool] = []

    def check(name: str, actual, expected) -> None:
        ok = actual == expected
        results.append(ok)
        verdict = "PASS" if ok else "FAIL"
        print(f"[validate_silver] {verdict}  {name:<48} expected={expected}  actual={actual}", flush=True)

    got = spark.sql(COUNTS_SQL.format(d=day)).collect()[0].asDict()
    for key, value in got.items():
        check(key, value, want[key])
    rows = spark.sql(
        f"SELECT final_intent, count(*) FROM lake.silver.nlu_prediction WHERE event_date = DATE '{day}' "
        "GROUP BY final_intent"
    ).collect()
    check("final_intent", {r[0]: r[1] for r in sorted(rows)}, want["final_intent"])

    for name, sql in load_checks(args.rules, marker="rule").items():
        check(f"rule {name}", spark.sql(sql.replace("${ingest_date}", day)).collect()[0][0], 0)

    for table in TABLES:
        types = dict(spark.table(f"lake.silver.{table}").dtypes)
        check(f"{table}.event_ts type", types.get("event_ts"), "timestamp_ntz")
        check(f"{table}.event_date type", types.get("event_date"), "date")

    spark.stop()
    failed = results.count(False)
    print(f"[validate_silver] {len(results) - failed}/{len(results)} checks passed for event_date={day}", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
