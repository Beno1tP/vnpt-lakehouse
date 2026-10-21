"""Task 1 — validate the Bronze tables for one ingest date against versioned expectations.

Runs every "-- check: <table>.<key>" query from a SQL file, compares the result with
tests/expected/bronze.json, prints one PASS/FAIL line per check (counts only, never row
contents) and exits 1 if any check fails, so `make acceptance` (and later an Airflow task)
fails loudly instead of relying on someone reading numbers.

    spark-submit validate_bronze.py --ingest-date 2026-10-16 \
        --expected /opt/tests/expected/bronze.json --sql /opt/tests/sql/bronze_checks.sql [--check-idempotency]
"""
from __future__ import annotations

import argparse
import json
import re
import sys

TABLES = ("chatlog", "bot_predict")


def load_checks(path: str) -> dict[str, str]:
    """Parse '-- check: <name>' blocks into {name: sql}; other comment lines are ignored."""
    checks: dict[str, str] = {}
    name, lines = None, []
    with open(path) as f:
        for line in f:
            m = re.match(r"\s*--\s*check:\s*(\S+)", line)
            if m:
                if name:
                    checks[name] = "".join(lines).strip().rstrip(";")
                name, lines = m.group(1), []
            elif name and not line.lstrip().startswith("--"):
                lines.append(line)
    if name:
        checks[name] = "".join(lines).strip().rstrip(";")
    return checks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest-date", required=True, help="YYYY-MM-DD; the partition to check")
    ap.add_argument("--expected", required=True)
    ap.add_argument("--sql", required=True)
    ap.add_argument("--check-idempotency", action="store_true",
                    help="also assert the latest snapshot replaced exactly one batch (run after a reload)")
    args = ap.parse_args()

    with open(args.expected) as f:
        expected = json.load(f)

    from pyspark.sql import SparkSession  # imported here so load_checks can be tested without Spark

    spark = SparkSession.builder.appName("validate_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    results: list[bool] = []

    def check(name: str, actual, want) -> None:
        ok = actual == want
        results.append(ok)
        print(f"[validate] {'PASS' if ok else 'FAIL'}  {name:<36} expected={want}  actual={actual}", flush=True)

    for name, sql in load_checks(args.sql).items():
        table, key = name.split(".", 1)
        actual = spark.sql(sql.replace("${ingest_date}", args.ingest_date)).collect()[0][0]
        check(name, actual, expected[table][key])

    for table in TABLES:
        check(f"{table}.columns", len(spark.table(f"lake.bronze.{table}").columns), expected[table]["columns"])

    if args.check_idempotency:
        for table in TABLES:
            last = spark.sql(
                f"SELECT operation, summary['added-records'] AS added, summary['deleted-records'] AS deleted "
                f"FROM lake.bronze.{table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0]
            rows = expected[table]["rows"]
            check(f"{table}.last_snapshot_operation", last["operation"], "overwrite")
            check(f"{table}.last_snapshot_added", int(last["added"] or 0), rows)
            check(f"{table}.last_snapshot_deleted", int(last["deleted"] or 0), rows)

    spark.stop()
    failed = results.count(False)
    print(f"[validate] {len(results) - failed}/{len(results)} checks passed for _ingest_date={args.ingest_date}", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
