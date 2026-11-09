"""Task 2 step 2.5b drill 7: catalog administration for Iceberg tables in the Hive catalog `lake`.

    info     --table bronze.chatlog [--table …]   current metadata file, snapshots, rows (counts only)
    forget   --table bronze.chatlog               remove the table from the CATALOG only (DROP TABLE without PURGE:
                                                 Parquet + metadata files stay in MinIO)
    register --table bronze.chatlog --metadata s3a://lakehouse/warehouse/bronze.db/chatlog/metadata/<file>.metadata.json
                                                 re-create the catalog entry from an existing metadata.json
                                                 (CALL lake.system.register_table)
    tag      --table … --tag task2_v1 [--retain-days 365]
                                                 freeze: ALTER TABLE … CREATE TAG IF NOT EXISTS (keeps an existing
                                                 tag where it is), then verify-tag
    verify-tag --table … --tag task2_v1          rows VERSION AS OF the tag = expected totals (bronze_daily.json);
                                                 exit 1 on a mismatch or a missing tag
Inputs are validated before they reach SQL (identifiers and paths only from a strict pattern).
"""

from __future__ import annotations

import argparse
import re
import sys

TABLE_RE = re.compile(r"^bronze\.(chatlog|bot_predict)$")
TAG_RE = re.compile(r"^task[2-5]_v[0-9]{1,3}$")  # one freeze tag per task version, e.g. task2_v1
EXPECTED = "/opt/tests/expected/bronze_daily.json"  # baked into the spark-job image
EXPECTED_KEY = {"bronze.chatlog": "chatlog_rows", "bronze.bot_predict": "bot_predict_rows"}
METADATA_RE = re.compile(
    r"^s3a://lakehouse/warehouse/bronze\.db/(chatlog|bot_predict)/metadata/[0-9A-Za-z._-]+\.metadata\.json$"
)


def check_table(table: str) -> str:
    if not TABLE_RE.match(table):
        raise ValueError(f"table must be bronze.chatlog or bronze.bot_predict, got {table!r}")
    return table


def register_sql(table: str, metadata: str) -> str:
    check_table(table)
    m = METADATA_RE.match(metadata)
    if not m or m.group(1) != table.split(".", 1)[1]:
        raise ValueError(f"metadata file must be a .metadata.json of {table} under s3a://lakehouse/warehouse/")
    return f"CALL lake.system.register_table(table => '{table}', metadata_file => '{metadata}')"


def check_tag(tag: str) -> str:
    if not TAG_RE.match(tag):
        raise ValueError(f"tag must look like task2_v1 (task2..task5, _v + number), got {tag!r}")
    return tag


def tag_sql(table: str, tag: str, retain_days: int) -> str:
    if not 1 <= retain_days <= 3650:
        raise ValueError(f"retain days must be 1..3650, got {retain_days}")
    return f"ALTER TABLE lake.{check_table(table)} CREATE TAG IF NOT EXISTS {check_tag(tag)} RETAIN {retain_days} DAYS"


def expected_rows(table: str, expected: dict) -> int:
    return int(expected["totals"][EXPECTED_KEY[check_table(table)]])


def verify_tag(spark, table: str, tag: str, expected: dict) -> bool:
    t = f"lake.{check_table(table)}"
    ref_sql = f"SELECT snapshot_id, max_reference_age_in_ms FROM {t}.refs WHERE name = '{check_tag(tag)}'"
    refs = spark.sql(ref_sql).collect()
    if not refs:
        print(f"[catalog] FAIL {table}: tag {tag} not found", flush=True)
        return False
    rows = spark.sql(f"SELECT count(*) FROM {t} VERSION AS OF '{tag}'").collect()[0][0]
    want = expected_rows(table, expected)
    days = (refs[0][1] or 0) // 86_400_000
    ok = rows == want
    print(
        f"[catalog] {'PASS' if ok else 'FAIL'} {table}: tag {tag} -> snapshot {refs[0][0]}, retain {days} d, "
        f"rows {rows} (expected {want})",
        flush=True,
    )
    return ok


def info(spark, table: str) -> None:
    t = f"lake.{check_table(table)}"
    meta = spark.sql(f"SELECT file FROM {t}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1").collect()[0][0]
    snaps = spark.sql(f"SELECT count(*), max(committed_at) FROM {t}.snapshots").collect()[0]
    rows = spark.table(t).count()
    print(f"[catalog] {table}: metadata={meta} snapshots={snaps[0]} last_commit={snaps[1]} rows={rows}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["info", "forget", "register", "tag", "verify-tag"])
    ap.add_argument("--table", action="append", required=True)
    ap.add_argument("--metadata", help="register: s3a://…/metadata/<file>.metadata.json")
    ap.add_argument("--tag", help="tag / verify-tag: e.g. task2_v1")
    ap.add_argument("--retain-days", type=int, default=365)
    args = ap.parse_args()
    if args.command in ("tag", "verify-tag") and not args.tag:
        sys.exit(f"{args.command} needs --tag")
    if args.command == "tag":
        sqls_tag = [tag_sql(t, args.tag, args.retain_days) for t in args.table]  # validate before Spark starts
    if args.command == "register" and (len(args.table) != 1 or not args.metadata):
        sys.exit("register needs exactly one --table and --metadata")
    sqls = [register_sql(args.table[0], args.metadata)] if args.command == "register" else []

    from pyspark.sql import SparkSession  # imported here so the helpers can be tested without Spark

    spark = SparkSession.builder.appName(f"catalog_{args.command}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    if args.command == "forget":
        for table in args.table:
            spark.sql(f"DROP TABLE lake.{check_table(table)}")  # no PURGE: files stay
            print(f"[catalog] {table}: removed from the catalog (files kept)", flush=True)
    for sql in sqls:
        spark.sql(sql).collect()
        print(f"[catalog] {args.table[0]}: registered from {args.metadata}", flush=True)
    if args.command in ("info", "register"):
        for table in args.table:
            info(spark, table)
    ok = True
    if args.command in ("tag", "verify-tag"):
        import json

        if args.command == "tag":
            for table, sql in zip(args.table, sqls_tag, strict=True):
                spark.sql(sql).collect()
                print(f"[catalog] {table}: tag {args.tag} created (or already present: kept where it was)", flush=True)
        with open(EXPECTED) as f:
            expected = json.load(f)
        ok = all([verify_tag(spark, t, args.tag, expected) for t in args.table])
    spark.stop()
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
