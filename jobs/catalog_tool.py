"""Task 2 step 2.5b drill 7: catalog administration for Iceberg tables in the Hive catalog `lake`.

    info     --table bronze.chatlog [--table …]   current metadata file, snapshots, rows (counts only)
    forget   --table bronze.chatlog               remove the table from the CATALOG only (DROP TABLE without PURGE:
                                                 Parquet + metadata files stay in MinIO)
    register --table bronze.chatlog --metadata s3a://lakehouse/warehouse/bronze.db/chatlog/metadata/<file>.metadata.json
                                                 re-create the catalog entry from an existing metadata.json
                                                 (CALL lake.system.register_table)
Inputs are validated before they reach SQL (identifiers and paths only from a strict pattern).
"""

from __future__ import annotations

import argparse
import re
import sys

TABLE_RE = re.compile(r"^bronze\.(chatlog|bot_predict)$")
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


def info(spark, table: str) -> None:
    t = f"lake.{check_table(table)}"
    meta = spark.sql(f"SELECT file FROM {t}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1").collect()[0][0]
    snaps = spark.sql(f"SELECT count(*), max(committed_at) FROM {t}.snapshots").collect()[0]
    rows = spark.table(t).count()
    print(f"[catalog] {table}: metadata={meta} snapshots={snaps[0]} last_commit={snaps[1]} rows={rows}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["info", "forget", "register"])
    ap.add_argument("--table", action="append", required=True)
    ap.add_argument("--metadata", help="register: s3a://…/metadata/<file>.metadata.json")
    args = ap.parse_args()
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
    spark.stop()


if __name__ == "__main__":
    main()
