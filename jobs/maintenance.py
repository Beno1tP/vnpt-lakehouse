"""Task 2 step 2.5c (gap #10): Iceberg table maintenance for the Bronze tables. Counts only, never row contents.

Per table, in this order (each step logged with before/after numbers):
  1. table properties: keep at most 10 previous metadata.json files (deleted automatically at the next commits)
  2. expire_snapshots: snapshots older than --snapshot-hours, but always keep --retain-last
     (trade-off: time travel to an expired snapshot is no longer possible; the job proves it)
  3. rewrite_data_files (compaction, only where a partition has >= 2 files) and rewrite_manifests
  4. remove_orphan_files older than --orphan-hours (files no snapshot references, e.g. from a failed write or the
     drill-7 restore). prefix_listing=true lists through Iceberg's S3FileIO, so no Hadoop S3A jar is needed. The
     age threshold protects files of a write that is still running: never set it below the longest job.
  5. assert: row count unchanged
--dry-run: only the counts and the orphan files that WOULD be removed (expire/rewrite skipped).
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta, timezone

TABLE_RE = re.compile(r"^(bronze\.(chatlog|bot_predict)|scratch\.tag_drill)$")  # scratch: Task 4 tag drill (TABLES=)


def ts_literal(hours_ago: float, now: datetime | None = None) -> str:
    """Spark TIMESTAMP literal in UTC (the session time zone is Asia/Ho_Chi_Minh, so the zone must be explicit)."""
    t = (now or datetime.now(timezone.utc)) - timedelta(hours=hours_ago)
    return f"TIMESTAMP '{t:%Y-%m-%d %H:%M:%S} UTC'"


def check_table(table: str) -> str:
    if not TABLE_RE.match(table):
        raise ValueError(f"unsupported table {table!r}")
    return table


def stats(spark, t: str) -> dict:
    row = spark.sql(
        f"SELECT (SELECT count(*) FROM {t}.snapshots) AS snapshots,"
        f" (SELECT count(*) FROM {t}.files) AS live_files,"
        f" (SELECT count(DISTINCT file_path) FROM {t}.all_data_files) AS all_files,"
        f" (SELECT coalesce(sum(file_size_in_bytes), 0) FROM {t}.files) AS live_bytes,"
        f" (SELECT count(*) FROM {t}.metadata_log_entries) AS metadata_files,"
        f" (SELECT count(*) FROM {t}) AS row_count"
    ).collect()[0]
    return row.asDict()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", action="append", required=True)
    ap.add_argument("--snapshot-hours", type=float, default=168)
    ap.add_argument("--retain-last", type=int, default=5)
    ap.add_argument("--orphan-hours", type=float, default=72)
    ap.add_argument("--dry-run", default="false")
    args = ap.parse_args()
    dry = args.dry_run.lower() == "true"

    from pyspark.sql import SparkSession  # imported here so the helpers can be tested without Spark

    spark = SparkSession.builder.appName("lake_maintenance").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    for table in args.table:
        t = f"lake.{check_table(table)}"
        before = stats(spark, t)
        print(f"[maint] {table} before: {before}", flush=True)
        oldest = spark.sql(
            f"SELECT snapshot_id, committed_at FROM {t}.snapshots ORDER BY committed_at LIMIT 1"
        ).collect()[0]
        if not dry:
            spark.sql(
                f"ALTER TABLE {t} SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled'='true',"
                f" 'write.metadata.previous-versions-max'='10')"
            )
            older = ts_literal(args.snapshot_hours)
            r = spark.sql(
                f"CALL lake.system.expire_snapshots(table => '{table}', "
                f"older_than => {older}, retain_last => {args.retain_last})"
            ).collect()[0]
            print(f"[maint] {table} expire_snapshots: {r.asDict()}", flush=True)
            r = spark.sql(
                f"CALL lake.system.rewrite_data_files(table => '{table}', options => map('min-input-files', '2'))"
            ).collect()[0]
            print(f"[maint] {table} rewrite_data_files: {r.asDict()}", flush=True)
            r = spark.sql(f"CALL lake.system.rewrite_manifests('{table}')").collect()[0]
            print(f"[maint] {table} rewrite_manifests: {r.asDict()}", flush=True)
        orphans = spark.sql(
            f"CALL lake.system.remove_orphan_files(table => '{table}', "
            f"older_than => {ts_literal(args.orphan_hours)}, prefix_listing => true, "
            f"dry_run => {str(dry).lower()})"
        ).count()
        print(f"[maint] {table} remove_orphan_files: {orphans} files {'would be ' if dry else ''}removed", flush=True)
        after = stats(spark, t)
        print(f"[maint] {table} after: {after}", flush=True)
        if after["row_count"] != before["row_count"]:
            raise RuntimeError(f"{table}: row count changed {before['row_count']} -> {after['row_count']}")
        if not dry:
            try:
                spark.sql(f"SELECT count(*) FROM {t} VERSION AS OF {oldest['snapshot_id']}").collect()
                print(
                    f"[maint] {table} time travel to oldest snapshot {oldest['committed_at']}: still possible",
                    flush=True,
                )
            except Exception as e:  # noqa: BLE001 - expected after expiry
                print(
                    f"[maint] {table} time travel to oldest snapshot {oldest['committed_at']}: gone "
                    f"({type(e).__name__}) = expected trade-off of expire_snapshots",
                    flush=True,
                )
    spark.stop()


if __name__ == "__main__":
    main()
