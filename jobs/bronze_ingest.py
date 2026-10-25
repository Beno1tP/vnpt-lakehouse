"""Task 1 — Excel -> Bronze Iceberg tables on MinIO (idempotent per ingest date).

Run inside the spark container:
    spark-submit --master local[2] /opt/jobs/bronze_ingest.py --ingest-date 2026-10-15
Rerunning with the same --ingest-date replaces that partition (no duplicates).

Task 2: --source s3://landing reads that day's files from the landing zone instead of the local folder
(<source>/<table>/dt=<ingest-date>/<file>); endpoint from S3_ENDPOINT, keys from AWS_* env. Default = Task 1.
"""
from __future__ import annotations

import argparse
import io
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SOURCES = {
    # table name        : (file name,                          sheet)
    "bronze.chatlog":     ("bot_dvc_hcm.xlsx",                 "Export Worksheet"),
    "bronze.bot_predict": ("bot_ddvc_hcm_bot_predict.xlsx",    "Export Worksheet"),
}
RAW_DIR = "/data/raw"


def source_path(source: str, table: str, fname: str, ingest_date: str) -> str:
    if source.startswith("s3://"):  # landing zone: one export per table and day
        return f"{source.rstrip('/')}/{table.split('.', 1)[1]}/dt={ingest_date}/{fname}"
    return f"{source}/{fname}"


def read_excel_as_strings(path: str, sheet: str) -> pd.DataFrame:
    """Bronze keeps everything as raw text; typing happens in Silver."""
    if path.startswith("s3://"):
        import pyarrow.fs as pafs
        fs = pafs.S3FileSystem(endpoint_override=os.environ["S3_ENDPOINT"], scheme="http", region="us-east-1")
        with fs.open_input_file(path[len("s3://"):]) as f:  # missing file -> FileNotFoundError -> job fails
            path = io.BytesIO(f.read())
    pdf = pd.read_excel(path, sheet_name=sheet, dtype=str, engine="openpyxl")
    return pdf.astype(object).where(pdf.notna(), None)  # NaN -> None (Spark null)


def to_spark(spark: SparkSession, pdf: pd.DataFrame):
    schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    return spark.createDataFrame(pdf, schema=schema)


def write_bronze(spark: SparkSession, table: str, df) -> None:
    full = f"lake.{table}"
    if spark.catalog.tableExists(full):
        df.writeTo(full).overwritePartitions()          # idempotent rerun
    else:
        (df.writeTo(full).using("iceberg")
           .partitionedBy(F.col("_ingest_date"))
           .tableProperty("format-version", "2")
           .create())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest-date", required=True, help="YYYY-MM-DD; the partition to (re)write")
    ap.add_argument("--source", default=RAW_DIR, help=f"folder with the Excel files (default {RAW_DIR}) or s3://landing")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()

    for table, (fname, sheet) in SOURCES.items():
        path = source_path(args.source, table, fname, args.ingest_date)
        pdf = read_excel_as_strings(path, sheet)
        df = (to_spark(spark, pdf)
              .withColumn("_ingest_date", F.to_date(F.lit(args.ingest_date)))
              .withColumn("_ingested_at", F.lit(ingested_at))
              .withColumn("_source_file", F.lit(path if path.startswith("s3://") else fname))
              .withColumn("_batch_id", F.lit(batch_id)))
        write_bronze(spark, table, df)
        n = spark.table(f"lake.{table}").where(F.col("_ingest_date") == args.ingest_date).count()
        print(f"[bronze] {table}: {len(pdf)} rows read, {n} rows in partition {args.ingest_date}")
        if n != len(pdf):  # fail loudly (non-zero exit) so the caller retries/alerts instead of trusting a bad load
            raise RuntimeError(f"{table}: {n} rows in partition {args.ingest_date} != {len(pdf)} rows read")

    spark.stop()


if __name__ == "__main__":
    main()
