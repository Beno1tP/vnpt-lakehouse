"""Task 1 — Excel -> Bronze Iceberg tables on MinIO (idempotent per ingest date).

Run inside the spark container:
    spark-submit --master local[2] /opt/jobs/bronze_ingest.py --ingest-date 2026-10-15
Rerunning with the same --ingest-date replaces that partition (no duplicates).
"""
from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

SOURCES = {
    # table name        : (file name,                          sheet)
    "bronze.chatlog":     ("bot_dvc_hcm.xlsx",                 "Export Worksheet"),
    "bronze.bot_predict": ("bot_ddvc_hcm_bot_predict.xlsx",    "Export Worksheet"),
}
RAW_DIR = "/data/raw"


def read_excel_as_strings(path: str, sheet: str) -> pd.DataFrame:
    """Bronze keeps everything as raw text; typing happens in Silver."""
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
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()

    for table, (fname, sheet) in SOURCES.items():
        pdf = read_excel_as_strings(f"{RAW_DIR}/{fname}", sheet)
        df = (to_spark(spark, pdf)
              .withColumn("_ingest_date", F.to_date(F.lit(args.ingest_date)))
              .withColumn("_ingested_at", F.lit(ingested_at))
              .withColumn("_source_file", F.lit(fname))
              .withColumn("_batch_id", F.lit(batch_id)))
        write_bronze(spark, table, df)
        n = spark.table(f"lake.{table}").where(F.col("_ingest_date") == args.ingest_date).count()
        print(f"[bronze] {table}: {len(pdf)} rows read, {n} rows in partition {args.ingest_date}")

    spark.stop()


if __name__ == "__main__":
    main()
