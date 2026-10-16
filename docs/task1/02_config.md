# Task 1 — Config: what each setting does

> The config files are **untested starting points**; I couldn't pull Docker images here. Expect to fix small issues; log them in `05_errors_and_fixes.md`.

## Files
| File | Purpose |
|---|---|
| `infra/compose/docker-compose.yml` | MinIO, bucket init, Postgres, Hive Metastore, Spark runner |
| `infra/compose/.env.example` | Credentials + versions → copy to `.env` (git-ignored) |
| `infra/compose/hive/Dockerfile` | Adds the Postgres JDBC driver + `hadoop-aws` to the Metastore image |
| `infra/compose/hive/core-site.xml` | S3A settings so the Metastore can reach MinIO |
| `docker/spark/Dockerfile` | Spark 3.5.9 + Iceberg 1.11.0 jars + pandas/openpyxl |
| `docker/spark/spark-defaults.conf` | Iceberg catalog `lake` → Hive Metastore + MinIO |
| `jobs/bronze_ingest.py` | Excel → Bronze Iceberg (idempotent) |
| `Makefile` | `up`, `down`, `bronze`, `logs`, `clean` |

## Versions (pinned in `.env`)
| Var | Value | Note |
|---|---|---|
| `SPARK_IMAGE` | `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` | If this tag doesn't exist, use `...-python3-r-ubuntu` (verified to exist, larger because of R) |
| `ICEBERG_VERSION` | `1.11.0` | Latest 1.11 on the Iceberg releases page (1.12.0 deferred) |
| `HIVE_IMAGE` | `apache/hive:4.0.1` | Verified tag. Lighter alternative: `apache/hive:standalone-metastore-4.1.0` (check that its env vars match) |
| `HADOOP_AWS_VERSION` | must equal Hive's bundled Hadoop | Check inside the container: `ls /opt/hadoop/share/hadoop/common/ \| grep hadoop-common` |
| Postgres | `postgres:16` | |
| MinIO | pin a tag you can pull | MinIO changed its community distribution in 2025; check that `docker pull` works first |

## Key Spark/Iceberg settings (`spark-defaults.conf`)
| Setting | Value | Why |
|---|---|---|
| `spark.sql.extensions` | `IcebergSparkSessionExtensions` | Enables `MERGE`, `CALL` procedures, `ALTER ... PARTITION FIELD` |
| `spark.sql.catalog.lake` | `org.apache.iceberg.spark.SparkCatalog` | Defines catalog `lake` |
| `spark.sql.catalog.lake.type` | `hive` | Pointer stored in the Hive Metastore |
| `spark.sql.catalog.lake.uri` | `thrift://hive-metastore:9083` | Service DNS name |
| `spark.sql.catalog.lake.warehouse` | `s3a://lakehouse/warehouse` | Root for tables |
| `spark.sql.catalog.lake.io-impl` | `org.apache.iceberg.aws.s3.S3FileIO` | Iceberg's own S3 client (from `iceberg-aws-bundle`) |
| `spark.sql.catalog.lake.s3.endpoint` | `http://minio:9000` | MinIO instead of AWS |
| `spark.sql.catalog.lake.s3.path-style-access` | `true` | Required for MinIO |
| `spark.sql.catalog.lake.client.region` | `us-east-1` | The AWS SDK requires a region; MinIO ignores it |
| `spark.sql.session.timeZone` | `Asia/Ho_Chi_Minh` | Oracle timestamps carry no zone; be explicit |
| `spark.sql.execution.arrow.pyspark.enabled` | `true` | Faster pandas → Spark |
| `spark.driver.memory` | `2g` | Enough for ~32k rows; keep the container limit at 3g |

Credentials come from env vars `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (read by S3FileIO).

## Table design (Bronze)
| Table | Columns | Partition | Write mode |
|---|---|---|---|
| `lake.bronze.chatlog` | 15 source columns as **string** + `_ingest_date`, `_ingested_at`, `_source_file`, `_batch_id` | `_ingest_date` | `overwritePartitions()` |
| `lake.bronze.bot_predict` | 17 source columns as **string** + same metadata | `_ingest_date` | `overwritePartitions()` |

All columns are kept as strings in Bronze: raw as landed. Typing and parsing happen in Silver.
