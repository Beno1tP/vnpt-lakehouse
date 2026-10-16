# Task 1 — Errors & fixes

Pre-filled with **likely** errors for this stack (marked *expected*). Replace them with what you actually hit (marked *seen*).

| ID | Symptom | Root cause | Fix | Theory | Status |
|---|---|---|---|---|---|
| E01 | `pull access denied` / `manifest unknown` for a MinIO or Spark tag | Tag doesn't exist, or the distribution changed | `docker pull <tag>` to test; pin a tag that exists; for Spark try `...-python3-r-ubuntu` | Images are content-addressed and tagged by the publisher | expected |
| E02 | `bind source path does not exist` for raw data | `RAW_DATA_DIR` wrong (Windows vs WSL path) | Use a WSL path (`/home/<you>/data/vnpt-lab`) and run `make` from WSL | Bind mounts resolve paths on the Docker host | expected |
| E03 | Metastore: `Connection refused postgres:5432` at start | Started before Postgres was ready | `depends_on: condition: service_healthy` (already set); `restart: unless-stopped` | Startup ordering vs readiness (same idea as K8s readiness probes) | expected |
| E04 | Metastore: `ClassNotFoundException org.postgresql.Driver` | JDBC jar missing or not at `/opt/hive/lib/postgres.jar` | Check the `ADD` in `hive/Dockerfile`; `docker compose exec hive-metastore ls /opt/hive/lib \| grep postgres` | JVM classpath | expected |
| E05 | `No FileSystem for scheme "s3a"` or `ClassNotFound S3AFileSystem` on `CREATE NAMESPACE` | The **Metastore** (not Spark) tries to create the DB directory on S3 without hadoop-aws | hadoop-aws + aws-sdk-bundle in `/opt/hive/lib` and `core-site.xml` (already in Dockerfile); **hadoop-aws version must equal Hive's hadoop-common** | HMS creates DB locations through the Hadoop FileSystem API | expected |
| E06 | `NoSuchMethodError` / `ClassNotFound` in Hadoop/AWS classes | hadoop-aws ≠ hadoop-common version, or wrong SDK bundle | Match versions exactly (`ls /opt/hadoop/share/hadoop/common`) | Binary compatibility between jars | expected |
| E07 | Metastore: `schema already exists` / init failure on 2nd start | Schema init runs every start | Set `IS_RESUME=true` after the first successful start | Idempotent init | expected |
| E08 | Spark: `UnknownHostException: lakehouse.minio` | Virtual-host style addressing | `s3.path-style-access=true` (set) | S3 addressing styles | expected |
| E09 | Spark: `403 Forbidden` / `InvalidAccessKeyId` | Credentials not passed to the Spark container | `AWS_ACCESS_KEY_ID/SECRET` env in Compose; recreate the container after editing `.env` | S3FileIO credential chain | expected |
| E10 | Spark: `Unable to load region` | AWS SDK needs a region even for MinIO | `client.region=us-east-1` + `AWS_REGION` (set) | AWS SDK v2 behaviour | expected |
| E11 | Spark: `Cannot find catalog plugin class` / `ClassNotFound SparkCatalog` | Iceberg runtime jar missing or for the wrong Spark/Scala version | Jar must be `iceberg-spark-runtime-3.5_2.12`; check `/opt/spark/jars` | Spark catalog plugin API | expected |
| E12 | Exit code 137 / container killed | OOM: over `mem_limit` | Lower `spark.driver.memory` or raise `mem_limit`; close other apps; `make stats` | cgroups memory limit | expected |
| E13 | Duplicate rows after rerun | Used `append` instead of `overwritePartitions` | Keep the job's write mode; check `.snapshots` operation = `overwrite` | Idempotency | expected |
| E14 | `CommitFailedException` | Two writers committed at once (optimistic concurrency) | Retry; don't run two loads on the same table at once | Iceberg OCC | expected |
| E15 | Excel read slow or memory spike | openpyxl loads the whole workbook | Fine at 21k rows; at scale convert to CSV/Parquet upstream | Driver-side pandas vs distributed read | expected |

## How to debug, in order
1. `make ps`: which container is down or restarting?
2. `make logs SVC=<name>`: read the **first** error, not the last.
3. `docker compose exec <svc> bash`: check files, jars, env (`env | grep AWS`), DNS (`getent hosts minio`).
4. Reproduce with the smallest case (step 1.5 smoke test).
5. Log it here: symptom → root cause → fix → theory.
