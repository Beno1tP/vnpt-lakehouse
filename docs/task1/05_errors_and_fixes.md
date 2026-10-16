# Task 1 — Errors & fixes

Pre-filled with **likely** errors for this stack (marked *expected*). Replace them with what you actually hit (marked *seen*).

| ID | Symptom | Root cause | Fix | Theory | Status |
|---|---|---|---|---|---|
| E01 | `pull access denied for minio/minio` and `minio/mc` (2026-10-16) | MinIO community images are no longer published on Docker Hub | Use `cgr.dev/chainguard/minio` pinned by digest; bucket init with `amazon/aws-cli` (`aws --endpoint-url http://minio:9000 s3 mb`); `busybox` one-shot `chown 65532` on the data volume because the Chainguard image runs as non-root | Images are published and tagged by their owner; pin by **digest** for reproducibility; non-root containers need writable volumes | **seen → fixed** |
| E02 | `bind source path does not exist` for raw data | `RAW_DATA_DIR` wrong (Windows vs WSL path) | Use a WSL path (`/home/<you>/data/vnpt-lab`) and run `make` from WSL | Bind mounts resolve paths on the Docker host | expected |
| E03 | Metastore: `Connection refused postgres:5432` at start | Started before Postgres was ready | `depends_on: condition: service_healthy` (already set); `restart: unless-stopped` | Startup ordering vs readiness (same idea as K8s readiness probes) | expected |
| E04 | Metastore: `ClassNotFoundException org.postgresql.Driver` | JDBC jar missing or not at `/opt/hive/lib/postgres.jar` | Check the `ADD` in `hive/Dockerfile`; `docker compose exec hive-metastore ls /opt/hive/lib \| grep postgres` | JVM classpath | expected |
| E05a | (verified) hadoop-common in `apache/hive:4.0.1` = **3.3.6** | — | `HADOOP_AWS_VERSION=3.3.6` is correct | Jar binary compatibility | **checked** |
| E05 | `No FileSystem for scheme "s3a"` or `ClassNotFound S3AFileSystem` on `CREATE NAMESPACE` | The **Metastore** (not Spark) tries to create the DB directory on S3 without hadoop-aws | hadoop-aws + aws-sdk-bundle in `/opt/hive/lib` and `core-site.xml` (already in Dockerfile); **hadoop-aws version must equal Hive's hadoop-common** | HMS creates DB locations through the Hadoop FileSystem API | expected |
| E06 | `NoSuchMethodError` / `ClassNotFound` in Hadoop/AWS classes | hadoop-aws ≠ hadoop-common version, or wrong SDK bundle | Match versions exactly (`ls /opt/hadoop/share/hadoop/common`) | Binary compatibility between jars | expected |
| E07 | Metastore: `schema already exists` / init failure on 2nd start | The image runs `schematool -initSchema` on every container start | Set `IS_RESUME: "true"` in compose after the first successful start (now needed: schema 3.1.0 already in Postgres) | Idempotent init | **applied 10-16** |
| E08 | Spark: `UnknownHostException: lakehouse.minio` | Virtual-host style addressing | `s3.path-style-access=true` (set) | S3 addressing styles | expected |
| E09 | Spark: `403 Forbidden` / `InvalidAccessKeyId` | Credentials not passed to the Spark container | `AWS_ACCESS_KEY_ID/SECRET` env in Compose; recreate the container after editing `.env` | S3FileIO credential chain | expected |
| E10 | Spark: `Unable to load region` | AWS SDK needs a region even for MinIO | `client.region=us-east-1` + `AWS_REGION` (set) | AWS SDK v2 behaviour | expected |
| E11 | Spark: `Cannot find catalog plugin class` / `ClassNotFound SparkCatalog` | Iceberg runtime jar missing or for the wrong Spark/Scala version | Jar must be `iceberg-spark-runtime-3.5_2.12`; check `/opt/spark/jars` | Spark catalog plugin API | expected |
| E12 | Exit code 137 / container killed | OOM: over `mem_limit` | Lower `spark.driver.memory` or raise `mem_limit`; close other apps; `make stats` | cgroups memory limit | expected |
| E13 | Duplicate rows after rerun | Used `append` instead of `overwritePartitions` | Keep the job's write mode; check `.snapshots` operation = `overwrite` | Idempotency | expected |
| E14 | `CommitFailedException` | Two writers committed at once (optimistic concurrency) | Retry; don't run two loads on the same table at once | Iceberg OCC | expected |
| E15 | Excel read slow or memory spike | openpyxl loads the whole workbook | Fine at 21k rows; at scale convert to CSV/Parquet upstream | Driver-side pandas vs distributed read | expected |

| E16 | `CREATE TABLE lake.tmp.t ...` → `RuntimeException: Failed to get table info from metastore` · **Caused by `TApplicationException: Invalid method name: 'get_table'`** (2026-10-16). `SHOW NAMESPACES` and `CREATE NAMESPACE` worked | **Thrift API mismatch**: Spark 3.5 bundles the **Hive 2.3.x** metastore client, which Iceberg's HiveCatalog uses; it calls the old `get_table` method. **HMS 4.x no longer serves it** (Hive 4 moved clients to the newer request-style APIs, HIVE-26537). Database calls are still compatible, table calls are not | Run **HMS 3.1.3** (`apache/hive:3.1.3`), match `hadoop-aws` to its Hadoop version, wipe the Postgres volume (schema 4.0 → 3.x is not a downgrade path) | Client/server RPC compatibility: a newer server may drop methods older clients still call; pin server versions to what your clients speak | **seen → fixing** |

| E17 | After the HMS 3.1.3 switch: `CREATE NAMESPACE lake.tmp` → `WARN RetryingMetaStoreClient: MetaStoreClient lost connection … createDatabase` + `TTransportException` (2026-10-16). `SHOW NAMESPACES` OK | **Suspected E06**: `.env` had `HADOOP_AWS_VERSION=3.1.3`, but the image's hadoop-common is **3.1.0**. `createDatabase` is the first call where the Metastore uses S3A (to create `tmp.db` on MinIO); a jar mismatch throws inside the Thrift handler and the connection drops | `HADOOP_AWS_VERSION=3.1.0` (SDK bundle 1.11.271), rebuild; confirm in the Metastore log | The client only sees "connection lost": the real error is in the **server** log | **seen → fixing** |

## How to debug, in order
1. `make ps`: which container is down or restarting?
2. `make logs SVC=<name>`: read the **first** error, not the last.
3. `docker compose exec <svc> bash`: check files, jars, env (`env | grep AWS`), DNS (`getent hosts minio`).
4. Reproduce with the smallest case (step 1.5 smoke test).
5. Log it here: symptom → root cause → fix → theory.
