# Task 1 — Process log

One entry per work block. Newest first. Keep it factual: what you did, what you saw, what you learned.

## Template
```
### YYYY-MM-DD HH:MM — <step id> <short title>
Did:
Observed (numbers, logs, screenshots path):
Learned (theory link):
Next:
Time spent:
```

---

### 2026-10-16 — 1.4 E17 fixed; HMS on the official image (no build)
Did:
- Confirmed E17 in `/tmp/hive/hive.log` (the real HMS log; `docker logs` only shows the console): Guava `NoSuchMethodError` inside `S3AFileSystem.initialize` during `create_database`.
- Added one-shot `jars-init` (`curlimages/curl:8.22.0`, script `infra/compose/fetch-jars.sh`) → downloads `hadoop-aws-3.1.0`, `aws-java-sdk-bundle-1.11.271`, `postgresql-42.7.4` into git-ignored `infra/compose/jars/`, SHA-1 checked against Maven Central.
- `hive-metastore` now `image: apache/hive:3.1.3` + jars and `core-site.xml` bind-mounted; `IS_RESUME=true` (E07).
- `.env.example` rewritten: digest pins (MinIO, aws-cli), `HADOOP_AWS_VERSION=3.1.0`, `CURL_IMAGE`, correct comments and `RAW_DATA_DIR`.
Observed: jars in `/opt/hive/lib` = 3.1.0 / 1.11.271 / postgres.jar (42.7.4); smoke test OK (2 rows, snapshot `append`); MinIO shows `tmp.db/t/{metadata/*.metadata.json, *.avro, data/*.parquet}`. Harmless warnings: SLF4J multiple bindings (HMS); `ObjectStore … schema version 2.3.0` in Spark = Spark's own embedded Derby metastore for `spark_catalog`, not our HMS.
Learned: transitive dependency drift inside a patch line (Hadoop 3.1.3 → Guava 27) breaks binary compatibility; the client only sees "connection lost", so read the server log.
Next: 4d Spark on the official image (`spark.jars.packages` + Ivy volume, pip deps in a volume).

### 2026-10-16 — 1.3b Repo audit + disk cleanup (Claude Code session)
Did: read handoff docs vs real files; audited disk; removed `apache/hive:4.0.1`, `apache/airflow:3.3.2`, all build cache, volume `airflow_postgres-db-volume` (Bee typed the commands).
Observed:
- Before: images 11.73 GB, build cache 6.19 GB, volumes 0.12 GB. After: images 6.06 GB, cache 0 B, volumes 0.05 GB → **~11.9 GB freed inside Docker**.
- On C: `docker_data.vhdx` 22.9 GB, Ubuntu `ext4.vhdx` 6.4 GB (holds 4.7 GB). The vhdx files do not shrink until compacted (planned after E17).
- Running HMS container has `hadoop-aws-3.1.3.jar` + `aws-java-sdk-bundle-1.11.271.jar`, image has `hadoop-common-3.1.0.jar` → E17 mismatch confirmed to exist (cause not yet proven). HMS 3.1.3 also ships its own `postgresql-9.4.1208.jre7.jar` next to our `postgres.jar` (42.7.4).
- Spark image Python is **3.10.12** (not 3.11 as in the matrix); `HOME=/nonexistent` for user `spark`.
- Doc/file drift: MinIO and aws-cli still `:latest` (not digest-pinned); `IS_RESUME` still commented out (E07 not applied); `.env.example` comments describe Hive 4.0.1/Hadoop 3.3.6 and `RAW_DATA_DIR=/home/user/...`.
Learned: deleting images frees space inside the WSL virtual disk only; Windows gets it back after `compact vdisk`.
Next: step 3, replace custom images with pulled images + compose config.

### 2026-10-15 — 1.0 Kick-off
Did: confirmed with the leader that tasks 1–5 are standalone mini-projects on the same data. Chose Docker Compose for Task 1. Prepared knowledge/config/plan docs and starter config.
Observed: data profile → chatlog 21,224 rows / predict 10,729 rows / 89 senders / 18 days (see `06_validation.md`).
Learned: why Task 1 needs a catalog (Hive Metastore) even without Trino yet: atomic pointer swap; Hadoop catalog unsafe on S3.
Next: 1.0 prerequisites, 1.1 data copy, 1.2 `make up`.
