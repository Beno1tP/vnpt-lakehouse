# Task 1 — Excel → Spark → Iceberg on MinIO (Docker Compose)

Load the two trial chatbot Excel files into **Bronze Iceberg tables** on MinIO, with a **Hive Metastore** catalog, using **digest-pinned official images** plus **two thin images** that only add dependencies (Iceberg/S3A/JDBC jars, pip packages). Configuration is mounted, credentials are **secret files**, volumes hold **data only**. Jobs are idempotent and the stack starts from zero with one command.

Status (2026-10-21): Bronze validated, idempotent, restart-safe, from-zero tested, failure drills done; **standard build** done (thin images, secrets as files; `08_standard_build.md`). Silver is the next stretch step.

## Architecture

```
Excel files (WSL folder, read-only bind mount)
   │  pandas reads every cell as text (Bronze = raw strings + load metadata)
   ▼
Spark 3.5.9 driver ──(Thrift :9083)──► Hive Metastore 3.1.3 ──(JDBC)──► Postgres 16
   │                                     stores only: table → current metadata.json
   │  Iceberg S3FileIO (path-style)
   ▼
MinIO  s3://lakehouse/warehouse/bronze.db/{chatlog,bot_predict}/
         metadata/*.metadata.json → snap-*.avro (manifest list) → *-m0.avro (manifest) → data/*.parquet
```

A commit = write data files to MinIO (invisible), then atomically swap the table's `metadata.json` pointer in HMS. That swap gives snapshots, time travel and safe retries.

| Service | Image | Role | Mem limit |
|---|---|---|---|
| `minio` | `cgr.dev/chainguard/minio@sha256:2136…` | S3-compatible object store | 512m |
| `postgres` | `postgres:16@sha256:1a6a…` (password via `POSTGRES_PASSWORD_FILE`) | HMS backend DB | 384m |
| `hive-metastore` | `vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1` = `apache/hive:3.1.3@sha256:d102…` + hadoop-aws, SDK, 1 JDBC driver; mounted `core-site.xml`; JDBC credentials in a `hive-site.xml` **secret**; TCP health check | Iceberg catalog (Thrift) | 1g (`-Xmx768m`) |
| `spark` | `vnpt-lake-spark:3.5.9-iceberg1.11.0-r1` = `apache/spark:3.5.9…@sha256:f3d6…` + Iceberg jars + pip deps; mounted `spark-defaults.conf`; starts when HMS is healthy | Runner; `make bronze` execs jobs here | 3g (driver 2g) |
| One-shots | `busybox:1.36@sha256:73aa…`, `amazon/aws-cli@sha256:733d…`, HMS image | `minio-perms` (volume owner), `minio-init` (bucket), `hms-schema-init` (init **only** if the schema is missing; any other error stops the stack) | — |

Every one-shot is idempotent and runs on each `make up` (same idea as Kubernetes initContainers).

## Run it

Prerequisites: Windows + WSL2 Ubuntu, Docker Desktop with WSL integration, `make`, internet on first start. The two Excel files in a WSL folder.

```bash
cp infra/compose/.env.example infra/compose/.env   # change the passwords; set RAW_DATA_DIR
make build                   # once per version change: builds the 2 thin images (~42 s)
make up                      # renders secrets, starts everything (~19 s from nothing: volumes, bucket, HMS schema)
make smoke                   # Spark -> Iceberg -> HMS -> Postgres answers SHOW NAMESPACES
make bronze DATE=2026-10-16  # load (or reload) the partition for that ingest date
make sql                     # interactive Spark SQL, catalog `lake`
make down                    # stop, keep data      |  make clean  # stop AND delete all volumes
```

Footprint: see [Resource footprint](#resource-footprint). Command reference: see [Commands by phase](#commands-by-phase).

## Versions and why

| Component | Version | Why |
|---|---|---|
| Spark | 3.5.9, Scala 2.12, Java 17, Python 3.10 (image) | Widest Iceberg / Spark Operator support; Scala must match the Iceberg jar |
| Iceberg | 1.11.0 jars baked into the Spark image (SHA-256 checked), format v2 | 1.12.0 was 2 weeks old and changes the AWS SDK HTTP client; v2 readable by Trino 483 / StarRocks 3.5 |
| Hive Metastore | 3.1.3 | Spark 3.5 uses the Hive 2.3 client; HMS 4 dropped `get_table` (E16) |
| hadoop-aws / AWS SDK | 3.1.0 / 1.11.271 | Must equal the image's hadoop-common 3.1.0; 3.1.3 needs Guava 27 (E17) |
| Postgres JDBC | 42.7.4 | Postgres 16 uses SCRAM auth; the image's bundled 9.4 driver cannot (E21) |
| Python deps | pandas 2.2.3, openpyxl 3.1.5, pyarrow 17.0.0, numpy 2.2.6 | Pinned in `docker/spark/requirements.txt`, installed into the Spark image |

## Design decisions and alternatives

| Decision | Chosen | Alternatives | Why chosen | What production usually does |
|---|---|---|---|---|
| Runtime | Docker Compose, Spark `local[2]` in one container | Spark on K8s now; standalone Spark cluster | Isolates Spark/Iceberg/S3 problems from K8s problems; Task 2 moves the same job to K8s | Spark on K8s or YARN, orchestrated by Airflow (Task 2) |
| Table format | Apache Iceberg v2 | Delta Lake, Apache Hudi, plain Parquet folders | Open spec, engine-neutral (Spark, Trino, StarRocks), snapshots and time travel, hidden partitioning; the lab names it | Iceberg or Delta (Delta when Databricks-centric) |
| Catalog | Hive Metastore 3.1.3 + Postgres | Iceberg REST catalog (Polaris, Lakekeeper, Gravitino), JDBC catalog, Nessie, **Hadoop catalog** | The lab names HMS; Trino and StarRocks read it (Tasks 3–4). **Hadoop catalog is unsafe on S3**: it commits by renaming the next `vN.metadata.json`; that is safe only where rename is atomic and fails if the target exists (HDFS). S3 has no atomic rename, so without an external lock (e.g. DynamoDB lock manager) two writers can both *believe* they committed and one commit is silently lost (lost update). HMS 3.1.3 because Spark 3.5's Hive 2.3 client fails on HMS 4 (E16) | Trend: REST catalog (no Thrift/Hive-client version coupling); HMS still common on Hadoop-heritage platforms |
| Object storage | MinIO (Chainguard image, pinned digest) | Ceph RGW, SeaweedFS, Garage; cloud S3 | S3 API locally, same code works on real S3; official MinIO images were withdrawn (E01) | Cloud object storage (S3/GCS/ADLS) or Ceph on-prem |
| Excel ingestion | pandas + openpyxl in the driver, all strings → Spark | `spark-excel` reader; convert Excel → CSV/Parquet first; JDBC from the source DB | ~32k rows fit easily in the driver; no extra JVM dependency; strings avoid parse errors in Bronze | JDBC/CDC from the source database (the timestamp format suggests an Oracle export) |
| Bronze schema | All source columns as text + 4 load-metadata columns | Typed Bronze | A raw copy cannot fail on parsing; typing happens in Silver, where errors are visible and fixable | Same: raw/landing layer kept close to source |
| Partitioning | `_ingest_date` (identity), `overwritePartitions()` | Event-date partition; append only | Bronze tracks *loads*: idempotent rerun and rollback per batch. Event-time partitioning belongs in Silver after timestamps are parsed | Same pattern: ingest date in raw, event date in curated |
| Packaging | **Thin images** `FROM` digest-pinned official images, adding only dependencies (jars with SHA-256 checks, pinned pip); config mounted; credentials as secret files; volumes for data only | (lab v1) runtime downloads into volumes (`jars-init`, `spark-init`, Ivy); community image `tabulario/spark-iceberg`; fat custom images with config baked in | Immutable, versioned, no runtime downloads; Task 2 pulls the same images; build cache pruned after builds | Same, built by CI and pushed to a registry (GHCR in Task 2), referenced by digest |

## Resource footprint

| Resource | Value | How measured |
|---|---|---|
| Docker images | 5.69 GB (official bases + 2 thin images; thin layers ≈ 0.4 GB Spark, 0.09 GB HMS) | `docker system df`, `docker history` |
| Volumes (MinIO data, Postgres only) | 63 MB | `docker system df` |
| Bronze on MinIO | 10.4 MB, 54 objects (data 7.0 MB chatlog + 3.1 MB bot_predict, incl. files kept for older snapshots; source Excel 3.4 MB) | `aws s3 ls --recursive --summarize s3://lakehouse/warehouse/bronze.db/` via `minio-init` |
| RAM idle | hive-metastore 467 MiB, postgres 91 MiB, minio 77 MiB, spark 1 MiB | `docker stats --no-stream` |
| RAM at peak (during `make bronze`) | spark **1.41 GiB** (sampled every ~2 s; limit 3 GiB, driver heap 2g) | `docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}"` in a 2nd terminal while the job runs |
| RAM limits (upper bound) | 512m + 384m + 1g + 3g ≈ 4.9 GB of the 10 GiB WSL gets | `mem_limit` in compose |
| Downloads | at `make build` only (~200 MB: jars + pip wheels); `make up` downloads nothing | `time make build` = 42.3 s |
| Windows disk (C:) | Docker/WSL virtual disks grow and do not shrink by themselves → `wsl --shutdown` + `diskpart compact vdisk` | size of the `.vhdx` files |

Start of the session (incl. images from earlier experiments): images 11.73 GB + build cache 6.19 GB. Lab v1: 4.98 GB + 0.54 GB volumes. Standard build: 5.69 GB + 0.06 GB volumes.

## Commands by phase

`$DC` = `docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env`

| Phase | Command | Expect |
|---|---|---|
| Configure | `cp infra/compose/.env.example infra/compose/.env`, edit passwords and `RAW_DATA_DIR` | `git check-ignore -v infra/compose/.env` shows it ignored |
| Validate config | `$DC config --quiet && echo ok` | `ok` (checks schema only, not intent: E18) |
| Build | `make build` | 2 images `vnpt-lake-spark:…-r1`, `vnpt-lake-hms:…-r1`; then `docker builder prune -f` |
| Start | `make up` | secrets rendered; one-shots `Exited (0)`; hive-metastore `(healthy)`; minio, postgres, spark `Up` |
| Smoke | `make smoke` | `bronze`, `default` |
| State | `make ps` · `make stats` | as above |
| Logs | `make logs SVC=hive-metastore` · `$DC exec hive-metastore sh -c 'grep -n -i -E "exception\|caused by" /tmp/hive/hive.log'` | read the **first** error; since the standard build the DB password is no longer in HMS logs |
| Load | `make bronze DATE=2026-10-16` | `[bronze] … 21224 …` and `… 10729 …` |
| Validate | `make sql`, queries in `06_validation.md` | numbers equal the results table |
| Snapshots | `SELECT committed_at, operation, summary['total-records'] FROM lake.bronze.chatlog.snapshots;` | `append`, then `overwrite` per rerun |
| Time travel | `SELECT count(*) FROM lake.bronze.chatlog VERSION AS OF <snapshot_id>;` | the earlier batch |
| Remove a batch | `DELETE FROM lake.bronze.chatlog WHERE _ingest_date = DATE 'YYYY-MM-DD';` | new `delete` snapshot (metadata-only) |
| Browse files | MinIO console http://localhost:9001 → `lakehouse/warehouse/bronze.db/` | `data/`, `metadata/` |
| Stop / reset | `make down` (keep data) · `make clean` (delete both data volumes) | — |
| After a reboot | `make up` | HMS may show `Exited (127)` until then (E23) |
| From zero | `make clean && make up && make bronze DATE=…` | same counts, 1 `append` snapshot |
| Background run | `$DC exec -T … < /dev/null > log 2>&1 &` | without `-T` and `< /dev/null` bash suspends it (E22) |

## The job

`jobs/bronze_ingest.py --ingest-date YYYY-MM-DD` reads both sheets as strings, adds `_ingest_date`, `_ingested_at`, `_source_file`, `_batch_id`, and writes `lake.bronze.chatlog` and `lake.bronze.bot_predict` (Iceberg v2, partitioned by `_ingest_date`). First run `create()`, later runs `overwritePartitions()`: rerunning a date **replaces** that partition, other dates are untouched.

## Results (details: `06_validation.md`)

| Check | Result |
|---|---|
| chatlog rows / distinct ID / senders | 21,224 / 21,224 / 89 ✅ |
| bot_predict rows / distinct ID / senders | 10,729 / 10,729 / 89 ✅ |
| SOURCE client / bot · `/home` (client) · channels | 10,613 / 10,611 · 10,378 · livechat 21,192 / platform 32 ✅ |
| predict rows without a chat row | 116 ✅ |
| Rerun same date | snapshot `overwrite`, deleted = added = 21,224, total unchanged ✅ |
| New date | new partition, total 42,448, old partition untouched ✅ |
| Time travel | `VERSION AS OF` and `TIMESTAMP AS OF` return the earlier batch ✅ |
| Files vs table | `data/_ingest_date=2026-10-16/` holds 7 Parquet files (1.17 MB each) = 7 loads of the same 21,224 rows; `chatlog.files` = 1 current, `all_data_files` = 7. Old files stay for time travel until `expire_snapshots` |
| From zero (`make clean && make up && make bronze`) | counts match, 1 `append` snapshot ✅ |

## Failure drills

| Drill | Result |
|---|---|
| HMS stopped during a load | Fails fast (`UnknownHostException: hive-metastore`), exit 2, no snapshot |
| Spark killed between the two table commits | Exit 137; each table complete but on different batches (no cross-table transaction); no orphan files |
| Recovery | One rerun → both tables on one batch |

Takeaway: commits are **atomic per table**; a failed job exits non-zero so an orchestrator retries; the idempotent rerun repairs everything.

## Errors met and lessons (details: `05_errors_and_fixes.md`)

| ID | Problem → fix | Lesson |
|---|---|---|
| E01 | `minio/minio`, `minio/mc` no longer published → Chainguard MinIO + aws-cli, pinned by digest | Images can vanish; pin digests |
| E07 / E19 | Schema init always (crash loop) or never (empty DB breaks) → `hms-schema-init` checks `schematool -info` first | Init = check-then-create |
| E16 | HMS 4 rejects `get_table` from Spark's Hive 2.3 client → HMS 3.1.3 | Client/server RPC versions must match |
| E17 | `NoSuchMethodError` (Guava) inside S3A → hadoop-aws 3.1.0 | Jars must match incl. transitive deps; read the **server** log |
| E18 | Long paste into nano dropped characters; `compose config` still OK → heredocs/patches, review diffs | Schema validation ≠ intent |
| E20 / E21 | Missing bind-mount sources become root-owned dirs; containers are created before `jars-init` runs → old JDBC driver, SCRAM error → `.gitkeep` + `make up` fetches jars first (both obsolete after the standard build: jars live in the image, one JDBC driver) | `depends_on` orders start, not create |
| E22 | Background `docker exec` suspended (SIGTTIN); exit 141 (SIGPIPE) → `-T`, `< /dev/null`, `kill -9` | Job control and signals |
| E23 | After a reboot Docker Desktop restarts HMS before WSL bind mounts exist → `Exited (127)` → `make up` | Bind mounts depend on the host path at create time |

## Known limits / next

- **Silver** (stretch): parse `CREATED_TIME` (Oracle text, 9-digit fraction), dedupe, `final_intent`, flags (internal senders, `/home`, 116 unmatched predict rows); check matching `_batch_id` before joining.
- **Leader questions**: UC6 "top 12 intents" but `final_intent` has 3 values (xin_chao 10,588 · fallback 107 · bot_khong_hieu 34; `UPDATED_INTENT` empty); UC7 "30-day comparison" but data covers 18 days (97% on 07-24/25).
- Secrets: HMS/Postgres credentials are secret files since the standard build (0 hits in inspect / ps / logs). Still env vars: MinIO root password and S3 keys (phase 2 in `08_standard_build.md` §5; K8s Secrets in Task 2).
- No `expire_snapshots` / `remove_orphan_files` yet (planned maintenance job).
- Data handling: trial data, stays on the laptop; jobs and checks print counts only.

## Files

| Path | What |
|---|---|
| `infra/compose/docker-compose.yml` | All services |
| `infra/compose/.env.example` | Versions, image pins, credentials template |
| `docker/spark/`, `docker/hive/` | Thin image Dockerfiles (digest-pinned bases, SHA-256-checked jars) + `requirements.txt` |
| `infra/compose/render-secrets.py` | Renders `hive-site.xml` + `pg_password` from `.env` into git-ignored `infra/compose/secrets/` (mode 600) |
| `infra/compose/hive/core-site.xml` | S3A → MinIO for HMS |
| `infra/compose/spark/spark-defaults.conf` | Catalog `lake`, S3 endpoint, memory |
| `jobs/bronze_ingest.py` | Bronze job |
| `docs/task1/01–06` | Knowledge, config, plan, process log, errors, validation |
| `docs/task1/07_learning_guide.md` | What I learned per tool, standard patterns, reading list, likely questions |
| `docs/task1/08_standard_build.md` | Standard build: decisions, steps, rollback, results |
| `docs/task1/09_command_paths.md` | Every command step with its error branches |
