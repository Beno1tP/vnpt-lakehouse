# Task 1 — Knowledge: Docker · MinIO · Spark · Iceberg (+ Hive Metastore)

Goal of Task 1: **2 Excel files → Spark → Iceberg tables stored in MinIO**, running in Docker (no K8s, no Airflow yet).

```
 ~/data/vnpt-lab/*.xlsx ──bind mount──▶ [spark container] ──S3 API (HTTP :9000)──▶ [minio]  (data + metadata files)
                                            │
                                            └──Thrift :9083──▶ [hive-metastore] ──JDBC :5432──▶ [postgres]
                                                               (table name → current metadata.json pointer)
```

---

## 1. Docker: what actually happens

| Concept | Mechanism | Task 1 consequence |
|---|---|---|
| Image | Read-only **layers** (overlay2), content-addressed. Each Dockerfile instruction = a layer; a change invalidates the layers after it | Put `pip install` and jar downloads **before** `COPY jobs/`, so code edits rebuild in seconds |
| Container | A process isolated by **namespaces** (what it can see: PID, network, mounts) and limited by **cgroups** (how much CPU/RAM). Shares the WSL2 kernel | `mem_limit` in Compose = a cgroup limit; going over it means the kernel OOM-kills the process (exit code 137) |
| Writable layer | **Copy-on-write**: modifying a file copies it up first; lost when the container is removed | Never write data inside containers; use **volumes** |
| Volume vs bind mount | Named volume = managed by Docker (inside the WSL2 VM disk). Bind mount = a host folder | MinIO/Postgres data → named volumes. Excel input → read-only **bind mount** (`:ro`) |
| Network | Compose creates one bridge network; each **service name is a DNS name** | Spark reaches `http://minio:9000`, `thrift://hive-metastore:9083`. `localhost` inside a container = that container itself |
| Ports | `ports: "9001:9001"` publishes to Windows | Only publish UIs you open in a browser (MinIO console 9001, Spark UI 4040) |
| Healthcheck + `depends_on: condition: service_healthy` | Compose waits until a dependency reports healthy, not just "started" | Prevents the Metastore starting before Postgres accepts connections |
| Restart policy | `restart: unless-stopped` restarts crashed services | Same idea as K8s self-healing, on one machine |

**Management:**
- Inspect: `docker compose ps`, `docker compose logs -f <svc>`, `docker stats` (live RAM/CPU).
- Get inside: `docker compose exec <svc> bash`.
- Clean up: `docker system df`, `docker image prune`.
- Recover: `docker compose down` keeps volumes; `down -v` **deletes data**.

## 2. MinIO: S3-compatible object storage

| Concept | Mechanism | Task 1 consequence |
|---|---|---|
| Object store, not a filesystem | Flat namespace `bucket/key`; "folders" are only key prefixes. No atomic rename | Why Iceberg exists: it commits by swapping a **pointer** (in the catalog), not by renaming directories |
| S3 API | HTTP REST (PUT/GET/LIST) on port 9000; console on 9001 | Spark/Iceberg talk S3 API; you browse in the console |
| Consistency | Strong read-after-write consistency | A file is readable right after a write succeeds |
| Path-style vs virtual-host | `http://minio:9000/bucket/key` vs `http://bucket.minio:9000/key` | **Must set path-style access = true**, or DNS lookups for `lakehouse.minio` fail |
| Credentials | Root user/password; better: a dedicated access key per service | Keep them in `.env` (git-ignored) |
| Single-node mode | One disk, no erasure coding | Fine for a lab; no redundancy, so the volume is your only copy |

## 3. Spark: how a job runs (local mode in Task 1)

| Concept | Mechanism | Task 1 consequence |
|---|---|---|
| Driver | Runs your Python `main`; builds a **lazy** plan; nothing runs until an action (`write`, `count`) | Errors in config often appear only at the first action |
| Executors / `local[N]` | In local mode the driver JVM also executes tasks on N threads | `--master local[2]` + `spark.driver.memory=2g` is enough for ~32k rows |
| Partitions → tasks | One task per data partition per stage; a **shuffle** (join/groupBy) starts a new stage | Task 1 has almost no shuffles; Silver joins will |
| PySpark ↔ JVM | Python talks to the JVM via Py4J; `createDataFrame(pandas_df)` serialises rows through Arrow (if enabled) | Enable `spark.sql.execution.arrow.pyspark.enabled=true` |
| Excel input | Spark has no native `.xlsx` reader | Read with **pandas + openpyxl** in the driver (`dtype=str`), then `createDataFrame` with an explicit schema. Fine at this size; at large scale, convert to Parquet/CSV upstream |
| Catalog plugin | `spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog` adds a named catalog (`lake`) | Tables are addressed as `lake.bronze.chatlog` |
| Spark UI | Port 4040 while the job runs | Check stages, tasks, rows written |

## 4. Iceberg: what is written and how a commit works

**Files on MinIO, per table:**
```
lakehouse/warehouse/bronze.db/chatlog/
  data/_ingest_date=2026-10-15/00000-...parquet    ← your rows (Parquet)
  metadata/v1.metadata.json, v2.metadata.json      ← schema, partition spec, snapshot list
  metadata/snap-<id>.avro                          ← manifest list (one per snapshot)
  metadata/<uuid>-m0.avro                          ← manifests: data files + column stats (min/max/null counts)
```

**Commit (optimistic concurrency):**
1. Write the new data files.
2. Write new manifests, a new manifest list and a new `metadata.json`.
3. Ask the **catalog** to swap the table pointer from the old `metadata.json` to the new one, *only if* it still points to the old one (compare-and-swap).
4. If another writer committed first, retry on top of the new state.

Readers always see a complete snapshot: either before or after, never half.

| Feature | What it gives you in Task 1 |
|---|---|
| **Snapshots / time travel** | `SELECT * FROM lake.bronze.chatlog.snapshots`; `VERSION AS OF <id>` |
| **Rollback** | `CALL lake.system.rollback_to_snapshot('bronze.chatlog', <id>)` after a bad load |
| **Hidden partitioning** | You declare `PARTITIONED BY (_ingest_date)` or `days(created_at)`; queries filter on the column, and Iceberg prunes files itself |
| **Schema evolution** | Add or rename columns without rewriting data (columns tracked by ID, not name) |
| **Format v2** | Row-level deletes (merge-on-read); needed later for `MERGE` |
| **Metadata tables** | `.snapshots`, `.files`, `.history`, `.partitions`: how you prove what was written |
| **Maintenance** | `expire_snapshots`, `rewrite_data_files` (compaction), `remove_orphan_files` |

**Idempotent load pattern for Bronze:** partition by `_ingest_date` (or `_batch_id`) and use `overwritePartitions()`. Rerunning the same batch **replaces** that partition instead of appending duplicates.

## 5. Hive Metastore: why it's in Task 1

- Iceberg needs a **catalog** to hold the "current metadata pointer" atomically (step 3 of the commit).
- Options: Hadoop (file-based), Hive Metastore, REST, JDBC, Glue.
  - The **Hadoop catalog is unsafe on object stores**: it relies on an atomic rename, which S3/MinIO doesn't have.
  - The lab later needs **Trino and StarRocks** to read the same tables through the **Hive Metastore**, so use it from day 1. Then Task 3–4 need no migration.
- HMS stores **names and pointers in Postgres**; data stays in MinIO.
- Gotcha: HMS itself may touch S3 (creating database directories), so it needs `hadoop-aws` jars + `fs.s3a.*` settings. See `05_errors_and_fixes.md` (E05).

## 6. Theory to be able to explain (leader questions)

| Question | Short answer |
|---|---|
| Why Iceberg instead of plain Parquet folders? | Atomic commits, snapshots/rollback, schema evolution and file-level stats on an object store that has no atomic rename |
| Why a catalog? | To make the pointer swap atomic, so writers don't corrupt each other and readers see consistent snapshots |
| Why MinIO? | S3-compatible locally: the same code later runs on real S3 |
| Why Docker for Task 1? | Pinned, reproducible versions of Spark + jars + Python; services wired by DNS; the same images move to K8s in Task 2 |
| What makes the load safe to rerun? | Partition overwrite on a deterministic partition key + Iceberg atomic commit |
| Where can it break? | Jar version mismatch, S3 endpoint/path-style, HMS ↔ S3 access, timestamp parsing, memory |
