---
# Slide 1 — Task 1: Excel → Spark → Iceberg on MinIO
VNPT AI · Data Engineering onboarding lab · Task 1 of 5
Bronze layer for chatbot logs, catalogued in Hive Metastore
Bee (Pham Quang Minh) · 2026-10-22

Visual: dark title slide; small orange line "VNPT AI · DE onboarding lab · Task 1".

Speaker notes: The lab builds an end-to-end lakehouse on trial chatbot logs in 5 standalone tasks. Task 1 builds the foundation every later task depends on: storage, table format, catalog and a reliable Bronze load. I will explain why each tool is there, how it works, how Task 1 uses it, and what I proved.

---
# Slide 2 — Task 1 in one sentence, and the agenda
Load two Excel exports into versioned, rerunnable Iceberg tables on S3-compatible storage, so any engine can query them later.

1. Why: the problem and the file formats
2. What: Docker, MinIO, Spark, Iceberg, Hive Metastore + Postgres
3. How: the pipeline, commits, idempotency, time travel, data quality
4. Proof: results, failure drills, three build stages
5. Next: open questions and Task 2

Speaker notes: The balance is why → what → how → proof. Every number in this deck comes from command output; the full log is in docs/task1 on GitHub.

---
# Slide 3 — The problem
- Source: two Excel exports of a chatbot (chat log 21,224 rows; NLU predictions 10,729 rows)
- Needed: tables that are correct, rerunnable, versioned, and readable by several engines (Spark now; Trino, StarRocks, Superset in Tasks 3–5)
- Requirements:
  - Correct: numbers match an independent reference
  - Rerunnable: a retry or backfill never duplicates data
  - Versioned: history, rollback, audit
  - Open: no engine owns the data

Visual: left "Excel files on a laptop"; right "shared tables for Spark, Trino, StarRocks, Superset"; an arrow labelled "Task 1".

Speaker notes: Excel is for people, not for pipelines. The rest of the talk is how each requirement is met and how I proved it. The data is trial data for freshers and stays on the laptop; every check prints counts only.

---
# Slide 4 — Why not keep Excel? xlsx vs CSV vs Parquet

| | xlsx | CSV | Parquet |
|---|---|---|---|
| What it is | ZIP of XML parts (Office Open XML) | Plain text, one line per row | Binary, columnar |
| Schema / types | Per cell, inferred | None (everything is text) | Stored in the file footer |
| Layout | Row/cell oriented | Row oriented | Column oriented |
| Compression | ZIP of the whole workbook | None by default | Per column (dictionary, run-length, Snappy/zstd) |
| Read one column | Parse the whole workbook | Scan every line | Read only that column's chunks |
| Skip data | No | No | Yes, via min/max statistics |
| Split across workers | No | Yes, if uncompressed | Yes, by row group |
| Made for | People | Simple exchange | Analytics engines |

Our data: chat log xlsx 2.3 MB → Parquet 1.17 MB for the same 21,224 rows.

Speaker notes: xlsx is a zipped set of XML documents designed for spreadsheet apps; a program must unzip and parse the whole workbook. CSV has no types and ambiguous quoting. Parquet stores data by column with types and statistics, so engines read less. In the job we read Excel once, with pandas, and from then on everything is Parquet.

---
# Slide 5 — Inside a Parquet file
- File = row groups → column chunks → pages, plus a footer with metadata
- Row group: a horizontal slice of rows (the unit of parallel work)
- Column chunk: all values of one column inside one row group
- Page: the smallest unit read and decoded (about 1 MB by default), encoded + compressed
- Footer: schema, row-group locations, per-column statistics (min, max, null count)
- A reader opens the footer first, then reads only the needed columns and skips row groups whose min/max cannot match the filter

Visual: a tall rectangle "PAR1 | row group 1 [column chunk A: pages | column chunk B: pages] | row group 2 [...] | footer (schema, offsets, stats) | PAR1"; arrows from "SELECT B WHERE A > 10" to column chunk B of row groups whose stats allow it.

Speaker notes: Two optimisations come straight from this layout. Column pruning: only the columns in the query are read. Predicate pushdown: statistics let the reader skip whole row groups. Iceberg adds a second level of statistics per data file in its manifests, so planning can skip whole files before opening them.

---
# Slide 6 — Docker: packaging and running each service
- What: runs programs in containers built from images
- How it works:
  - Image = read-only layers + metadata, identified by a digest (sha256)
  - Container = a process with its own filesystem view (image layers + a writable layer)
  - Isolation by Linux namespaces (processes, network, mounts); limits by cgroups (our memory limits)
  - Volumes keep data beyond a container; networks give DNS by service name
- Architecture: docker CLI → Docker Engine (dockerd) → containerd → runc → container; images pulled from a registry
- Compose: one YAML file declares all services, volumes, networks, start order and health checks
- Task 1: 7 services, thin images, config mounted, credentials as secret files, data-only volumes

Visual: stack diagram "CLI → dockerd → containerd → runc → containers", a registry box feeding images, and a container drawn as "image layers + writable layer + volume".

Speaker notes: A container is not a VM: it shares the host kernel and is isolated by namespaces and limited by cgroups. On Windows, Docker Desktop runs the engine in its own WSL2 VM. Pinning images by digest makes them immutable. Our rule: dependencies in images, configuration outside, credentials in files, data in volumes.

---
# Slide 7 — MinIO and the S3 API: where the data lives
- What: object storage that implements the Amazon S3 API
- How it works:
  - Bucket → objects (key + bytes + metadata), flat namespace
  - "Folders" are only key prefixes; there is no atomic rename (rename = copy + delete)
  - HTTP REST API: PUT, GET, LIST, DELETE, with signed requests
- What for: cheap, scalable storage for data files and table metadata; the same code works on AWS S3
- Architecture: single node, single drive in this lab; production MinIO runs distributed with erasure coding (data + parity spread over drives, survives drive loss)
- Task 1: bucket lakehouse; API port 9000, console port 9001; path-style addressing (http://minio:9000/lakehouse/key)

Visual: bucket icon with keys "warehouse/bronze.db/chatlog/metadata/…json", "…/data/…parquet"; a client sending "PUT/GET over HTTP".

Speaker notes: Every engine talks to MinIO with its normal S3 client; only the endpoint, path-style flag, region and keys differ from AWS. No atomic rename is the reason a table format needs a catalog for commits. MinIO stopped publishing community images in 2025, so we use a Chainguard rebuild pinned by digest.

---
# Slide 8 — Spark: the compute engine (1/2, architecture)
- What: a distributed engine for batch (and streaming) data processing
- Architecture:
  - Driver program: runs main(), owns the SparkSession, turns DataFrame code into a plan of jobs → stages → tasks, schedules tasks
  - Cluster manager: hands out resources (standalone, YARN, Kubernetes)
  - Executors on worker nodes: JVM processes that run tasks, keep cached data, read/write shuffle data, report back to the driver
- Execution: transformations are lazy; an action (count, write) triggers a job; shuffles split a job into stages; each stage runs one task per partition

Visual: classic diagram "Driver (SparkSession, DAG scheduler) ↔ Cluster manager ↔ Worker nodes [Executor: task, task, cache]".

Speaker notes: The driver is the brain: it builds the logical plan, the Catalyst optimizer rewrites it, and the physical plan is split into stages at shuffle boundaries. The cluster manager only allocates resources. Executors do the actual work and talk back to the driver.

---
# Slide 9 — Spark: deploy modes, and what Task 1 uses (2/2)

| Mode | Where the driver runs | Executors | Used for |
|---|---|---|---|
| Local (local[N]) | One JVM on one machine | N threads in the same JVM | Development, tests, small data: Task 1 |
| Client | On the machine that submits (e.g. a laptop or gateway) | On the cluster | Interactive work, notebooks |
| Cluster | Inside the cluster (e.g. a driver pod) | On the cluster | Production jobs: Task 2 on Kubernetes |

- Task 1: spark-submit --master local[2], driver heap 2g inside a 3 GiB container; peak 1.41 GiB
- Catalogs plug in through configuration: catalog lake = Iceberg SparkCatalog → Hive Metastore at thrift://hive-metastore:9083, S3FileIO → MinIO

Speaker notes: Same code, different master. In Task 2 the Spark Operator creates a driver pod in cluster mode, which asks Kubernetes for executor pods. Honest point: 32k rows don't need Spark (DuckDB would be enough); the lab teaches the path that scales.

---
# Slide 10 — Iceberg: the table format
- What: an open specification (plus libraries) that turns files in object storage into tables
- How it works (metadata tree):
  - Catalog stores one pointer per table → current metadata.json
  - metadata.json: schema, partition spec, list of snapshots
  - Snapshot → manifest list (Avro) → manifests (Avro: data files + column statistics) → data files (Parquet)
- What for: ACID commits, snapshots and time travel, schema evolution by column ID, hidden partitioning, many engines on one table
- Architecture: a library inside each engine (in Spark: iceberg-spark-runtime) + a catalog + files; no Iceberg server
- Task 1: format v2; tables lake.bronze.chatlog and lake.bronze.bot_predict, partitioned by _ingest_date

Visual: tree "Catalog pointer → v7.metadata.json → snap list → manifest → Parquet files", with old metadata files greyed out on the side.

Speaker notes: A commit never edits a file: it writes new files and moves the pointer. Our chatlog table is at metadata file 00007 after 8 commits; HMS also keeps previous_metadata_location = 00006.

---
# Slide 11 — Hive Metastore + Postgres: the catalog
- What: Hive Metastore (HMS) is a catalog service; Postgres is the database where HMS stores its catalog
- How it works:
  - HMS serves a Thrift API on port 9083 (get_table, create_database, lock, alter_table)
  - HMS persists its model through DataNucleus (an object-relational mapper) over JDBC to Postgres
  - "Schema 3.1.0" = HMS's own relational schema: 74 tables (DBS, TBLS, TABLE_PARAMS…), not our table schema
- What for: one shared directory of tables for Spark, Trino and StarRocks
- For Iceberg tables HMS keeps only: table_type = ICEBERG, metadata_location (and the previous one); the real schema and snapshots live in metadata.json
- Version constraint: Spark 3.5 ships the Hive 2.3 client, which HMS 4 no longer serves (get_table) → HMS 3.1.3 (E16)

Visual: "Spark / Trino / StarRocks → Thrift :9083 → HMS → JDBC :5432 → Postgres (74 tables)"; a callout "metadata_location = s3a://…/00007-….metadata.json".

Speaker notes: No table data ever passes through HMS, only small Thrift calls. Postgres uses SCRAM-SHA-256 password authentication, which is why the old JDBC driver bundled in the Hive image failed and we ship exactly one modern driver (E21).

---
# Slide 12 — How the pieces talk

| From → To | Protocol / format | Port |
|---|---|---|
| Spark (Iceberg library) → HMS | Thrift RPC | 9083 |
| HMS → Postgres | JDBC (DataNucleus) | 5432 |
| Spark (Iceberg S3FileIO) → MinIO | S3 REST over HTTP (AWS SDK v2) | 9000 |
| HMS → MinIO (create database folder) | S3 via Hadoop S3A (AWS SDK v1) | 9000 |
| Iceberg reading its own files | metadata.json (JSON) → manifests (Avro) → data (Parquet) | — |
| Name resolution | Docker network DNS (minio, postgres, hive-metastore) | — |

- Read: HMS gives the pointer → Spark reads metadata, manifests and only the needed Parquet from MinIO
- Write: Spark writes new files to MinIO → asks HMS to swap the pointer (the commit)

Visual: four boxes Spark, HMS, Postgres, MinIO with labelled arrows (Thrift, JDBC, S3/HTTP).

Speaker notes: Two different S3 client stacks exist: Iceberg's S3FileIO in Spark and Hadoop's S3A inside HMS. Our hardest bug (E17) was in the S3A path inside HMS, not in Spark.

---
# Slide 13 — Roles in Task 1

| Role | Tool | Why it is needed |
|---|---|---|
| Runtime / packaging | Docker Compose, thin images | Same environment everywhere, one command to start |
| Object storage | MinIO (S3 API) | Where all bytes live |
| File format | Parquet | Columnar, typed, compressed, skippable |
| Table format | Iceberg v2 | Commits, snapshots, time travel, many engines |
| Catalog | Hive Metastore 3.1.3 + Postgres 16 | Atomic pointer swap, shared table directory |
| Compute | Spark 3.5.9 | Reads the Excel files, writes the tables |
| Operations | make targets, versioned checks | One command to load, validate, accept |

Visual: layered stack bottom-up: MinIO → Parquet → Iceberg → HMS/Postgres → Spark → operator (make); Docker as a frame around all.

Speaker notes: Each layer has one job. That separation is what lets Trino and StarRocks plug in later without moving data.

---
# Slide 14 — Why these, and not others

| Role | Ours | Main alternatives | Why ours here |
|---|---|---|---|
| Storage | MinIO | AWS S3, GCS, ADLS, Ceph | S3 API on a laptop; same code on S3 |
| Table format | Iceberg | Delta Lake, Hudi, Paimon | Engine-neutral for Spark, Trino, StarRocks |
| Catalog | HMS | Iceberg REST (Polaris, Lakekeeper, Gravitino), Glue, Nessie, JDBC | Named by the lab; Trino/StarRocks support it maturely |
| Compute | Spark | Trino, Flink, DuckDB, PyIceberg | Scale-out path for Task 2 |
| Packaging | Thin images | Runtime downloads, community images | Immutable, no runtime internet |

- Honest view: DuckDB would be enough for 32k rows; the lab teaches the stack that scales
- Next idea: an Iceberg REST catalog would remove the Hive-client version chain behind E16/E17

Speaker notes: Alternatives win in other settings: Delta on Databricks, Flink for streaming, managed cloud catalogs on AWS. Managed cloud is not an option here because the data must stay on the laptop.

---
# Slide 15 — The pipeline end to end
- Spark reads both Excel files from a read-only folder (pandas, every cell as text)
- Adds 4 load columns: _ingest_date, _ingested_at, _source_file, _batch_id
- Writes lake.bronze.chatlog and lake.bronze.bot_predict, partitioned by _ingest_date
- First load create(), later loads overwritePartitions()
- Medallion layers: Bronze = raw copy + load metadata (now); Silver = typed, deduplicated, flagged (next); Gold = business marts with dbt (Task 4)
- Why Bronze is all text: the raw copy never fails on a parse error; typing is a Silver rule where errors are counted

Visual: Excel → Spark → Bronze (MinIO + HMS) → Silver → Gold, with Bronze highlighted.

Speaker notes: Partitioning by load date makes every load a replaceable unit: rerun one date, or delete one date, without touching others. Event-time partitioning belongs in Silver, after timestamps are parsed.

---
# Slide 16 — Commits and concurrency
- Optimistic concurrency control: writers don't lock while writing data files
- Commit = compare-and-swap of the pointer: "move to v2 only if it is still v1"
- With HMS: lock → check metadata_location → alter_table → unlock (milliseconds)
- Conflict → refresh, re-check, retry (commit.retry.num-retries)
- Crash before the swap → nothing visible; written files are orphans until cleanup
- Why not the Hadoop catalog on S3: it commits by renaming the next vN.metadata.json, safe only where rename is atomic and fails if the target exists (HDFS). S3 has no atomic rename, so without an external lock two writers can both believe they committed and one commit is silently lost (lost update)

Visual: sequence "Spark → MinIO: data files · Spark → MinIO: manifests + new metadata.json · Spark → HMS: swap v1→v2 · HMS → Spark: committed".

Speaker notes: The catalog's job is exactly this atomic swap. Iceberg's docs state the Hadoop catalog is not safe for concurrent writes on S3 unless a lock manager such as DynamoDB is configured.

---
# Slide 17 — Idempotency: a rerun replaces, never duplicates

| # | Action | Operation | Added | Deleted | Total |
|---|---|---|---|---|---|
| 1 | First load (10-16) | append | 21,224 | – | 21,224 |
| 2 | Rerun the same date | overwrite | 21,224 | 21,224 | 21,224 |
| 3 | Load a new date (10-17) | overwrite | 21,224 | – | 42,448 |
| 4 | Remove the test partition | delete | – | 21,224 | 21,224 |

- overwritePartitions() = dynamic partition overwrite: replaces only partitions present in the new data, as one atomic snapshot
- Why it matters: Airflow (Task 2) retries and backfills; a job that runs twice must leave the table as if it ran once

Speaker notes: Row 2 is the proof: deleted equals added, total unchanged. Row 3 shows other partitions are untouched. Row 4: deleting a whole partition is metadata-only, no file is rewritten.

---
# Slide 18 — Snapshots, time travel, and their cost
- Every commit writes a new metadata.json and moves the pointer; data files are never moved or edited
- Time travel reads an older snapshot's file list: VERSION AS OF snapshot_id, TIMESTAMP AS OF 'a past time' → earlier batch, 21,224 rows
- Evidence: one Parquet file per load on MinIO, but chatlog.files = 1 current
- Costs to manage:
  - Storage: old files stay until expire_snapshots
  - Metadata: metadata.json grows about 1.2 KB per commit (2,860 → 10,164 bytes over 7 commits) and old versions are kept
  - Many small commits → many small files → slower planning
- Standard maintenance: expire_snapshots (retention), rewrite_data_files (compaction), rewrite_manifests, remove_orphan_files

Visual: timeline of snapshots 1…8 with the pointer on the newest; files below, the current one highlighted.

Speaker notes: Retention is a business decision (for example keep 7 days of history). Until then, time travel is a feature that costs storage.

---
# Slide 19 — Data quality: from manual to automated
- Before: expected numbers computed independently with pandas; results compared by eye (manual reconciliation)
- Now: make validate DATE=… → 17 checks from a versioned SQL file against versioned expected values → PASS/FAIL per check, exit code ≠ 0 on any FAIL
- Two levels of assertion:
  - In the job: refuse success if rows written ≠ rows read
  - In the validator: business numbers + "both tables from the same load" + idempotency (last snapshot overwrite, added = deleted)
- make acceptance: smoke → load → validate (17/17) → reload → validate + idempotency (23/23) → PASSED, exit 0
- Negative test (empty date): 13 FAIL, exit 2 → the gate really stops a run
- Standard pattern next: Write-Audit-Publish with Iceberg branches (write → check → publish only if checks pass)

Speaker notes: Failing loudly drives everything: an orchestrator retries transient failures, alerts on persistent ones, and the PASS/FAIL lines tell you where to look. Four checks passed even on the empty partition (vacuous checks), which is why every table keeps at least one row-count check.

---
# Slide 20 — When things fail (drills)
- Drill A — catalog down: stop HMS, run the load → "Failed to connect to Hive Metastore", exit 2, no new snapshot
- Drill B — crash between the two table commits: kill Spark the moment chatlog is committed → exit 137; chatlog on the new batch, bot_predict on the old one; each table complete, no orphan files
- Recovery: one rerun → both tables on the same batch (now checked automatically by both.distinct_batch_ids)
- Lesson: commits are atomic per table, not across tables; a failed job must exit non-zero so the orchestrator retries; idempotent reruns repair the state

Visual: two timelines: A (HMS down → job stops before writing) and B (chatlog committed ✓, kill ✗, bot_predict not committed → rerun → both ✓).

Speaker notes: Drill B needed a log-triggered kill because the window between the two commits is about 2.7 seconds.

---
# Slide 21 — Three stages: lab → standard → operable

| | v1 lab | v2 standard | v3 operable |
|---|---|---|---|
| Dependencies | Downloaded at runtime into volumes | Baked into 2 thin images (checksummed jars, pinned pip) | same |
| Base images | Tags | Digests | same |
| HMS/Postgres credentials | In JVM flags, logs, inspect | Secret files: 0 hits in inspect/ps/logs | same |
| JDBC drivers in HMS | 2 | 1 | same |
| Volumes | 4 (549 MB, incl. 483 MB dependencies) | 2 (63 MB, data only) | same |
| Start from zero | 49.5 s | 19.1 s | same |
| Proof | Manual SQL | Manual SQL | make acceptance: 17 + 23 PASS, exit 0 |
| Git tag | task1-v1-lab | task1-v2-standard | task1-v3-operable |

- The data survived both rebuilds: same tables, all snapshots, time travel intact

Speaker notes: v1 worked but was a lab shortcut. v2 applies the standard rule (dependencies in images, config outside, credentials in files, data in volumes). v3 makes it operable: one command proves everything with exit codes.

---
# Slide 22 — Open questions and next steps
- UC6 "top 12 intents": final_intent has 3 values in this data (xin_chao 10,588 · fallback 107 · bot_khong_hieu 34); UPDATED_INTENT is empty → proposal: generic top-N without inventing labels; is a bigger sample or an intent catalogue available?
- UC7 "30-day comparison": 18 days of data (2024-07-02 → 07-25), 97% on 07-24/25 → proposal: window anchored to the latest event date, window length as a parameter, empty previous window → NULL
- Next: Silver (parse CREATED_TIME, dedupe, final_intent, flags, batch check)
- Then Task 2: CI builds images → GHCR; Kubernetes + Spark Operator (cluster mode); Airflow DAG load → validate; secrets as Kubernetes Secrets
- Later: Iceberg maintenance job; REST catalog proposal

Speaker notes: Both proposals keep dashboards reconcilable with the source. I need the leader's decision on UC6/UC7 before Gold models.

---
# Appendix A1 — Errors and fixes (grouped)

| Theme | Errors | Lesson |
|---|---|---|
| Images | E01 MinIO images withdrawn | Pin digests, know a fallback |
| Versions | E16 HMS 4 vs Hive 2.3 client; E17 Guava NoSuchMethodError in S3A | Match RPC and jar versions incl. transitive dependencies; read the server log |
| Init and order | E07/E19 schema init always/never; E20/E21 bind mounts created as root dirs, old JDBC driver | Check-then-create; depends_on orders start, not create |
| Tooling | E18 paste corruption; E22 background exec suspended, SIGPIPE; E23 bind mounts not ready after reboot | Validate intent not only syntax; non-interactive runs need -T and no stdin; make up after a reboot |

Speaker notes: Full symptom → root cause → fix → theory for each error is in 05_errors_and_fixes.md; every command path with its error branches is in 09_command_paths.md.

---
# Appendix A2 — Debugging story: E17

| Step | Evidence |
|---|---|
| Client symptom | CREATE NAMESPACE → "MetaStoreClient lost connection" |
| Server log (/tmp/hive/hive.log) | NoSuchMethodError: Preconditions.checkArgument(boolean, String, Object, Object) |
| Stack | create_database → Warehouse.determineDatabasePath → S3AFileSystem.initialize |
| Versions in the container | hadoop-aws 3.1.3 vs hadoop-common 3.1.0; Guava 11/19 |
| Build files (poms) | hadoop 3.1.3 → Guava 27; hadoop 3.1.0 → Guava 11 |
| Fix + proof | hadoop-aws 3.1.0 → smoke test passes, files on MinIO |

Speaker notes: The method: the client shows symptoms, the server shows causes; then the stack, the versions actually on disk, and the dependency's own build file. Java links methods by exact signature at runtime, so the error appears only when that code path runs.

---
# Appendix A3 — Versions and the compatibility chain

| Component | Version | Reason |
|---|---|---|
| Spark | 3.5.9, Scala 2.12, Java 17, Python 3.10 | Scala must match the Iceberg runtime jar |
| Iceberg | 1.11.0, format v2 | 1.12.0 too new; changed the AWS SDK HTTP client |
| Hive Metastore | 3.1.3 (Java 8) | Spark's Hive 2.3 client fails on HMS 4 |
| hadoop-aws / AWS SDK | 3.1.0 / 1.11.271 | Must equal the image's hadoop-common 3.1.0 |
| Postgres / JDBC | 16 / 42.7.4 | SCRAM authentication needs a modern driver |
| MinIO | Chainguard build, by digest | Official images withdrawn |

Chain: Spark → Hive 2.3 client → HMS 3.1.3 → hadoop-common 3.1.0 → hadoop-aws 3.1.0 → Guava / AWS SDK.

Speaker notes: Change one version at a time; read the dependency's own build file instead of guessing.

---
# Appendix A4 — How to reproduce

- cp infra/compose/.env.example infra/compose/.env (edit passwords, RAW_DATA_DIR)
- make build (once), make up, make smoke
- make bronze DATE=2026-10-16
- make acceptance DATE=2026-10-16 → [acceptance] PASSED
- MinIO console: http://localhost:9001 → lakehouse/warehouse/bronze.db/
- Stages: git tags task1-v1-lab, task1-v2-standard, task1-v3-operable
- Docs: docs/task1/README.md, 10_operations.md (runbook), 05 (errors), 06 (validation)

Speaker notes: Everything in this deck can be reproduced from the repository with these commands.
