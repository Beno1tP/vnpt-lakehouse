---
# Slide 1 — Task 1: Excel → Spark → Iceberg on MinIO
VNPT AI · Data Engineering onboarding lab · Task 1 of 5
Bronze layer for chatbot logs, catalogued in Hive Metastore
Bee (Pham Quang Minh) · 2026-10-22

Visual: dark title slide; small orange line "VNPT AI · DE onboarding lab · Task 1".

Speaker notes: The lab builds an end-to-end lakehouse on trial chatbot logs in 5 standalone tasks. Task 1 builds the foundation every later task depends on: storage, table format, catalog and a reliable Bronze load. I will explain why each tool is there, how it works, how Task 1 uses it, and what I proved.

---
# Slide 2 — What Task 1 does with the data
In one sentence: load the two Excel files with Spark, process them into table form, and store them in the MinIO lake in Iceberg table format (Bronze layer).

Lab target flow: Excel files → S3 Landing Zone (raw files) → Spark ETL → Bronze (Iceberg tables on MinIO)

What Task 1 implements:
- Excel files (local folder, read-only) → Spark ETL → Bronze Iceberg tables on MinIO
- The landing zone is not built yet: MinIO holds no .xlsx files, only Parquet data files + Iceberg metadata. The landing zone comes with Task 2 (Airflow waits for the file, then runs the job)
- "Process" in Bronze changes the form, not the values: read every cell as text → add 4 load columns (_ingest_date, _ingested_at, _source_file, _batch_id) → write Parquet → commit one Iceberg snapshot

Agenda: Why (file formats) · What (Docker, MinIO, Spark, Iceberg, Hive Metastore) · How (pipeline, commits, idempotency) · Proof

Visual: four boxes left to right: "2 Excel files" → "S3 Landing Zone" (dashed border, label "planned, Task 2") → "Spark ETL" → "Bronze: Iceberg on MinIO"; under Spark a small list: "read as text · add load columns · write Parquet · commit".

Speaker notes: The key question is what happens to the data. In Bronze we do not change any value; we change the form, from a spreadsheet to columnar files inside a versioned table. A landing zone would keep the original file in object storage for audit and replay. Task 1 reads from a local folder because there is no orchestrator yet; reading the file from s3://lakehouse/landing/ instead is a contained change planned for Task 2.
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
| Compression | ZIP of the whole workbook | None by default | Columnar storage allows compression optimised per column: similar values sit together → dictionary / run-length encoding, then a codec (ours: zstd) |
| Read one column | Parse the whole workbook | Scan every line | Read only that column's chunks |
| Skip data | No | No | Yes, via min/max statistics |
| Split across workers | No | Yes, if uncompressed | Yes, by row group |
| Made for | People | Simple exchange | Analytics engines |

Evidence from our chat-log file: 23.9 MB of uncompressed column data → 1.17 MB Parquet file (xlsx: 2.3 MB). Column INPUT_CHANNEL (2 distinct values, 21,224 rows) takes 153 bytes thanks to dictionary + run-length encoding.

Speaker notes: xlsx is a zipped set of XML documents designed for spreadsheet apps; a program must unzip and parse the whole workbook. CSV has no types and ambiguous quoting. Parquet stores data by column, so values of the same kind sit next to each other, which is what makes dictionary and run-length encoding so effective. The numbers come from reading the footer of our own file.
---
# Slide 5 — Inside a Parquet file: row group → column chunk → page
- File layout: magic "PAR1" · row groups · footer (file metadata) · footer length · "PAR1"
- Row group: a horizontal slice of rows; the unit of parallel reading (writers target about 128 MB)
- Column chunk: all values of one column inside one row group, stored contiguously
- Pages: the unit that is encoded, compressed and read (about 1 MB each), each with a page header (sizes, encoding, value count)
  - Dictionary page: first page of a chunk when dictionary encoding is used (the distinct values)
  - Data pages: the values (as dictionary indices or plain), plus definition levels that mark nulls; compressed with the codec
  - Optional page index (min/max per page) lets readers skip pages
- Footer: schema + for every row group and column chunk: offsets, sizes, encodings, codec, statistics (min, max, null count)
- Reading: jump to the end → read the footer → choose columns and row groups (statistics) → read only those pages

| Our chat-log file (from its footer) | Value |
|---|---|
| Writer | parquet-mr 1.17.1 (via Iceberg) |
| Rows / row groups / column chunks | 21,224 / 1 / 19 |
| Codec | ZSTD (Iceberg default) |
| Footer size | 4,344 bytes |
| ID (all unique) | PLAIN encoding, 849 KB → 435 KB |
| INPUT_CHANNEL (2 values) | dictionary page + RLE data pages → 153 bytes |

Visual: a tall rectangle "PAR1 | row group 1 [column chunk ID: data pages | column chunk INPUT_CHANNEL: dictionary page + data pages | …] | footer (schema, offsets, encodings, stats) | length | PAR1"; an arrow from "SELECT SOURCE WHERE …" to only the SOURCE chunk.

Speaker notes: The encodings list of each chunk also shows RLE and BIT_PACKED: those encode the definition levels (where nulls are), not the values. Our file has one row group because the data is small; big tables have many row groups per file and many files per table, which is what lets engines read in parallel and skip most of the data.
---
# Slide 6 — Docker: packaging and running each service
- What: runs programs in containers built from images
- How it works:
  - Image = read-only layers + metadata, identified by a digest (sha256); pulled from a registry, unpacked by containerd's snapshotter (overlay filesystem)
  - Container = a process with its own view of the system: Linux namespaces (processes, network, mounts, hostname) isolate it; cgroups limit it (our memory limits)
  - Volumes keep data beyond a container; a Compose network gives DNS by service name
- Runtime architecture:

| Layer | Talks to the next by | Role |
|---|---|---|
| docker CLI | REST API over /var/run/docker.sock | What we type |
| dockerd (Docker Engine) | gRPC | Images, networks, volumes, Compose |
| containerd | spawns one shim per container | Container lifecycle, image content |
| containerd-shim-runc-v2 | executes runc | Keeps the container's stdio and exit code |
| runc | creates namespaces + cgroups, starts the process, then exits | OCI runtime |
| container process | parent is now the shim, not runc | Our MinIO / HMS / Spark |

- Compose: one YAML file declares all services, volumes, networks, start order and health checks
- Task 1: 7 services, 2 thin images, config mounted, credentials as secret files, data-only volumes

Visual: vertical chain CLI → dockerd → containerd → shim → runc → process, with "REST", "gRPC", "exec" on the arrows and runc drawn with a dashed outline ("exits after start").

Speaker notes: A container is not a VM: it shares the host kernel. The shim is why containers survive a restart of dockerd or containerd: runc exits right after starting the process, and the shim stays as its parent. On Windows, Docker Desktop runs this whole stack inside its own WSL2 VM and forwards the socket into Ubuntu. Our rule: dependencies in images, configuration outside, credentials in files, data in volumes.
---
# Slide 7 — MinIO: how object storage works
- What: object storage that implements the Amazon S3 API
- Data model: bucket → objects; an object = key + bytes + metadata; flat namespace ("folders" are only key prefixes)
- Every action is an HTTP request: PUT, GET, LIST, DELETE on http://minio:9000/bucket/key (path-style), signed with the access key (AWS Signature v4)
- PUT, step by step:
  1. Authenticate the signature
  2. Hash the key to an erasure set (SIPMOD hashing)
  3. Split the object into data + parity shards (Reed-Solomon erasure coding; our single drive has no parity)
  4. Write the shards with bitrot checksums, then write xl.meta (size, checksums, versions)
  5. Reply with an ETag; from now on every GET sees the new object (strict read-after-write consistency)
- GET: read xl.meta → read the shards → verify checksums → stream the bytes
- No rename and no in-place edit: "rename" = copy + delete; an object is replaced, never modified
- Large objects use multipart upload (parts uploaded in parallel, then completed)

Visual: client → "PUT /lakehouse/…/x.parquet (signed)" → MinIO → erasure set → drive(s) with "shard + checksum" and "xl.meta".

Speaker notes: Every engine talks to MinIO with its normal S3 client; only the endpoint, the path-style flag, a placeholder region and the keys differ from AWS. Because objects can't be renamed or edited, a table format cannot commit by renaming files; that is why Iceberg needs a catalog for its commit. MinIO stopped publishing community images in 2025, so we use a Chainguard rebuild pinned by digest.
---
# Slide 7b — MinIO on disk: what is really in our volume
- The drive is formatted as "xl-single" (single-drive erasure format, from .minio.sys/format.json)
- Every object key is a folder on the drive:

| Object | On disk |
|---|---|
| Small: metadata.json (2,860 bytes) | Only xl.meta (3,320 bytes): the data is inlined into the metadata file |
| Larger: our Parquet file (1,170,001 bytes) | xl.meta (389 bytes) + one data folder (a UUID) with part.1 (1,170,066 bytes: data + bitrot checksums) |
| System | .minio.sys/: format.json, config, buckets, multipart, pool.bin |

- Distributed mode (production): the same format across many drives; an object becomes K data + M parity shards; any K shards rebuild it, so the cluster survives the loss of up to M drives
- Rule: never touch files in the volume directly; always go through the S3 API

Visual: a folder tree "lakehouse/warehouse/bronze.db/chatlog/data/_ingest_date=2026-10-16/00000-…parquet/ → xl.meta, 99fc…/part.1".

Speaker notes: This is why "the table lives in MinIO" is literal: the Parquet files and Iceberg metadata files are S3 objects stored as xl.meta plus parts on the MinIO volume. The extra 65 bytes in part.1 are the bitrot checksums MinIO verifies on every read.
---
# Slide 8 — Spark: how it processes big data (1/2)
- Architecture: driver program (SparkSession, plans and schedules) · cluster manager (standalone, YARN, Kubernetes: gives resources) · executors on worker nodes (run tasks, cache data, exchange shuffle data)
- Mechanism:
  - Partition: the data is split into partitions; one task per partition; tasks run in parallel on executor cores → more executors = more parallelism
  - Lazy evaluation: transformations only build a plan; an action (count, write) runs it, so the whole plan is optimised first
  - Catalyst optimizer: logical plan → optimised plan (predicate pushdown, column pruning) → physical plan
  - Tungsten engine: compact binary rows in memory, whole-stage code generation
  - Narrow vs wide: filter / select / withColumn stay inside a partition; groupBy / join / distinct need a shuffle (redistribute data by key) → a new stage
  - Memory: data that does not fit is spilled to disk; Adaptive Query Execution adjusts partitions and skewed joins at runtime
  - Fault tolerance: a lost partition is recomputed from its lineage, not restored from a copy
- Task 1: withColumn (narrow) for the load columns; the validation counts (count DISTINCT, joins) shuffle into 4 partitions (spark.sql.shuffle.partitions = 4 instead of the default 200, because the data is small)

Visual: a DAG: read → withColumn → [shuffle] → aggregate → write, split into Stage 1 and Stage 2 at the shuffle; each stage drawn as several parallel tasks.

Speaker notes: This is what makes Spark scale: the same code runs as 2 tasks on a laptop or 2,000 tasks on a cluster. The expensive part is the shuffle, because data moves between executors over the network and through disk.
---
# Slide 9 — Spark: deploy modes, and our command (2/2)

| | Where the driver runs | Executors | Used for |
|---|---|---|---|
| Client deploy mode | On the machine that submits (laptop, gateway) | On the cluster | Interactive work, notebooks |
| Cluster deploy mode | Inside the cluster (e.g. a driver pod) | On the cluster | Production jobs: Task 2 on Kubernetes |
| Local master (not a deploy mode) | One JVM, no cluster manager | Threads inside that same JVM | Development, tests, small data: Task 1 |

Our command, piece by piece:
spark-submit --master local[2] --driver-memory 2g /opt/jobs/bronze_ingest.py --ingest-date 2026-10-16
- spark-submit: the launcher; starts the driver JVM and the Python process of our PySpark job
- --master local[2]: no cluster manager; the driver runs the tasks itself with 2 threads (local = 1 thread, local[*] = all cores)
- --driver-memory 2g: heap of the driver JVM (in local mode it is also the executor); container limit 3 GiB, measured peak 1.41 GiB
- /opt/jobs/bronze_ingest.py: the application; arguments after it (--ingest-date …) go to our code
- Catalog settings come from the mounted spark-defaults.conf (catalog lake → Hive Metastore, S3FileIO → MinIO)
- Cluster equivalents: --master spark://host:7077 (standalone), yarn, k8s://https://api-server-host:6443 with --deploy-mode client|cluster

Speaker notes: Officially Spark has two deploy modes, client and cluster; local is a master setting for running everything in one process. Same code, different master: in Task 2 the Spark Operator submits in cluster mode, creating a driver pod that asks Kubernetes for executor pods. 32k rows don't need Spark (DuckDB would be enough); the lab teaches the path that scales.
---
# Slide 10 — Iceberg: the table format (structure)
- What: an open specification (plus libraries) that turns files in object storage into tables
- Structure (metadata tree):
  - Catalog: one pointer per table → the current metadata.json
  - metadata.json: schema (columns with IDs), partition spec, list of snapshots, current snapshot ID
  - Snapshot = the table at one moment → manifest list (Avro)
  - Manifest list: one entry per manifest, with partition value ranges
  - Manifest (Avro): one entry per data file: path, partition value, record count, column statistics (min, max, null count)
  - Data files: Parquet
- What for: ACID commits, snapshots, schema evolution by column ID, hidden partitioning, many engines on one table
- Architecture: a library inside each engine (in Spark: iceberg-spark-runtime) + a catalog + files; there is no Iceberg server
- Task 1: format v2; lake.bronze.chatlog and lake.bronze.bot_predict, partitioned by _ingest_date; chatlog pointer at metadata version 00007

Visual: tree "Catalog pointer → 00007.metadata.json → snapshot → manifest list → manifests → Parquet files", older metadata files greyed out beside it.

Speaker notes: A commit never edits a file: it writes new files and moves the pointer. Hidden partitioning means queries filter on the column (_ingest_date = …) and Iceberg maps that to partitions itself, without users knowing the folder layout.
---
# Slide 10b — Iceberg in action: how Spark reads and writes a table
Read: SELECT … FROM lake.bronze.chatlog WHERE _ingest_date = DATE '2026-10-16'
1. Spark asks catalog lake (Iceberg SparkCatalog → HiveCatalog) → Thrift get_table to HMS → metadata_location (…/00007-….metadata.json)
2. S3FileIO GETs metadata.json from MinIO → schema, partition spec, current snapshot (an older snapshot for time travel)
3. GET the manifest list → skip manifests whose partition range cannot contain 2026-10-16
4. GET the remaining manifests → list of data files with partition values and column min/max → skip files that cannot match
5. Plan scan tasks (split big files, combine small ones) → executors read the Parquet footers → read only the needed row groups and columns (v2: apply delete files) → rows

Write (our overwritePartitions):
1. Executors write new Parquet files to MinIO (invisible to readers)
2. The driver writes new manifests + a new manifest list + a new metadata.json
3. Commit: HMS lock → check the pointer is still the old one → alter_table to the new metadata.json → unlock; on conflict, refresh and retry

Visual: two lanes. Top lane (read): Spark → HMS (pointer) → MinIO: metadata.json → manifest list → manifests → Parquet. Bottom lane (write): Spark → MinIO: Parquet → manifests → metadata.json → HMS: swap pointer.

Speaker notes: Planning uses only metadata, so the engine knows exactly which files to open before it opens any data file. Our table has one file per load on MinIO but only one in the current snapshot, which is why a read opens one Parquet file.
---
# Slide 11 — Hive Metastore + Postgres: the catalog
- What HMS is, two things at once:
  - A running service: a Thrift server (port 9083) that engines ask "where are the files of table X, and what are its columns?"
  - A catalog model: maps logical names (databases, tables, columns, types, partitions) to physical paths in storage (HDFS, S3, our MinIO)
- It never moves or processes data files; it only holds the dictionary that tells engines where data lives
- Storage: HMS persists its catalog through DataNucleus (an object-relational mapper) over JDBC in a relational database: Postgres here (MySQL or Oracle elsewhere)
- "Schema 3.1.0" = HMS's own relational schema: 74 tables (DBS, TBLS, TABLE_PARAMS…), not the schema of our tables
- For Iceberg tables HMS keeps only table_type = ICEBERG and metadata_location (+ previous); the real schema and snapshots live in metadata.json on MinIO
- HMS also provides the lock used by Iceberg commits
- Version constraint: Spark 3.5 ships the Hive 2.3 client, which HMS 4 no longer serves (get_table) → HMS 3.1.3 (E16)

Visual: "Spark / Trino / StarRocks → Thrift :9083 → HMS → JDBC :5432 → Postgres (74 tables)"; a callout "metadata_location = s3a://…/00007-….metadata.json"; a crossed-out arrow "data files ↛ HMS".

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
