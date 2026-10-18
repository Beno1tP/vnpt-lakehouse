# Task 1 — What I learned, how to keep learning, likely questions

For the presentation: the leader wants to know what I understand about the stack, how I handled Task 1, and how I will handle Tasks 2–5. Links checked 2026-10-18 (HTTP 200), pinned to our versions where the docs are versioned.

## 1. How to explain each component (same 5 questions for every tool)

1. **What problem does it solve?** 2. **Core abstraction?** 3. **How did we configure it?** 4. **How did it fail for us, and why?** 5. **What does production do differently?**

| Tool | Problem it solves | Core abstraction | Our config | How it failed for us | Production difference |
|---|---|---|---|---|---|
| **Docker / Compose** | Same runtime everywhere; one file describes the whole stack | Image (immutable layers) → container (process + writable layer); volumes; one network with DNS by service name | Official images; config via `.env`, mounted files, one-shot init services; `mem_limit` everywhere | E01 image withdrawn; E18 paste corruption passes `config`; E20/E21 missing bind-mount source becomes a root dir, containers created before `jars-init`; E22 background exec suspended | Images baked in CI, pushed to a registry; Kubernetes instead of Compose (Task 2) |
| **MinIO** | S3 API on a laptop, so the same code runs on real S3 | Bucket + object key; no real folders, no atomic rename | Chainguard image by digest, path-style access, bucket created by `minio-init` | E01 | Cloud object storage or Ceph; versioning, lifecycle rules, IAM per service |
| **Postgres** | Durable, transactional store for the catalog's metadata | Database, tables, auth methods (`pg_hba.conf`, SCRAM) | `postgres:16`, healthcheck, one DB `metastore` | E21: old JDBC driver can't do SCRAM-SHA-256 (auth type 10) | HA (replicas, backups, PITR); one instance per platform, DB per service |
| **Hive Metastore** | Shared catalog: table name → location/metadata, used by Spark, Trino, StarRocks | Thrift service; databases, tables; HMS stores Iceberg's current `metadata_location` | `apache/hive:3.1.3`, mounted `hadoop-aws` 3.1.0 + SDK + JDBC jar, `core-site.xml` for S3A, `hms-schema-init` | E16 HMS 4 vs Hive 2.3 client; E17 Guava mismatch in S3A; E07/E19 schema init | HA HMS behind a load balancer, or a REST catalog |
| **Spark** | Distributed compute for batch transforms | Driver + executors; DataFrame → logical plan → physical plan; catalogs plugged in via config | `apache/spark:3.5.9`, `local[2]`, driver 2g, catalog `lake` in `spark-defaults.conf`, packages via Ivy | Embedded Derby warning for `spark_catalog` (harmless); Python stdout buffering in background runs | Cluster mode on K8s/YARN, dynamic allocation, history server, tuned shuffle |
| **Iceberg** | Table semantics (ACID, schema evolution, time travel) on plain files in object storage | Table → metadata.json → snapshot → manifest list → manifests → data files; commit = atomic pointer swap in the catalog | Format v2, partition by `_ingest_date`, `overwritePartitions()` | Drill B: atomic per table, not across tables | Scheduled maintenance: `expire_snapshots`, `rewrite_data_files` (compaction), `remove_orphan_files`; Write-Audit-Publish with branches |

## 2. Standard approaches to learn (patterns, not tools)

| Pattern | One line | Where we used it |
|---|---|---|
| Medallion (Bronze/Silver/Gold) | Raw copy → cleaned/typed → business marts | Bronze now, Silver next, Gold in Task 4 |
| Idempotent loads | Rerun = same result; overwrite a partition or MERGE by key | `overwritePartitions()` per `_ingest_date` |
| Atomic commit + retry | Failed job exits non-zero; orchestrator retries; rerun is safe | Drills A and B |
| Check-then-create init | Init steps safe to run every start | `hms-schema-init`, `jars-init`, `spark-init` |
| Pin everything | Tags or digests; change one version at a time | `.env`, `requirements.txt` |
| Config outside the image (12-factor) | Same image, different env/config | `.env`, mounted `spark-defaults.conf`, `core-site.xml` |
| Read the server log | Clients only see symptoms ("connection lost") | E17 |
| Table maintenance | Snapshots and small files grow forever unless expired/compacted | Not yet: planned Iceberg maintenance job |

## 3. Reading list (start with the bold ones)

| Topic | Read | Why |
|---|---|---|
| Iceberg | **[Spark quickstart](https://iceberg.apache.org/spark-quickstart/)**, **[Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/)** (`overwritePartitions`, MERGE), [Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/), [Reliability](https://iceberg.apache.org/docs/latest/reliability/) (why commits are atomic), [Maintenance](https://iceberg.apache.org/docs/latest/maintenance/), [Spark procedures](https://iceberg.apache.org/docs/latest/spark-procedures/), [Hive](https://iceberg.apache.org/docs/latest/hive/), [Table spec](https://iceberg.apache.org/spec/) (the metadata tree) | Everything we did maps to these pages |
| Spark 3.5.9 | **[Configuration](https://spark.apache.org/docs/3.5.9/configuration.html)** (`spark.jars.packages`, memory), [Submitting applications](https://spark.apache.org/docs/3.5.9/submitting-applications.html), [Hive tables](https://spark.apache.org/docs/3.5.9/sql-data-sources-hive-tables.html) (the Hive 2.3 client), [Performance tuning](https://spark.apache.org/docs/3.5.9/sql-performance-tuning.html), **[Running on Kubernetes](https://spark.apache.org/docs/3.5.9/running-on-kubernetes.html)** (Task 2) | Config is where most of our work was |
| Docker | **[Compose file reference](https://docs.docker.com/reference/compose-file/)**, **[Startup order](https://docs.docker.com/compose/how-tos/startup-order/)** (`depends_on` conditions, E21), [Bind mounts](https://docs.docker.com/engine/storage/bind-mounts/) (E20/E21), [Build best practices](https://docs.docker.com/build/building/best-practices/) (for when an image must be built) | Our three Docker bugs are all in these pages |
| S3A / MinIO | **[Hadoop S3A guide](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)** (stable = newest Hadoop; our HMS uses 3.1.0, so option names may differ), [MinIO docs](https://docs.min.io/) | Path-style access, credentials providers, why rename is not atomic |
| Hive Metastore | [Hive docs](https://hive.apache.org/docs/latest/) (Metastore administration section) | Schema tool, metastore config |
| Postgres 16 | **[Password authentication](https://www.postgresql.org/docs/16/auth-password.html)** (SCRAM vs md5, E21), [Manual](https://www.postgresql.org/docs/16/index.html) | Why the old driver failed |
| Patterns | [12-factor: config](https://12factor.net/config), Databricks "medallion architecture" glossary page | Vocabulary for the design |
| Books (optional) | *Apache Iceberg: The Definitive Guide* (O'Reilly, 2024); *Learning Spark*, 2nd ed. (O'Reilly, 2020; Spark 3.0, still valid for concepts) | Deeper theory |

Suggested order before Task 2: Iceberg quickstart + writes + reliability (1 h) → Spark configuration + running on Kubernetes (1 h) → Compose startup order + bind mounts (30 min).

## 4. Likely questions (short answers)

| Question | Answer |
|---|---|
| Why does Iceberg need a catalog? | A commit swaps the pointer to the new `metadata.json` atomically; object storage can't do compare-and-swap, the catalog can. |
| Why not the Hadoop catalog? | It commits by file rename; S3 has no atomic rename, so concurrent writers can corrupt the table. Trino can't use it either. |
| What if a load fails halfway? | Nothing is committed, so readers keep the previous snapshot; files already uploaded become orphans (cleaned by `remove_orphan_files`). Drill A: no snapshot. Drill B: table unchanged until its own commit. Rerun the date. |
| Why are two tables out of step after a crash? | Commits are atomic per table, not across tables. The idempotent rerun realigns them; Silver should check matching `_batch_id`. |
| Why all strings in Bronze? | The raw copy never fails or loses data on a parse error; typing is a Silver rule where errors are counted. |
| Why HMS 3.1.3, not 4? | Spark 3.5's built-in Hive 2.3 client calls `get_table`, which HMS 4 no longer serves (E16). |
| Why hadoop-aws 3.1.0 exactly? | It must match the image's hadoop-common; 3.1.3 was built against Guava 27, the image has Guava 11/19 → `NoSuchMethodError` (E17). |
| How do you prove idempotency? | Rerun → `overwrite` snapshot, deleted = added = 21,224, total unchanged; new date → new partition only. |
| Why not fill empty columns like `UPDATED_INTENT`? | They are empty in the source (0 non-null); filling would fabricate data. Open question for Silver: should a future `UPDATED_INTENT` override `INTENT_NAME`? (Current rule in CLAUDE.md does not use it.) |
| What would you change for production? | JDBC/CDC instead of Excel; baked images from CI; REST catalog or HA HMS; secrets out of logs; snapshot expiry/compaction jobs; K8s + Airflow (Task 2). |
| How will Task 1 carry into Tasks 2–5? | Same job and versions on K8s (init steps become initContainers, `.env` becomes ConfigMap/Secret); Trino/StarRocks read the same HMS catalog; Gold builds on Silver. |

## 5. Global view: roles, alternatives, other architectures

| Role | Ours | Alternatives | Alternative wins when | Why ours here |
|---|---|---|---|---|
| Object storage | MinIO (Chainguard) | AWS S3, GCS, ADLS; Ceph RGW, SeaweedFS, Garage | Cloud: no ops; Ceph: big on-prem | S3 API locally; risk: community distribution ended (E01) |
| Table format | Iceberg v2 | Delta Lake, Hudi, Paimon, plain Parquet | Delta: Databricks; Hudi: upsert-heavy; Paimon: Flink streaming | Engine-neutral for Spark/Trino/StarRocks |
| Catalog | Hive Metastore 3.1.3 | Iceberg REST (Polaris, Lakekeeper, Gravitino, Unity OSS), Glue, Nessie, JDBC catalog | REST: no Hive-client coupling (would avoid E16/E17); Glue: AWS; Nessie: multi-table branches | Named by the lab; Trino/StarRocks speak HMS |
| Catalog DB | Postgres 16 | MySQL/MariaDB, Derby (dev only), Oracle | Platform already runs MySQL | One instance for HMS, Airflow, Superset |
| Compute | Spark 3.5.9 | Trino, Flink, DuckDB, PyIceberg + pandas/Polars, EMR/Dataproc/Databricks | Small data: DuckDB/PyIceberg (32k rows don't need Spark); SQL-only: Trino; real time: Flink | Lab teaches the scale-out path (Task 2 on K8s) |
| Ingestion | pandas reads Excel | spark-excel, CSV/Parquet first; prod: JDBC, CDC (Debezium/Kafka), Airbyte/Fivetran | Continuous changes: CDC | Lab gives Excel; fits in the driver |
| Runtime | Compose + official images + config | Baked CI images, Kubernetes, managed Spark | Production: immutable images, multi-node | One laptop, disk-limited, no builds |
| Layering | Medallion | Direct typed load, Data Vault, warehouse raw schema | Many sources + strict history: Data Vault | Raw copy can't fail; per-batch rerun/audit |

| Architecture | Pros | Cons | Fits when |
|---|---|---|---|
| **Open lakehouse (ours)** | Open formats, many engines, scales | Most moving parts, version coupling | Several engines share data (Tasks 3–5) |
| Classic warehouse (Postgres/ClickHouse/StarRocks native) | Simplest, SQL from day one | One engine owns storage; history is manual | One engine, moderate data |
| Single node (DuckDB, PyIceberg) | Tiny footprint, minutes to build | One machine, limited concurrency | Small data, prototypes |
| Managed cloud (Glue+Athena/EMR, Databricks, Snowflake+Iceberg) | No infra, governance built in | Cost, lock-in; data leaves the laptop (not allowed here) | Company cloud platform |
| Streaming (Debezium/Kafka → Flink → Iceberg/Paimon) | Near real time | Hardest to operate | Live metrics |

One-liner: "DuckDB would be enough for this data size; I used the lakehouse stack because later tasks share tables across engines. Today I'd evaluate an Iceberg REST catalog instead of HMS to remove the Hive-client version chain (E16/E17)."
