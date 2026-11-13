# Slide 1 — Task 3: Silver with Spark, queried by Trino
VNPT AI · Data Engineering onboarding lab · Task 3 of 5
A typed, deduplicated Silver layer, and a second engine that reads the same Iceberg files
Bee (Pham Quang Minh) · 2026-11-10

Visual: dark title slide; small orange line "VNPT AI · DE onboarding lab · Task 3".

Speaker notes: Task 1 built the lake and a reliable Bronze load; Task 2 runs it per day on Kubernetes under Airflow. Task 3 adds the layer the business can use, Silver, and proves the lake is open: Trino, a different engine with a different architecture, reads the same tables through the same catalog and gets the same numbers. Same structure as before: knowledge, the stack, what I learned running it, the errors.

---
# Slide 2 — What Task 3 does with the data
In one sentence: every day, after Bronze, a Spark job parses, deduplicates and flags the day's rows into three Silver Iceberg tables; then Trino, read-only, recomputes every Silver number and must agree with Spark and with pandas.

Lab target flow: Bronze (Iceberg) → Spark ETL → Silver (Iceberg) → Trino query

What Task 3 implements:
- Silver job in the existing DAG: check_landing → bronze → validate (23) → **silver → validate_silver (28)** → notify
- Three tables: silver.chat_message (every chat row + flags), silver.nlu_prediction (every prediction + final_intent), silver.user_message (client messages joined with their prediction)
- Trino 483 on Kubernetes, read-only twice (MinIO policy + catalog setting)
- DAG trino_reconcile, started by an Airflow asset when Silver changes: Trino recomputes 191 values and compares them
- Iceberg tag task3_v1 on the 2 Bronze + 3 Silver tables (the freeze)

Agenda: Knowledge (Silver rules, timestamps, idempotent writes, Trino architecture, read-only, assets) · The stack · Running it (pruning, metadata tables, two engines, time travel, stats) · Errors · Next

Visual: Bronze box → "Spark: parse · dedup · flag · final_intent · join" → three Silver boxes → a Trino box reading the same Silver boxes with a dashed "read-only" border; Airflow above with "asset lake_silver" arrow into "trino_reconcile".

Speaker notes: Bronze keeps the data as delivered; Silver is where the data becomes trustworthy: typed, one row per id, business flags. The Trino part is the open-lake proof: no copy, no export, just a second reader of the same files.

---
# Slide 3 — The problem
- Bronze is all text, by design: dates are strings, the confidence is a string, testers and "/home" clicks are mixed with real questions
- Business questions need: a real timestamp and day, one row per message, "who is a client", "was the bot understood", the message and its prediction together
- Requirements:
  - Correct: the same numbers from pandas (Excel), Spark and Trino
  - Idempotent: rerunning a day replaces that day, nothing else
  - No silent drops: flag, don't delete (the 116 predictions without a chat row stay, flagged)
  - Open: a second engine reads the same tables, read-only
  - Automated: the cross-engine check runs by itself when Silver changes
- Constraint: the laptop cannot run Trino and a Spark job at the same time (RAM)

Visual: left a Bronze row of strings ("02-JUL-24 03.40.09.895455000 PM", "0.93", "/home"); right the Silver row (timestamp, double, is_home = true, final_intent = xin_chao).

Speaker notes: Each requirement maps to one mechanism in this deck: type rules, overwrite by day, flags, Trino's catalog, assets, and modes for the RAM limit.

---
# Part 0 — Global view (from the Task 3 learning pack, docs/task3/Fundamentals.pdf)

---
# Slide G1 — Three maps for Task 3
- **Data flow:** Bronze (chatlog, bot_predict, partition _ingest_date = event day) → silver_transform checks both tables carry the same _batch_id, types and flags the rows, dedups by ID, computes final_intent → three Silver tables partitioned by event_date, overwritePartitions() → validate_silver (28 checks). Trino reads the same Iceberg files: no copy
- **Orchestration:** daily DAG check_landing → bronze → validate → silver → validate_silver → notify; asset-scheduled trino_reconcile on LAKE_SILVER (since E67: waits for an idle pipeline, verdict decides the run state)
- **Outside Airflow:** CronJob pg-backup at 06:30 UTC, nudged by make k8s-up (E66)
- **Modes:** mode-query = scheduler, dag-processor, triggerer and History → 0, Trino → 1 (api-server stays: UI and CLI work); mode-pipeline reverses it; from Task 4.5 trino_reconcile skips itself in gold mode

Visual: three columns: Airflow (chatbot_bronze → asset → trino_reconcile) · Spark apps → HMS → MinIO Iceberg tables · Trino coordinator (single node, 0 ↔ 1, catalog lake read_only, MinIO user trino) with arrows "Thrift 9083", "S3 read 9000", "K8s API + Trino REST 8080".

Speaker notes: Three maps answer three questions: where the data goes, who starts what, and what runs outside Airflow. The highlighted new piece is Trino, as a read-only second engine.

---
# Slide G2 — Durable state: what Task 3 added

| State | Where | Survives |
|---|---|---|
| Silver tables (3) | MinIO lakehouse + HMS pointer | pod restarts; not minikube delete |
| Iceberg tags task3_v1 (5 tables, 365 days) | inside each table's metadata | snapshot expiry: a tagged snapshot is never expired |
| Expected numbers (silver_daily.json, silver.json) | baked into the Airflow and spark-job images | rebuilt from the git tag |
| Spark event logs (195 files) | PVC spark-events (namespace spark-jobs), not MinIO | same failure domain as the lake; evidence copies them out with kubectl cp |
| Postgres dumps (pg-backup) | MinIO bucket backup | inside Minikube like the lake; the evidence copy takes them out |
| Evidence copy (120 MB) | ~/data/vnpt-lab/evidence/task3-20261110/ | outside Minikube: survives minikube delete (not the loss of the laptop disk) |
| Trino | stateless | nothing to back up; only config + Secret |

Speaker notes: For every piece of state, say where it lives and what kills it. The only copy outside the cluster is the evidence folder; everything else shares the laptop disk.

---
# Slide G3 — Is the Task 3 pipeline standard? Hop by hop (1/2)
- **The core is standard:** Spark writes Iceberg, Trino reads the same tables through a shared HMS (one copy, two engines); asset-triggered DAGs are the Airflow 3 way to start work when data changes
- **Lab-specific, and defensible:** (1) scaling Trino 0 ↔ 1 per check: in production Trino runs continuously; here Trino (2 Gi) and a Spark run don't fit together (D12). (2) a hand-written Trino REST client in plugins/lake_reconcile.py instead of the Trino Airflow provider: smaller image, unit-tested

| # | Hop | Connection | Failure handling |
|---|---|---|---|
| 1 | silver reads Bronze | Spark → HMS (Thrift 9083) → files from MinIO (9000) | refuses unless both tables carry the same _batch_id (no cross-table transaction) |
| 2 | silver writes 3 tables | executors write Parquet; driver commits through HMS | three separate commits; overwritePartitions() replaces only event_date = D; refuses rows with another date |
| 3 | validate_silver → asset event | task success records the event (Task Execution API) | failed or skipped task records nothing → no reconcile run |
| 4 | asset event → trino_reconcile run | scheduler creates the run from queued events | several updates can share one run; in the 11-10 backfill each day got its own (18 runs, ~2 min apart) |
| 5 | wait_pipeline_idle (E67) | sensor (reschedule) checks the K8s API for unfinished SparkApplications and the metadata DB for running or queued chatbot_bronze runs | keeps Trino and Spark from sharing RAM, and reconcile off half-updated Silver |

---
# Slide G4 — Is the Task 3 pipeline standard? Hop by hop (2/2)

| # | Hop | Connection | Failure handling |
|---|---|---|---|
| 6 | trino_up / trino_down | task → K8s API (443): scale the trino-coordinator | Role airflow-trino-scaler, only that Deployment; trino_down all_done; since E67 a verdict leaf fails the run, and CI refuses DAGs whose only leaves are all_done |
| 7 | reconcile → Trino | HTTP to the coordinator REST API (8080) | any mismatch or SQL error fails the task (retries 0) |
| 8 | Trino → HMS | Trino's own Thrift client (9083); HMS has no authentication | iceberg.security=read_only refuses writes inside Trino, before HMS is touched |
| 9 | Trino → MinIO | native S3 client (9000), user trino | read-only policy on lakehouse/: second layer |
| 10 | result vs expected | 191/191 at the freeze, 210/210 after step | three independent computations must agree: pandas, Spark validator, Trino SQL |
| — | pg-backup (outside Airflow) | CronJob controller | E66: missed slots after sleep; k8s-up nudges it |

Speaker notes: Public builds of the same core exist (Spark + Trino + Iceberg sharing one HMS); the two lab-specific parts are honest consequences of one laptop.

---
# Part 1 — Knowledge

---
# Slide 4 — Medallion: what changes from Bronze to Silver

| | Bronze (Tasks 1–2) | Silver (Task 3) |
|---|---|---|
| Types | every column a string | event_ts timestamp, event_date date, confidence double |
| Partition | _ingest_date (= the export day on Kubernetes) | event_date (the day business questions use) |
| Duplicates | none inside one partition | one row per id across the table |
| Meaning | none, as delivered | flags is_client, is_home, is_internal_sender; final_intent |
| Tables | two, as the source | plus a joined table: client message + its prediction |

- Rules: never fill empty source columns; flag, don't delete ("/home" and testers stay, the dashboard filters them)
- Iceberg has no cross-table transaction: before joining, Silver checks both Bronze tables of the day carry the same _batch_id, otherwise it fails (rerun Bronze)

Visual: two columns "Bronze" and "Silver" with arrows labelled "parse", "dedup", "flag", "join".

Speaker notes: The batch check is the lesson of Task 1's drill B: commits are atomic per table, so a crash between the two Bronze commits leaves them on different batches. Silver refuses to join two different loads.

---
# Slide 5 — Business rules in Silver
- final_intent:
  - INTENT_NAME = 'fallback' → fallback
  - confidence < the row's NLU_THRESHOLD → bot_khong_hieu ("bot did not understand")
  - otherwise COALESCE(UPDATED_INTENT, INTENT_NAME)
- NLU_THRESHOLD is a column of the predict file (0.8 everywhere here): use the row's value, never a hard-coded 0.8 or a job parameter that could disagree with the source (D9)
- UPDATED_INTENT is empty in this data: only a unit test exercises that branch
- Flags: is_client (SOURCE = 'client'), is_home (TEXT = '/home'), is_internal_sender (the 2 e-mail sender ids)
- has_chat_row on predictions: the 116 orphan predictions stay, flagged; has_prediction on user messages

Visual: a decision tree for final_intent with the three outcomes and their counts (xin_chao 10,588 · fallback 107 · bot_khong_hieu 34).

Speaker notes: The counts are the ones Gold and the dashboard show later. The rule order matters: fallback first, then the threshold.

---
# Slide 6 — Timestamps: parse once, store without a zone
- Source: CREATED_TIME = "02-JUL-24 03.40.09.895455000 PM": 9 fraction digits, no zone
- Pattern dd-MMM-yy hh.mm.ss.SSSSSSSSS a (case-insensitive month); Spark keeps microseconds (the last 3 digits are truncated)
- Stored as **TIMESTAMP_NTZ** (to_timestamp_ntz) → Iceberg timestamp without zone → Trino timestamp(6), StarRocks DATETIME: the wall clock as written, the same in every engine (D8)
- The trap: Spark's default TIMESTAMP is "with local zone" → Iceberg timestamptz (a UTC instant) → every reader converts to its session zone → "7 hours off"
- Plus event_date DATE: dates never shift; dashboards group by it
- The source zone (assumed Asia/Ho_Chi_Minh) is documented, not baked into values: a leader question

Visual: one value through three readers: NTZ → "15:40:09" everywhere; timestamptz → "15:40 / 08:40 / 22:40" depending on the reader's zone.

Speaker notes: This is a classic production bug. Storing the source's wall clock without a zone, plus a date column, means no engine can shift it.

---
# Slide 7 — Writing Silver idempotently: overwrite by day vs MERGE

| | overwritePartitions() (ours) | MERGE INTO … ON id |
|---|---|---|
| What | replaces exactly the event_date partitions in the batch | updates matching ids, inserts new ones |
| Rerun a day | same table ✅ | same table ✅ |
| Corrected export drops a row | row disappears ✅ | row stays ❌ unless a scoped "not matched by source → delete" |
| Late event of day D in the export of D+1 | would replace D with only the late rows ❌ | correct ✅ |
| Needs | nothing new | SQL extensions; copy-on-write rewrites whole files |

- Our pipeline: one export = one event day (a Bronze rule enforces it) → overwrite by event_date is correct and simpler (D7)
- Dedup first anyway (row_number() over id, newest _ingested_at): MERGE fails on duplicate source keys ("cardinality violation"); an overwrite would keep both
- MERGE is the tool for incremental loads (CDC, late events): knowledge only here

Visual: two timelines: "overwrite day 07-24" replacing one partition box; "MERGE" touching individual rows across boxes.

Speaker notes: Choosing the write strategy is choosing which failure you accept. Our grain is the day everywhere (schedule, partition, rerun, validation), so the day overwrite can never touch another day.

---
# Slide 8 — Trino: what it is and how it works
- What: a distributed SQL query engine; it stores nothing and reads data where it lives through connectors
- Coordinator: parses SQL, cost-based planning, splits work, tracks queries; UI and REST API on 8080
- Workers: run tasks on splits (pieces of files) in memory, stream pages between stages (pipelined, no disk shuffle by default)
- Catalog: one properties file per source; names are catalog.schema.table (ours: lake.silver.user_message)
- Single node (ours): the coordinator also schedules work on itself, 0 workers
- Failure model: a worker failure fails the query and the client reruns; fault-tolerant execution (retry-policy=TASK + an exchange on S3) is for long ETL, not needed here
- Memory: query.max-memory-per-node + memory.heap-headroom-per-node (both default 30 % of the heap) must fit inside -Xmx; ours: 600 MB query memory in a 1,536 MB heap inside a 2 Gi pod (heap 75 % of the limit)

| | Trino | Spark |
|---|---|---|
| For | interactive SQL, seconds, many users | batch ETL, DataFrame code |
| Failure | query fails, rerun | per-task retries, lineage |
| Data movement | in memory, streamed | shuffle files on disk |

Visual: client → coordinator (parser → planner → scheduler) → workers reading splits from MinIO; arrows "pages" between stages.

Speaker notes: Trino and Spark are not competitors here: Spark writes, Trino reads. Trino's speed comes from never writing intermediate data to disk; the price is that a lost worker fails the whole query.

---
# Slide 9 — How Trino reads our Iceberg tables
1. Trino's own Thrift client asks HMS (3.1.3) for the table's metadata_location
2. Reads metadata.json, the manifest list and the manifests from MinIO (native S3 file system)
3. Prunes files by partition values and column min/max statistics, at planning time
4. Reads only the needed Parquet column chunks of the remaining files
- Same files Spark wrote, no copy, no export

Catalog file (lake.properties, secrets from env):
- connector.name=iceberg · iceberg.catalog.type=hive_metastore · hive.metastore.uri=thrift://hive-metastore.lake…:9083
- **iceberg.security=read_only**
- fs.native-s3.enabled=true · s3.endpoint=http://minio.lake…:9000 · s3.path-style-access=true · s3.aws-access-key=${ENV:TRINO_S3_ACCESS_KEY}

Visual: Trino → HMS ("where is chat_message?") → MinIO (metadata.json → manifests → 1 Parquet file); 17 other files greyed out ("pruned").

Speaker notes: The legacy hive.s3.* Hadoop settings are gone in recent Trino; the native S3 client is configured with s3.* properties. The keys never appear in the file, only ${ENV:…} references filled from a Kubernetes Secret.

---
# Slide 10 — Read-only, at two layers (D11)
1. MinIO user trino: a read-only policy on lakehouse/* (one service user per consumer, as in Task 2)
2. iceberg.security=read_only in the catalog: Trino refuses INSERT, DELETE, DROP, ALTER, ANALYZE itself, before touching S3 or HMS
- Why both: HMS has no authentication. An S3 read-only user alone would not stop Trino from dropping a table in the metastore (a metadata change, no S3 write)
- Consequence: no ANALYZE from Trino (it writes a Puffin statistics file + a new metadata.json). Intended: Spark owns writes
- Proof by negative test: CREATE TABLE and ANALYZE → Access Denied; make s3-access-test 4 profiles × 6/6

Visual: Trino → two locks: "catalog: read_only" and "MinIO: policy read lakehouse/*"; HMS drawn without a lock, with a note "no auth".

Speaker notes: Least privilege is proven by what is denied, not by what works. The second layer exists because of a real gap: HMS trusts whoever can reach port 9083.

---
# Slide 11 — Airflow assets: linking DAGs by data
- Asset = a named piece of data (ours: lake_silver); a task declares outlets=[asset]; a DAG can be scheduled on assets instead of a clock
- validate_silver succeeds → an asset event → trino_reconcile is queued, no polling, no cross-DAG sensor
- trino_reconcile today (dags/trino_reconcile.py): not_gold_mode (short-circuit, added in Task 4) → wait_pipeline_idle (sensor, reschedule mode: no worker slot held; waits until no SparkApplication is unfinished **and** chatbot_bronze has no running or queued run) → trino_up (0→1) → reconcile (REST API; 191 values at the freeze, 210 after Task 4 added step) → trino_down (trigger rule all_done: always back to 0) → **verdict** (leaf: fails the run if reconcile or trino_down failed)
- At the freeze the sensor was wait_no_spark and trino_down was the only leaf; E67 (found during Task 4's backfill) changed both: see slide 21b
- RBAC: Airflow's ServiceAccount may scale only the trino-coordinator Deployment, nothing else in that namespace
- Pattern: one DAG per layer, linked by data events; the same pattern links Gold in Task 4

Visual: chatbot_bronze (… → validate_silver ● outlet) → "asset lake_silver" (a dataset icon) → trino_reconcile (6 task boxes; trino_down marked "all_done"; verdict drawn as the leaf).

Speaker notes: An asset is a contract: "Silver changed". The consumer does not need to know which DAG produced it or when. trino_down always runs, so a failed check never leaves Trino holding 2 GiB; verdict is what makes a failed check a failed run. A failed or skipped producer task records no event, so no reconcile run starts: silent by design.

---
# Slide C1 — The stack compared by criteria: identity and mechanism

| Tech | Kind | Basic unit | Core mechanism | Function here |
|---|---|---|---|---|
| Silver layer | a design contract, not a tool | one table per entity, one row per business key | type, flag, dedup, join; quality gates at the boundary | 3 tables from 2 Bronze tables, per event_date |
| Spark SQL window | part of Spark | window = partition + order | row_number() over (partition by ID order by _ingested_at desc) = 1 | dedup before writing |
| Iceberg (Task 3 features) | table format | snapshot, tag, partition; timestamp vs timestamptz | named refs to snapshots; zone-free vs UTC-instant time types | event_ts NTZ, tags task3_v1 |
| Trino | distributed SQL engine, stores nothing | query → stage → task → split | coordinator plans; workers stream pages in memory | read-only checks and exploration |
| Trino Iceberg connector | plugin | catalog lake → schema → table | asks HMS for metadata_location, prunes with manifest stats | same files Spark wrote |
| Airflow assets | data-aware scheduling | asset (URI) + asset event | producer outlets record events; consumer DAGs get runs | LAKE_SILVER → trino_reconcile |
| Task Execution API auth | part of Airflow 3 | short-lived JWT per task | supervisor heartbeats; the API returns a refreshed token | broke after laptop sleep (E64) |
| Kubernetes CronJob | K8s controller | CronJob → Job → Pod | computes the next slot, re-queues itself | pg-backup (E66) |

---
# Slide C2 — Knowledge levels and state

| Tech | Basic knowledge to own | Advanced (ours → production) | Evidence |
|---|---|---|---|
| Silver | grain, keys, types, "flag, don't delete" | rules in SQL + validator → SCD history, quarantine tables, CDC with MERGE | D7–D9, 28/28 × 18 days |
| Window dedup | partition vs order, ties | deterministic tie-break → "latest" defined as a business rule | no duplicate ID across the table |
| Iceberg | snapshots, time travel, zone semantics | tags with retention → branches, write-audit-publish | 7-hour-shift risk avoided (D8); freeze 11-10 |
| Trino | coordinator vs worker, connector, catalog | single node, 2 Gi Guaranteed, heap 1,536 MB → workers, fault-tolerant execution, resource groups | steps 3.3–3.5 |
| Trino security | read-only at two layers | read_only catalog + S3 read user → authentication, file/OPA access control | CREATE TABLE, ANALYZE → Access Denied |
| Assets | producer outlets, consumer schedule=[asset] | shared asset in plugins/ → conditions (&, \|), watchers, REST-pushed events | run created by the asset, 11-10 03:58 UTC |
| Task JWT | tokens expire; heartbeats renew them (default lifetime 600 s) | retries for transient failures → longer lifetime or never-sleeping nodes | E64 |
| CronJob | startingDeadlineSeconds, concurrencyPolicy (>100 missed slots without a deadline stops scheduling) | nudge after wake → production nodes don't sleep; alert on absence | E66 |

Speaker notes: The left column is what I must be able to explain without notes; the middle column is what I would change for production, and why the lab version is acceptable here.

---
# Part 2 — The stack of Task 3

---
# Slide 12 — Architecture (what Task 3 adds to Task 2)

| Namespace | Adds |
|---|---|
| spark-jobs | SparkApplications silver + validate_silver (same vnpt-lake-spark-job image) |
| trino | Trino 483 (chart 1.44.0, SHA-256 checked, image by digest): 1 coordinator = worker, 2 Gi Guaranteed |
| lake | MinIO user trino (read-only), Secret trino-s3-credentials |
| airflow | DAG trino_reconcile (asset-scheduled), RBAC to scale Trino |

Iceberg (HMS 3.1.3 + MinIO): bronze.chatlog, bronze.bot_predict · silver.chat_message · silver.nlu_prediction · silver.user_message — writer Spark, reader Trino.

Visual: the Task 2 diagram greyed, with the new pieces in colour: two Spark boxes, the trino namespace, the asset arrow.

Speaker notes: Trino gets a fixed name and a chart verified by checksum, the same discipline as the Airflow and Spark Operator charts.

---
# Slide 13 — How the pieces talk

| From → To | How | Credentials |
|---|---|---|
| Spark silver → MinIO / HMS | S3FileIO / Thrift :9083 | MinIO user spark-jobs |
| Trino → HMS | Thrift :9083 (Trino's own client) | none (HMS has no auth) |
| Trino → MinIO | native S3 client | MinIO user trino (read-only) |
| Airflow reconcile → Trino | REST API :8080 (POST /v1/statement, follow nextUri) | — |
| Airflow → K8s API | scale trino-coordinator, create SparkApplications | ServiceAccount + Roles |
| validate_silver → Airflow | asset event lake_silver | — |

Speaker notes: The reconcile task uses Trino's REST protocol with the Python standard library: no Trino provider in the Airflow image, one dependency less to pin.

---
# Slide 14 — Why these, and not others

| Role | Ours | Alternatives | Why ours here |
|---|---|---|---|
| Silver engine | Spark job in the daily DAG (D6) | dbt on Trino, SQL in StarRocks | same image and tooling as Bronze; Silver is ETL, not serving |
| Write strategy | overwrite by event_date (D7) | MERGE by id | one export = one day; MERGE for CDC/late data |
| Timestamps | TIMESTAMP_NTZ + event_date (D8) | timestamptz | no zone shift in any reader |
| Query engine | Trino 483 | Spark SQL, StarRocks, DuckDB | named by the lab; federated, interactive |
| Trino size | single node, 2 Gi | coordinator + workers | 21k rows; RAM budget |
| Cross-DAG link | asset | ExternalTaskSensor, TriggerDagRunOperator | data-driven, no polling, Airflow 3 native |

- Honest view: for 21k rows Trino is not needed for speed; it proves the lake is open to a second engine

Speaker notes: Each alternative wins elsewhere: dbt on Trino if Silver were pure SQL and Trino a shared cluster; MERGE if the source sent changes instead of daily exports.

---
# Slide 15 — Modes: pipeline vs query (D12)

| Mode | Runs | Requests |
|---|---|---|
| pipeline | Airflow scheduler 1, Trino 0: Spark can run | idle 4,036 Mi (55 %); during a Bronze run 7,236 Mi |
| query | scheduler 0, Trino 1 | ≈ 4,452 Mi |
| demo-task3 | view-only Airflow + Trino + History Server | **4,964 Mi (68 %)** |

- Never Trino + Spark together: during a Bronze run only 32 Mi would be left
- make mode-query / mode-pipeline switch; trino_reconcile scales Trino itself and always back to 0
- The cluster state is the single source of truth: commands refuse if a Spark run is active

Visual: a bar of 7,268 Mi Allocatable filled by blocks per mode.

Speaker notes: On one laptop the scheduler's requests are the budget. Modes are an honest way to share it, instead of overcommitting and hoping.

---
# Part 3 — Running it: what the engines showed

---
# Slide 16 — Partition pruning, measured (EXPLAIN ANALYZE)

| Query on silver.user_message | Splits (files) | Input rows | Input (logical / physical) |
|---|---|---|---|
| GROUP BY final_intent, no filter | **18** | 10,613 | 176.70 kB / 571.84 kB |
| … WHERE event_date = 2024-07-24 | **1** | 6,485 | 107.66 kB / 296.72 kB |

- event_date is the partition column; each manifest records the partition value of every file → Trino drops 17 files at planning time, before opening any Parquet

Visual: 18 file icons, 17 greyed; the one for 07-24 highlighted.

Speaker notes: On big tables this is the main reason dashboards filter by date. Our Gold tables in Task 4 keep the same partition key.

---
# Slide 17 — Metadata tables and "one table, two engines"
- Trino exposes Iceberg metadata as tables: "chat_message$snapshots", $files, $partitions, $history, $manifests
- silver.chat_message: **38 snapshots** (2 hand runs + 18 days × 2 backfill passes), 18 partitions, 1 file each, 21,224 rows, ~629 KiB

| Engine | Current metadata.json of bronze.chatlog | Snapshots |
|---|---|---|
| Spark (make catalog CMD=info) | …/00079-8085977d….metadata.json | 60 |
| Trino ($metadata_log_entries, $snapshots) | **the same file** | **60** |

- HMS stores one pointer per table; both engines resolve it to the same metadata.json → same snapshot → same rows
- Time zones: Spark's collect() printed commit times in the pod zone (UTC); Trino with SET TIME ZONE printed ICT: same instants, always label the zone

Visual: Spark and Trino on both sides, both arrows landing on one metadata.json in the middle.

Speaker notes: Reruns add snapshots, never rows: every overwrite is one commit. One compact file per day means no small-file problem yet; the weekly maintenance DAG would compact if it appeared.

---
# Slide 18 — Time travel and tags

| Version read by Trino | Rows | Meaning |
|---|---|---|
| bronze.chatlog FOR VERSION AS OF 'task2_v1' | 21,224 | the Task 2 freeze tag still reads the frozen state |
| refs: main (branch) vs task2_v1 (tag) | different snapshots | main moved on (two backfills), the tag stayed |
| silver.chat_message at its first snapshot | 86, only 2024-07-09 | the table after the very first Silver run |
| silver.chat_message now | 21,224, 18 days | |

- A snapshot keeps the file list of that commit; time travel reads that list
- expire_snapshots (weekly, 7 days / keep ≥ 5) removes old snapshots, **except those held by a tag**: that is the freeze mechanism (task3_v1, 365 days)
- Tags after a schema change: Task 4 added step later; at task3_v1 StarRocks reads 10,729 rows with step NULL (current schema, columns matched by field id). Spark and Trino may use the snapshot's own schema for time travel: check each engine before relying on it
- make freeze reports "created (or already present: kept where it was)": a rerun never moves a tag

Visual: a timeline of snapshots; a flag "task2_v1" pinned to one, "main" pointer at the end.

Speaker notes: Tags are why the presentation can show exactly the frozen state of each task even after more runs.

---
# Slide 19 — Statistics and the read-only proof

| SHOW STATS FOR lake.silver.chat_message | Value |
|---|---|
| row count | 21,224 |
| distinct values | empty (that is what ANALYZE would add) |
| null fraction | text 0.00075 (≈ 16 rows); session_id 1.0 (empty in every row: a data fact) |
| event_ts min / max | 2024-07-02 15:39:17.676687 / 2024-07-25 11:01:53.468563: not shifted (D8) |

- ANALYZE → "Access Denied: Cannot ANALYZE (missing insert privilege)"
- Row count, null fraction, min/max come from Iceberg manifests at no cost

Speaker notes: The empty distinct counts are the visible cost of read-only Trino, and an accepted one. The min and max timestamps are the visible proof that NTZ worked.

---
# Slide 20 — Three independent computations: 191 / 191
- pandas on the Excel files → tests/expected/silver.json + silver_daily.json (per day)
- Spark: validate_silver, 28 checks per day (rules in tests/sql/silver_rules.sql, e.g. id_unique_across_table, event_date_is_date_of_event_ts, rows_equal_bronze, final_intent_follows_rule, user_message.one_row_per_message; every rule expects 0, counts only)
- Trino: make trino-check and DAG trino_reconcile recompute 191 values (totals + per day) → 191/191
- Backfill 07-02 → 07-25 twice: 155 per-day lines identical between passes (idempotency)
- Freeze task3_v1: rows 21,224 / 10,729 / 21,224 / 10,729 / 10,613 = expected, read by Spark and again by Trino
- In Airflow: trino_reconcile 191/191 in 26 s (11-10 03:58 UTC, Trino ready in 15 s), Trino back to 0; after Task 4 added step: 210/210

Visual: three columns pandas · Spark · Trino, each with a tick, joined by "=" signs.

Speaker notes: Three engines that share no code agreeing on 191 numbers is strong evidence. Any one alone could share a bug with its own expected values.

---
# Slide L1 — Lifecycle: the phases Task 3 added (13–19)
Phases 1–12 from Task 2 (schedule → driver/executors → commit → validation → maintenance) are unchanged.

| Phase | What happens | Must be true | Typical failure |
|---|---|---|---|
| 13 Silver read | silver reads both Bronze tables for day D | same _batch_id in both | mixed batch → fail, rerun Bronze |
| 14 Silver transform | type, flag, dedup, final_intent with the row's threshold | every row parses; one prediction per chat ID; all rows event_date = D | null event_ts, wrong-date row or duplicate prediction → job refuses |
| 15 Silver write | three overwritePartitions() commits | each commit succeeds | one table committed, another not (no cross-table transaction) |
| 16 Silver validation | 28 rules + exact counts vs silver_daily.json | reads the intended snapshot | mismatch → fail, no asset event |
| 17 Asset event | validate_silver success → LAKE_SILVER | task succeeded | no event → no reconcile run (silent by design) |
| 18 Reconcile | wait idle → Trino 0→1 → SQL → compare → 1→0 → verdict (26 s, Trino ready in 15 s) | RBAC; Trino ready; HMS + MinIO reachable | mismatch fails reconcile; trino_down still runs; verdict fails the run |
| 19 Freeze (manual) | tag 5 tables, copy evidence out | tags point to validated snapshots | tag on the wrong state: it happened with the git tag task3-v1 (first on ba7dde4, re-tagged on c3f3fef); an Iceberg tag rerun never moves |

---
# Slide L2 — What Task 3 tested, and what it didn't
Tested, with evidence: three computations agree (191/191) · backfill twice = identical data lines (155 = 155) · event_ts is timestamp_ntz in all 3 tables · Trino write and ANALYZE denied, access test 6/6 · Spark and Trino name the same metadata.json · tags read back through Trino · reconcile run created by the asset · E64 and E66 diagnosed and fixed · 417/417 evidence files copied · 18 Silver updates → 18 reconcile runs · E67 hidden failures found and fixed (proven 09:06–09:11 UTC) · tag vs schema change read from StarRocks · TTL after sleep (E68) nudged

| Not tested yet | Why it matters |
|---|---|
| mixed _batch_id on the cluster (only unit-tested) | the detection gate has never fired for real |
| late or corrected row for a past day | overwritePartitions() would replace the day; the refusal path is the only guard |
| reconcile failing on a wrong expected value | no run has yet failed through verdict |
| Trino dies mid-reconcile | trino_down with all_done unproven |
| roll back Silver to a tag | freeze proven, restore not |
| failed backup pg-backup-29903100 (11-09 01:00 UTC slot) | cause not in the log; no alert seen: a failed backup may be silent |

Five drills worth doing first: (1) wrong expected value → verdict fails, Trino still 0; (2) mixed _batch_id → Silver refuses; (3) corrected export for a past day: rebuild vs MERGE; (4) roll back one Silver table to task3_v1; (5) find why pg-backup-29903100 failed and decide what alerts on a failed backup.

Speaker notes: Listing what I did not test is part of the result: the leader can judge the gaps, and each has a concrete drill.

---
# Part 4 — Errors

---
# Slide 21 — Errors and fixes (E63–E66)

| Error | Symptom | Root cause | Lesson |
|---|---|---|---|
| E63 | backfill twice: "DIFFERENT per-day lines" although 0 failed runs | the diff compared log lines with timestamps and a growing file count | compare data, not logs |
| E64 | ⏰ deadline alert; check_landing 42 min, run failed | laptop slept: an Airflow 3 task's short-lived JWT expired while the process was frozen | sleep breaks clocks; retry the cheap sensor once |
| E65 | CI: ModuleNotFoundError lake_assets | Airflow 3 DAG bundles are not on sys.path; shared code belongs in plugins/ | the Airflow image is built in 3 places: change all three; CI caught it |
| E66 | pg-backup slot never ran, no events | the CronJob controller waits on a monotonic clock that does not advance during suspend | make k8s-up nudges the CronJob; slot moved to 06:30 UTC (13:30 ICT) |

Speaker notes: Two of the four are about the laptop sleeping. Production servers don't sleep, but the lesson generalises: anything time-based must survive a paused process.

---
# Slide 21b — Two more Task 3 parts, found during Task 4 (E67, E68)

| Error | What happened | Fix | Lesson |
|---|---|---|---|
| E67 | 11-10 08:08–08:43 UTC: a backfill added step to all 24 days; each validate_silver fired the asset; 18 reconcile runs compared half-updated Silver with the new expected files (192/228 equal, rising to 208/212): 17 Telegram ❌ for reconcile, yet airflow dags list-runs showed every run **success**: the only leaf was trino_down with all_done | wait_pipeline_idle also waits for no running or queued chatbot_bronze run; a final verdict task fails the run when reconcile failed (PR #79); proven 09:06–09:11 UTC: the wait saw 1 SparkApplication + 1 queued run, then 0; reconcile 210/210; verdict passed | a DAG run's state comes from its leaf tasks: a cleanup task must never be the only leaf (Airflow's built-in answer: setup/teardown) |
| E68 | catalog-081805 (timeToLiveSeconds 86400) still present 25 h after it finished | an annotation (update event) made the operator delete it within 20 s; make k8s-up now nudges every finished SparkApplication | same class as E66: controllers re-check after a delay that stalls during laptop sleep (inferred from E66, not from the operator source) |

Speaker notes: E67 is the most important lesson of Task 3: a green run hid 18 failed checks. It was found by reading the log, not by an alert.

---
# Slide 22 — Debugging story: E66, the backup that never ran
| Step | Evidence |
|---|---|
| Symptom | lastScheduleTime 2026-11-09 01:00 UTC; the 11-10 slot missing although the cluster had been up since 01:06 UTC, inside the 12 h starting deadline |
| describe | no events at all |
| controller log | no CronJob line in 12 h |
| Theory | after a run the controller requeues the CronJob "after N hours" (time to the next slot), measured on the monotonic clock, which does not advance while the WSL VM sleeps: every hour of sleep moves the wake-up later, so the slot was not yet due by the controller's clock |
| Proof of the cause | kubectl annotate cronjob pg-backup (an update event) → within 20 s the controller created the missed 01:00 UTC job, Complete in 4 s |
| Fix | make k8s-up annotates the CronJob every time; slot moved to 06:30 UTC (13:30 ICT), when the laptop is usually awake |
| Proof | pg-backup-29904870 ran at 06:30:12 UTC on 11-10 |

Speaker notes: When nothing logs, the cause is often something that never ran. The fix is to give the controller a reason to look again.

---
# Slide 23 — Production gaps and next steps
- Not done in Task 3: from-zero acceptance (once, after Task 5); MERGE demo (knowledge only); ANALYZE (by design)
- Trino single node, ad hoc only (no fault-tolerant execution); HMS still without auth (Trino trusts the network)
- Leader questions: Silver placement (D6), source zone of CREATED_TIME, NLU_THRESHOLD semantics, Trino's role in production
- Next: Task 4, StarRocks + dbt Gold, linked by the same asset

Speaker notes: The pattern of Task 3, one engine writes and others read the same Iceberg tables, is what Task 4 builds on: StarRocks reads Silver the same way, then stores Gold for serving.

---
# Appendix A1 — Versions

| Component | Version | Why |
|---|---|---|
| Trino | 483, trinodb/trino@sha256:db58cc93…0534 | the lab's version; digest = no drift |
| Trino chart | 1.44.0 (appVersion 483), SHA-256 checked | newest chart on 483 |
| Silver job | vnpt-lake-spark-job (Spark 3.5.9, Iceberg 1.11.0) | one image for all Spark jobs |
| Airflow | 3.3.2, no Trino provider | REST with the standard library |

---
# Appendix A2 — How to reproduce
- make k8s-up · make trino-chart && make mode-query && make trino-install (once)
- make trino-check (191/191) · make trino-explore (plans, metadata, time travel) · make mode-pipeline
- make sparkapp APP=silver|validate_silver DATE=2024-07-24
- Freeze: make freeze TASK=3 → make evidence TASK=3 → make demo-task3 → make demo-check DEMO_TASK=3
- Docs: docs/task3/README.md, 01_knowledge.md, 06_validation.md, 07_explain_plans.md, 05_errors_and_fixes.md

Speaker notes: Everything in this deck is reproducible from the repository at tag task3-v1.

---
# Appendix A3 — Sources (the four Task 3 learning packs, PDFs with links, kept locally in docs/task3/: not in Git)
- **Official docs:** Trino concepts · resource management properties · table statistics · Iceberg connector iceberg.security values · Hive connector security (metastore auth defaults to NONE) · Iceberg Spark writes (dynamic overwrite vs MERGE) · Iceberg branching and tagging · cross-engine timezone handling (Cloudera) · Airflow asset-aware scheduling (3.3.2) · Task SDK concepts (heartbeats, token renewal) · modules management (sys.path) · Kubernetes CronJob
- **Papers:** Presto: SQL on Everything (ICDE 2019: connector API, lazy split enumeration, pipelined execution, why a worker failure fails a query) · Spark SQL / Catalyst (SIGMOD 2015) · Lakehouse (CIDR 2021). The Medallion pattern has no founding paper: it comes from vendor practice
- **Talks:** Trino Community Broadcast 14 (why HMS for Iceberg, ~1:18:39), 38 (with Ryan Blue), 40 (Iceberg connector + DML) · Trino Summit 2022 and 2024 · Iceberg tags and branches (Subsurface 2023)
- **Problems (58 leads, 13 problems):** timestamps and zones (D8) · overwrite vs MERGE, late data, dedup (D7) · Trino read path, pruning, statistics · Trino security and sizing · reconciliation (E63) · asset orchestration · E64 (Airflow #67939, PR #67944, #64826) · E65 (discussion #61901) · E66 (Kubernetes #45825) · tags and freeze · Spark event logs · E67 (trigger rules, cleanup leaf) · E68 (SparkApplication TTL)
- **Basics (32 sources), reading order:** Medallion/Silver → Trino → Trino + Iceberg → Airflow assets
- Reading-list rows were picked from search summaries, not read in full: treat them as leads

Speaker notes: The packs are the bibliography of this deck: every claim here comes from the repo docs or the terminal log, and the packs point to the primary sources behind them.
