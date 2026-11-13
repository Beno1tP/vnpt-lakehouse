# Slide 1 — Task 4: StarRocks Gold layer built by dbt
VNPT AI · Data Engineering onboarding lab · Task 4 of 5
An OLAP engine that reads Silver in place and serves pre-aggregated Gold, modelled and tested by dbt
Bee (Pham Quang Minh) · 2026-11-12

Visual: dark title slide; small orange line "VNPT AI · DE onboarding lab · Task 4".

Speaker notes: Tasks 1–3 built a lake that is correct and open: Bronze and Silver in Iceberg, written by Spark, read by Trino. Task 4 adds the serving side. StarRocks reads Silver in place and stores Gold, the business answers, in its own columnar storage so a dashboard gets them in milliseconds. dbt turns the SQL into a tested, documented project. Every Gold number is reconciled against values computed independently from the Excel files.

---
# Slide 2 — What Task 4 does with the data
In one sentence: when Silver changes, Airflow runs dbt inside StarRocks; dbt reads the Silver Iceberg tables through an external catalog and builds a star schema plus one pre-aggregated table per use case (UC4–UC8), each reconciled with pandas.

Lab target flow: Silver (Iceberg) → StarRocks (OLAP) → SQL aggregation → Gold tables in StarRocks

What Task 4 implements:
- StarRocks 3.5.21 on Minikube (operator + 1 FE + 1 BE), external catalog lake_iceberg = the Silver tables, no copy
- dbt project: stg views → dim_date / dim_intent / dim_sender → fct_prediction / fct_user_message → gold_uc4 … gold_uc8
- Use cases defined first (docs/metrics.md): UC4 scenarios, UC5 understood, UC6 intents, UC7 total users, UC8 new/active users
- Expected values from pandas → dbt seeds → reconcile tests; dbt build PASS=56; make gold-check 22/22
- DAG gold_build on the asset lake_silver → produces asset lake_gold
- Freeze: Iceberg tag task4_v1 on 5 tables; Gold rebuilt from that tag and reconciled

Agenda: Knowledge (OLAP, StarRocks architecture, table models, bitmap, external catalogs, dbt, Kimball) · The stack · Running it (plans, bitmap proof, tags, rebuild from a tag, modes) · Errors · Next

Visual: Silver Iceberg boxes on MinIO → "lake_iceberg (external catalog)" → StarRocks box (FE + BE) with dbt layers stacked inside: stg → dim/fct → gold_uc4…8; an Airflow box "gold_build (asset lake_silver)" above.

Speaker notes: The key design point: Silver stays in the lake, Gold lives in StarRocks. Gold is small, pre-aggregated and rebuilt from the lake, so it never becomes a second source of truth.

---
# Slide 3 — The problem
- The dashboard (Task 5) asks the same questions many times: per day, per scenario, per intent, distinct users over any date range
- Querying Silver for every chart means scanning and grouping all files on every refresh (Trino explores; it is not a dashboard server)
- Requirements:
  - Fast: a chart reads a few pre-computed rows
  - Correct: every Gold number = an independent computation from the Excel files
  - Exact distinct users over any range (distinct counts are not additive)
  - Defined: UC wording turned into precise metrics before any SQL
  - Rebuildable: Gold can be recreated from a frozen Silver tag
  - Automated: Gold rebuilds when Silver changes, without the two engines fighting for RAM

Visual: left a dashboard with 10 charts hammering "Silver: 10,729 rows × every refresh"; right the same dashboard reading "gold_uc5: 9 rows".

Speaker notes: The honest scale note: with 10k rows any engine is instant. The lab builds the pattern that still works with a billion rows: pre-aggregate once, serve many times.

---
# Part 0 — Global view (from the Task 4 learning pack, docs/task4/Fundamentals.pdf)

---
# Slide G1 — Three maps for Task 4
- **Data flow:** Silver (3 Iceberg tables, now with step) is read by StarRocks through lake_iceberg; dbt builds Gold inside StarRocks in four layers: (1) staging stg_* (views, or inlined CTEs when building from a tag) → (2) dimensions dim_date (24), dim_intent (3), dim_sender (89, stable integer keys) → (3) facts fct_prediction (10,729), fct_user_message (10,613) → (4) marts gold_uc4…uc8 (36 / 36 / 57 / 48 / 48 rows)
- **Lineage (dbt docs, 11-11):** 3 sources → 13 models → 34 tests, plus 6 seeds (5 expected_gold_uc* + scenario_labels); sources use 3-part names, so the planned raw.* view layer was never needed
- **Orchestration:** validate_silver emits lake_silver, which starts two DAGs: trino_reconcile (skips in gold mode) and gold_build (sensor up to 30 min → dbt build in a pod in gold-jobs → verdict → lake_gold). Modes are the human step; a DAG never scales the other engine, it waits or skips; demo profiles pause both asset DAGs
- **RAM:** StarRocks (FE 1 Gi + BE 1 Gi requested) cannot sit next to Spark or Trino on 7,268 Mi: gold mode + dbt pod = 6,212 Mi (85 %); demo-task4 = 5,092 Mi (70 %)

Visual: Airflow column (chatbot_bronze → lake_silver → trino_reconcile / gold_build) · dbt pod in gold-jobs (MySQL 9030 as dbt_rw) · StarRocks box (FE plans, catalog lake_iceberg; BE Gold tables on a PVC) · lake box (MinIO 9000 + HMS 9083); "BE scans Silver as user starrocks (read-only)".

Speaker notes: Only gold_build talks to StarRocks, through a dbt pod; StarRocks reads Silver from the same files Spark wrote.

---
# Slide G2 — Durable state: what Task 4 added

| State | Where | Survives | Rebuilt from |
|---|---|---|---|
| Gold tables (13 models) | StarRocks BE storage on a PVC (5 Gi) | scale to 0 (proven: 7 h at 0, TabletNum 90 after) | the 5 tags + the dbt image digest: make dbt-tag-rebuild TAG=task4_v1, about 10 s |
| dim_sender keys (1…89) | same Gold table, incremental | same | **not safely rebuildable**: a rebuild from empty assigns keys again; old bitmaps are right only if the order matches |
| catalog, database gold, users, grants | FE metadata on its PVC | scale to 0 | make starrocks-init (idempotent Job) |
| Iceberg tags task4_v1 (5 tables, 365 days) | each table's metadata | snapshot expiry (drill 11-12) | — |
| expected Gold values (5 seeds) | dbt image + Git | image rebuild from the git tag | tools/expected_gold.py |
| freeze record FREEZE.md | Git | everything but losing the repo | — |
| evidence (180 MB, 648/648 files, 284 event logs) | ~/data/vnpt-lab/evidence/task4-20261112/ | minikube delete | — |
| StarRocks secrets | secrets.env → K8s Secrets | laptop disk | make k8s-secrets |

- **Not in any backup:** pg-backup dumps Postgres only. A StarRocks BACKUP SNAPSHOT would need a repository on MinIO (bucket, user, policy) for tables rebuilt in about 10 s, so Gold is frozen as inputs + code, except the dim_sender keys (a tag rebuild reuses the incremental table, so they stay)

Speaker notes: The honest gap is the surrogate keys: everything else in Gold is reproducible, those only as long as the incremental table survives.

---
# Slide G3 — Is the Task 4 pipeline standard? Hop by hop (1/2)
- **The core is standard:** StarRocks reading Iceberg through an external catalog while dbt builds marts inside it; one DAG per layer linked by assets; dbt as a pod from Airflow (keeps dbt out of the Airflow image)
- **Lab-specific, and defensible:** modes switched by hand (in production the engines run on separate nodes); one FE, one BE, replication_num 1 (no failover: a lost BE disk loses Gold until the next dbt build); expected values as seeds (works because the dataset is fixed; live data would use row-count and freshness checks)

| # | Hop | Connection | Failure handling |
|---|---|---|---|
| 1 | validate_silver → gold_build run | asset event lake_silver; the scheduler creates the run | no event, no run; events queued while a run waits merge into the next run |
| 2 | starrocks_ready sensor | K8s API: FE/BE Ready condition + lastTransitionTime | Role airflow-starrocks-reader; reschedule mode; both Ready ≥ 120 s (E76); after 30 min fails with "make mode-gold" |
| 3 | dbt_build → pod | KubernetesPodOperator in gold-jobs, image {{ var.value.dbt_image }} | Role airflow-gold-runner; SA dbt without a token; 512 Mi request = limit; retries 1, delay 2 min; failed pod kept for its log |
| 4 | dbt → StarRocks FE | MySQL protocol 9030 as dbt_rw | Secret via env_var(); an error fails the model, downstream skipped |
| 5 | on-run-start REFRESH | REFRESH EXTERNAL TABLE on 3 Silver tables | its own privilege (E72); without it the FE may answer from cached, older metadata |

---
# Slide G4 — Is the Task 4 pipeline standard? Hop by hop (2/2)

| # | Hop | Connection | Failure handling |
|---|---|---|---|
| 6 | StarRocks → HMS → MinIO | FE asks HMS for metadata_location; BE reads Parquet as user starrocks | read-only MinIO policy (StarRocks can also write Iceberg); no stats: EXPLAIN guessed 7,153 rows, all 10,729 are read; without the policy: denied (E70) |
| 7 | Gold tables written | CTAS/INSERT into internal tables: PK facts, day partitions, 1 bucket, 1 replica | BE alive **and** not blacklisted: "Backend node not found … inBlacklist: true" right after start (E76); the one retry absorbs it |
| 8 | dbt tests | generic, reconcile_uc4…uc8 vs seeds, assert_silver_counts, assert_bitmap_users | a failed test skips downstream models, but not through ephemeral parents (E78): the tag rebuild runs the gate as its own step |
| 9 | verdict → lake_gold | leaf task; its outlet records the event | run fails if dbt failed (the E67 lesson, applied from the start) |
| 10 | BI reads Gold (Task 5) | 9030 as bi_ro | SELECT on gold; CREATE and INSERT denied (tested by starrocks-init) |
| — | Gold freeze | make freeze TASK=4 (Iceberg tags) + dbt-tag-rebuild | gate first, build only if it passes (E78) |

---
# Part 1 — Knowledge

---
# Slide 4 — OLAP vs a query engine on the lake

| | Trino (Task 3) | StarRocks (Task 4) |
|---|---|---|
| Data | reads files where they live | own columnar storage (+ can read the lake) |
| Speed source | pruning, pipelined execution | sort keys, pre-aggregated tables, vectorized C++ engine, caches |
| Best for | ad hoc exploration across sources | serving: many users, same questions, milliseconds |
| Writes | none here (read-only) | Gold tables (internal) |

- Both read the same Iceberg Silver through the same Hive Metastore: one lake, two roles
- Gold in StarRocks is derived and rebuildable; the source of truth stays in Iceberg

Visual: the lake in the middle; Trino on the left labelled "explore", StarRocks on the right labelled "serve", both arrows into the same Silver tables.

Speaker notes: This split, a lake for truth and an OLAP store for serving, is the standard lakehouse serving layer. StarRocks could also query the lake directly; Gold exists for speed and for stable, tested numbers.

---
# Slide 5 — StarRocks architecture

| Part | What it does |
|---|---|
| FE (Frontend, Java) | SQL parsing, cost-based planning, metadata, cluster management; leader + followers replicate metadata (BDB-JE), observers scale reads; port 9030 = MySQL protocol, 8030 HTTP |
| BE (Backend, C++) | stores data and executes queries: columnar storage, vectorized (SIMD) execution, MPP pipeline across BEs |
| CN (Compute Node) | stateless compute for the lake / shared-data mode (not used here) |
| Modes | shared-nothing: BEs keep data on local disk (ours) · shared-data: data in S3, CNs cache it |
| Protocol | MySQL wire protocol: any MySQL client works (mysql CLI, pymysql, dbt-starrocks, Superset) |

- Memory: FE = JVM heap (the shipped fe.conf has -Xmx8192m: an 8 GiB heap in a 1.5 Gi pod would be OOMKilled → -Xmx1024m); BE = no heap, mem_limit caps its own allocator (ours 1600M of a 2 Gi limit)
- On Kubernetes: the StarRocks operator turns one StarRocksCluster resource into FE/BE StatefulSets

Visual: clients (dbt, Superset, mysql) → FE :9030 (parser, planner, catalog) → BE (tablets on disk, vectorized operators); a dashed CN box greyed.

Speaker notes: Using the MySQL protocol is a big practical advantage: every BI tool and driver already speaks it. The memory settings were the first thing to fix: defaults assume a server, not a pod.

---
# Slide 6 — Table design: models, partitions, tablets

| Concept | Meaning | Here |
|---|---|---|
| Duplicate key | keep every row; key = sort order | gold_uc4/5/6/7 |
| Aggregate key | same key → values merged (SUM, MAX, BITMAP_UNION…) | not used (no dbt type) |
| Primary key | upsert by key; any column type as value | fct_* (day partitions), dim_sender, gold_uc8 |
| Partition | range by date → pruning, per-partition replace | event_date |
| Bucket / tablet | DISTRIBUTED BY HASH(col) BUCKETS n → tablets | 1 bucket: ~10k rows; more = tiny tablets |
| Replicas | default 3; with 1 BE → replication_num 1 or CREATE fails | 1 |

- Primary Key tables keep a primary index in memory and support real-time upserts; Duplicate tables are the cheapest for append-only aggregates

Visual: a table split into partitions (days) → each into tablets (buckets) → each tablet a columnar segment with a sort key.

Speaker notes: The table model decides what a column can be: a BITMAP column must be either aggregated (Aggregate table) or a plain value of a Primary Key table. That rule became E75.

---
# Slide 7 — Exact distinct users: bitmaps
- Distinct counts are not additive: users(Mon) + users(Tue) ≠ users(Mon–Tue): a person active on both days is counted twice
- Bitmap (Roaring): one bit per user id; per day store a bitmap of the users active that day
- BITMAP_UNION_COUNT(users) over any set of days = exact distinct users over that range
- Needs an integer key: sender ids are text (2 are e-mails) → dim_sender gives a stable integer sender_key
- Stable = incremental model: existing senders keep their key; new ones get max(key) + row_number(). Re-numbering all senders per run would make old bitmaps point at the wrong people
- Alternatives: COUNT(DISTINCT) on the fact (fine at 89 users, not at scale), HLL (approximate, ~1–2 % error), hashing ids (can collide)

Visual: three day-bitmaps (Mon 1011…, Tue 0110…, Wed 1001…) → OR → union bitmap → popcount = exact users.

Speaker notes: This is the one Gold design choice that matters at scale: pre-aggregated tables stay exact for distinct users only with a mergeable structure. I measured the difference in Part 3.

---
# Slide 8 — Reading the lake: the external catalog
- CREATE EXTERNAL CATALOG lake_iceberg (type iceberg, catalog type hive, hive.metastore.uris = thrift://hive-metastore…:9083, aws.s3.* = MinIO, path-style)
- Names are 3-part: catalog.database.table; default_catalog = internal tables (Gold)
- MinIO user starrocks is read-only on lakehouse/*: StarRocks can also write Iceberg, so the S3 user must forbid it (same reason as Trino)
- Iceberg timestamp (NTZ, Task 3 D8) → StarRocks DATETIME: no zone shift
- The FE caches Iceberg metadata: after a Silver write a query may see the previous snapshot → dbt runs REFRESH EXTERNAL TABLE first (a separate privilege: E72)
- Created by an init Job (keys from a Secret), never typed in a shell

Visual: StarRocks FE → HMS ("metadata_location") → MinIO (metadata.json → manifests → Parquet), the same arrows as Trino in Task 3; a small "cache" box on the FE with a "REFRESH" button.

Speaker notes: Spark wrote these tables, Trino read them in Task 3, StarRocks reads them now: three engines, one copy, one pointer in HMS.

---
# Slide 9 — dbt: SQL as a tested project

| Concept | Meaning |
|---|---|
| Model | a SELECT in a .sql file; dbt wraps it in CREATE TABLE / VIEW AS (materialization) |
| ref() / source() | dependencies → a DAG and a lineage graph |
| Materializations | view, table, incremental (unique_key), ephemeral (inlined CTE) |
| Tests | generic (unique, not_null, accepted_values, relationships) + singular SQL that returns failing rows |
| Seeds | small CSVs → tables: our expected values, so reconciliation is a normal test |
| dbt build | seeds + models + tests in DAG order; a failing test skips the models downstream of it |
| Docs | dbt docs generate → lineage graph and column docs |

- dbt-starrocks adds per-model table_type (DUPLICATE / PRIMARY), keys, partition_by (a list: E73), distributed_by, buckets, properties
- Profile: password from env_var('STARROCKS_PASSWORD'), user dbt_rw; never a literal

Visual: lineage graph sources → stg → dim/fct → gold, with a seed box and a test icon on each gold node.

Speaker notes: dbt adds nothing to SQL's power; it adds engineering: dependencies, tests, docs, and one command that builds in the right order and stops on a red test.

---
# Slide 10 — Kimball in 4 steps, and one mart per use case
1. Business process: a citizen messages the bot; the bot predicts an intent
2. Grain: one client message (fct_user_message) and one prediction (fct_prediction)
3. Dimensions: dim_date (24 days, incl. days without messages), dim_sender (stable sender_key, user_class internal/external), dim_intent (intent, group, is_understood)
4. Facts: counts (additive), confidence (non-additive → average), flags

| Mart | Grain | Measures | Filter column |
|---|---|---|---|
| gold_uc4 | day × scenario (step) × traffic_class | message_cnt | traffic_class |
| gold_uc5 | day × traffic_class | understood_cnt, not_understood_cnt | traffic_class |
| gold_uc6 | day × final_intent × intent_group × traffic_class | message_cnt | traffic_class |
| gold_uc7 | day × user_class | cumulative_users, users_30d_before, change_abs, change_pct | user_class |
| gold_uc8 | day × user_class | new_users, active_users, users (BITMAP) | user_class |

- traffic_class = internal > unmatched > home > real (D20): one dashboard filter instead of three booleans; counts real 219, home 10,378, internal 16, unmatched 116
- Two facts at two grains: UC4–UC6 count predictions, UC7–UC8 count users; measures: counts additive, confidence not (average), users not (bitmap)

Visual: a star: fct_user_message in the middle, three dimensions around; five small mart boxes to the right.

Speaker notes: Keeping the filter column inside every mart is what lets the dashboard filter pre-aggregated tables without going back to the facts. That decision paid off directly in Task 5.

---
# Slide 11 — Metrics defined before SQL (docs/metrics.md)
- Base of UC4–UC6: one predicted message = one row of silver.nlu_prediction: 10,729 (incl. the 116 without a chat row)
- Not understood = final_intent in (fallback, bot_khong_hieu); understood = every other intent
- Scenario (UC4) = predict STEP: 406665 start (/home) 10,494 · 406666 free text (NLU) 235 (labels inferred from the data: a leader question)
- User (UC7/UC8) = a distinct sender of a client message

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| M1 the 116 orphan predictions | traffic_class unmatched | count them as real | their text is unknown |
| M2 UC7 vs 30 days earlier with 24 days of data | users(D) − users(D−30); % = NULL on a zero base | an invented 100 % | honest arithmetic |
| M3 UC8 averages | over 24 calendar days, empty days = 0 | only the 18 days with data | a day without rows had no messages |
| M4 user filter | user_class internal / external | "real users" by message | ~98 % of messages are /home |

Speaker notes: Writing the definitions first turned vague wording ("so với 30 ngày trước") into testable numbers. Each M decision is documented with its alternative so the leader can overrule it.

---
# Slide 12 — Orchestration: one DAG per layer, linked by assets
- validate_silver → asset lake_silver → gold_build: starrocks_ready (sensor, reschedule, FE+BE ready ≥ 120 s, 30 min timeout) → dbt_build (KubernetesPodOperator, dbt image, retries 1) → verdict → asset lake_gold
- dbt runs as a pod in namespace gold-jobs, with only the dbt_rw Secret and a ServiceAccount without a token
- The laptop cannot run Spark and StarRocks together → gold mode: the event waits until StarRocks is up; DAGs never scale the other engine
- trino_reconcile (Task 3) skips in gold mode

Visual: chatbot_bronze → (lake_silver) → gold_build → (lake_gold) → "Task 5 dashboard"; a mode switch icon between Spark and StarRocks.

Speaker notes: In production both engines would run all the time and the asset would trigger Gold immediately. The pattern is the same; only the wait differs.

---
# Slide C1 — The stack compared by criteria: identity and mechanism

| Tech | Kind | Basic unit | Core mechanism | Function here |
|---|---|---|---|---|
| StarRocks FE | Java frontend | query plan; catalog metadata | parses SQL, cost-based plan, metadata (BDB-JE), MySQL protocol 9030 | plans dbt's SQL; holds lake_iceberg, gold, users |
| StarRocks BE | C++ backend | tablet (a bucket of a partition) | columnar storage + vectorized MPP execution | stores Gold; scans Iceberg Parquet from MinIO |
| Table types | storage model | row by key | Duplicate keeps all; Aggregate merges; Primary Key upserts | facts, dim_sender, gold_uc8 as Primary Key |
| Bitmap (Roaring) | exact set type | one set of integer IDs | to_bitmap per row, bitmap_union across days, bitmap_union_count | 89 exact vs 112 summed |
| External catalog | federation | catalog.database.table | reads HMS + Iceberg metadata, caches it, scans files | Silver without copying it |
| dbt / dbt-starrocks | SQL build tool / adapter | model (one SELECT) / model config | compiles Jinja, orders by ref(), materializes, tests / maps table_type, keys, partition_by to DDL | builds and tests Gold |
| KubernetesPodOperator | Airflow operator | one pod per task try | creates the pod, streams its log, reads its exit code | dbt build outside the Airflow image |
| Kimball | design method | grain | process → grain → dimensions → facts | 2 facts, 3 dims, 5 marts |
| StarRocks operator | K8s operator | StarRocksCluster CR | renders FE/BE StatefulSets; updates only when the spec hash changes (E69) | installs and owns FE/BE |

---
# Slide C2 — Knowledge levels and state

| Tech | Basic knowledge to own | Advanced (ours → production) | Evidence |
|---|---|---|---|
| StarRocks | FE vs BE, 9030, table types, partition + bucket | 1 FE + 1 BE, 1 replica → 3 FE (leader + followers), ≥ 3 BE, 3 replicas; or shared-data with CN on S3 | gold-check 22/22; EXPLAIN 1/1 partition |
| Bitmap | why distinct is non-additive; integer keys | stable sender_key → global dictionary, or HLL when ~1–2 % error is fine | 87 + 2 exact vs 106 + 6 summed |
| External catalog | metadata cache, REFRESH, 3-part names | fixed or event-driven refresh; per-catalog resource limits | 10,613 rows read as dbt_rw |
| dbt | ref/source, materializations, tests, seeds, build | Slim CI (state:modified+), artifacts upload, docs site, contracts | PASS=56; PASS=53 from the tag |
| KPO | pod lifecycle, RBAC, image by digest | Cosmos (one Airflow task per dbt model), quotas per namespace | dbt_build attempt 2 success after E76 |
| Kimball | grain first; additive vs non-additive | conformed dimensions, SCD type 2 for senders | 2 facts at different grains |
| Operator | CR vs StatefulSet, who reconciles what | GitOps on the CR only; no out-of-band scaling | E69 round trip proven |

Speaker notes: dbt's target/ artifacts are lost with the pod today; uploading them (Slim CI, docs site) is the first production step.

---
# Part 2 — The stack of Task 4

---
# Slide 13 — Architecture

| Namespace | Runs |
|---|---|
| starrocks | operator 1.11.8 + FE (1/1.5 Gi, -Xmx1024m) + BE (1/2 Gi, mem_limit 1600M); chart kube-starrocks 1.11.8 (SHA-256 checked); root Secret; init Job (catalog, database gold, users, grants, checks) |
| gold-jobs | dbt pods only (make dbt, DAG gold_build): dbt_rw Secret, SA without token |
| lake | MinIO user starrocks (read-only) |
| airflow | DAG gold_build; RBAC to read StarRocks pods and run pods in gold-jobs |

Users: dbt_rw (all on gold; SELECT + REFRESH on the catalog) · bi_ro (SELECT on gold: Task 5) · root only in the init Job.

Visual: the cluster with the two new namespaces; arrows from gold-jobs (dbt pod) to FE :9030, from FE to HMS and MinIO.

Speaker notes: dbt pods have their own namespace because the right to create a pod in a namespace is the right to read every Secret in it. In the starrocks namespace that would include root.

---
# Slide 14 — How the pieces talk

| From → To | How | Credentials |
|---|---|---|
| dbt pod → StarRocks FE | MySQL protocol :9030 | dbt_rw (Secret in gold-jobs) |
| FE → HMS | Thrift :9083 | — |
| FE/BE → MinIO | S3 (Iceberg scan) | user starrocks, read-only |
| init Job → FE | MySQL :9030 | root (Secret in starrocks only) |
| Airflow → K8s API | read StarRocks pods; create dbt pods | ServiceAccount + Roles |
| Superset (Task 5) → FE | MySQL :9030 | bi_ro |

Speaker notes: Three StarRocks users, one per role: root to set up, dbt_rw to build, bi_ro to read. Grants on gold use wildcards so they survive dbt dropping and recreating tables.

---
# Slide 15 — Why these, and not others

| Role | Ours | Alternatives | Why ours here |
|---|---|---|---|
| OLAP | StarRocks 3.5.21 | ClickHouse, Doris, Druid, Pinot | named by the lab; MySQL protocol; reads Iceberg natively |
| Gold build | dbt-core 1.12.6 + dbt-starrocks 1.12.2 | async materialized views, scheduled SQL | tests, seeds, lineage, CI parse |
| Deploy | operator + chart (pinned, digests) | plain StatefulSets | the official path; chart default 4.1-latest overridden |
| Rebuild strategy | full rebuild (table), only dim_sender incremental (D18) | incremental everywhere | small data; stable keys where they matter |
| Expected values | pandas → seeds (D19) | DuckDB | the same independent path as Tasks 1–3 |
| Distinct users | BITMAP in gold_uc8 | COUNT DISTINCT, HLL | exact and mergeable |

- Honest view: 10k rows need none of this for speed; the lab builds the serving pattern

Speaker notes: StarRocks async materialized views could replace the dbt marts and even rewrite queries automatically. dbt wins here for testing and documentation, and it is the tool the lab names.

---
# Slide 15b — Where each layer lives (Gold is not in MinIO)

| Layer | Physically | Format | Written by |
|---|---|---|---|
| Bronze | MinIO lakehouse/ bucket | Iceberg (Parquet + metadata), pointer in HMS | Spark |
| Silver | MinIO lakehouse/ bucket | Iceberg | Spark |
| **Gold** | StarRocks BE PersistentVolume (starrocks-be-0, 5 Gi) | StarRocks internal tables (columnar tablets), database gold | dbt inside StarRocks |

- Trade-off: only StarRocks can read Gold (not Trino or Spark); no Iceberg time travel on Gold (rebuild from a Silver tag instead); not in the MinIO evidence or pg-backup (covered by the rebuild procedure)

Speaker notes: Gold is derived and rebuildable; the source of truth stays in Iceberg.

---
# Slide 15c — The lab doc says two things about Gold: which did we follow?

| Where in the lab doc | What it says |
|---|---|
| Architecture diagram | "dbt Gold (analytics)" **inside** the box "ICEBERG (MinIO S3)", then a dashed arrow to StarRocks → Trino |
| "Kiến trúc tổng thể" text | "dbt tạo lớp Gold phục vụ StarRocks, Trino và Superset": Trino can only read Gold if it is Iceberg |
| Tech stack table | StarRocks: "OLAP · Gold layer, bitmap aggregation"; dbt: "Quản lý Gold layer" |
| Bước 4 (the task) | "Ghi kết quả vào table dạng gold **trên StarRocks**" |

- We followed Bước 4 + the tech-stack row: Gold as StarRocks internal tables. That is also the only place the BITMAP exact-users column can live: **Iceberg has no BITMAP type**
- The diagram's reading (Gold in Iceberg on MinIO) is possible: dbt writes Gold into the Iceberg catalog (through StarRocks, or dbt on Trino/Spark), StarRocks keeps a materialized view for the bitmap metric; it needs write access for that writer and new checks/freeze steps. Not verified yet: whether dbt-starrocks 1.12 materializes into an external catalog
- **Question for the leader:** which reading is intended?

Visual: the lab doc's diagram on the left (Gold circled inside the Iceberg box), our layout on the right (Gold inside StarRocks).

Speaker notes: I show the conflict instead of hiding it: the build follows the concrete task text, and the alternative is costed.

---
# Slide 16 — Modes and memory (D15)

| Mode | Runs | Requests of 7,268 Mi |
|---|---|---|
| pipeline | Spark can run, StarRocks 0 | 4,036 Mi |
| gold | StarRocks 1, Airflow up, Spark cannot start | 6,212 Mi (85 %) + dbt pod 512 Mi while it runs |
| demo-task4 | view-only Airflow + StarRocks | 5,092 Mi (70 %), limits 115 % |

- FE 2 + BE 3 Gi always on would need 9,156 Mi: impossible → modes; BE request lowered 1.5 → 1 Gi after measuring 302 Mi idle
- Idle usage: FE 485 Mi, BE 302 Mi, operator 19 Mi
- The operator ignores replica changes made behind its back (E69): starrocks-up scales the StatefulSets itself and waits with rollout status + a real BE query (E76)

Speaker notes: Measuring before sizing saved half a gigabyte. The readiness lesson: "pod Ready" and "heartbeat alive" are not "a query works".

---
# Part 3 — Running it: what StarRocks showed

---
# Slide 17 — Gold table vs lake scan (EXPLAIN)

| Query | Scan | Plan facts | Rows expected |
|---|---|---|---|
| UC5 on gold.gold_uc5 WHERE traffic_class = 'real' | internal OLAP table | partitions 1/1, tablets 1/1 | **9** |
| same question on lake_iceberg.silver.nlu_prediction | Iceberg scan via HMS + MinIO | no partition/tablet facts | 7,153 estimated (no statistics: default selectivity), 10,729 read |

- Gold = aggregate once in dbt build; a dashboard query reads a few pre-computed rows from local columnar storage

Visual: two plan trees side by side: OlapScanNode (9 rows) vs IcebergScanNode (10,729 rows) → Aggregate.

Speaker notes: The estimate of 7,153 shows the other cost of external tables: no statistics, so the planner guesses. Internal tables have both pruning and statistics.

---
# Slide 18 — Bitmap proof: exact vs summed

| user_class | exact users (bitmap union, all days) | sum of daily distinct users | counted twice |
|---|---|---|---|
| external | **87** | 106 | 19 |
| internal | **2** | 6 | 4 |
| total | **89** = Task 1's distinct senders = gold_uc7 on 07-25 | 112 | 23 |

- 23 people were active on more than one day; a sum counts them once per day
- The same 89 appears on the dashboard (Task 5) through BITMAP_UNION_COUNT(users) as a dataset metric

Visual: a bar pair per class: exact (short) vs summed (taller), the difference labelled "double counted".

Speaker notes: This is the slide to remember: a wrong distinct count looks plausible. 112 users looks fine unless you know it should be 89.

---
# Slide 19 — Reconciliation: four engines, one number
- pandas on the Excel files → expected_gold_uc4…uc8 seeds (36 / 36 / 57 / 48 / 48 rows)
- dbt build: 13 models, 6 seeds, 34 tests incl. reconcile_uc4…uc8 (filter on and off) and assert_bitmap_users → PASS=56
- make gold-check: 22 counts and measure sums (e.g. UC5 10,588 understood / 141 not; UC8 89 new, 112 active, bitmap 89) → 22/22
- StarRocks reads Silver: 10,613 = Trino = Spark = pandas
- Least privilege: MinIO user starrocks 6/6 read-only; bi_ro CREATE / INSERT → privilege error; grants survive rebuilds (wildcards)
- The lab's recommended checks, run 11-13 07:41 UTC (14:41 ICT) on the frozen Gold: make all (lint + unit tests) 142 passed · dbt test PASS=37 · dbt test --select staging PASS=4 · --select gold PASS=9 · the intent tests (no test is named test_valid_intent; the same idea = accepted_values + relationships to dim_intent) PASS=15 · 0 failures, 0 errors. pytest tests/ in the lab = make test (tests/unit) + the CI job "DAG integrity" for tests/dags (needs Airflow)

Visual: four logos (pandas, Spark, Trino, StarRocks) pointing at "10,613".

Speaker notes: Reconciliation is a dbt test like any other, so a wrong Gold number stops the build and the downstream models. Except when the graph edge is missing: that is E78.

---
# Slide 20 — Time travel by tag, and rebuilding Gold from a tag
- StarRocks resolves an Iceberg tag: SELECT count(*) FROM lake_iceberg.silver.nlu_prediction FOR VERSION AS OF 'task3_v1' → 10,729
- Schema evolution: step (added in 4.0a) reads as NULL for old snapshots: columns are matched by field id
- Freeze = Silver tag task4_v1 + the dbt image digest: Gold is reproducible, not snapshotted. make freeze TASK=4 at 02:28:03 UTC 11-12 (job catalog-022803), checked through Spark and StarRocks (5 × PASS): bronze.chatlog 4851909217252046539 (21,224) · bronze.bot_predict 7994949553350528317 (10,729) · silver.chat_message 4324401880006836136 (21,224) · silver.nlu_prediction 6940182659003592517 (10,729, step filled) · silver.user_message 9106066062539116072 (10,613)
- Rebuild procedure (FREEZE.md §5): git checkout task4-v1 → make k8s-up → make mode-gold → make starrocks-tag-check TASK=4 → make dbt-tag-rebuild TAG=task4_v1 → make gold-check 22/22 → make dbt CMD="build" to return
- make dbt-tag-rebuild TAG=task4_v1: Silver gate on the tag first, then build from it → PASS=53
- Negative run on task3_v1: the gate fails (step missing) → the rebuild stops
- Tag vs maintenance: on a scratch table, expire_snapshots(retain_last = 1) removed the untagged snapshot and kept the tagged one

Visual: Silver snapshots on a timeline with tags task3_v1 and task4_v1; an arrow from task4_v1 into "dbt build (ephemeral staging)" → Gold.

Speaker notes: Freezing a derived layer means freezing its input and its code. The scratch-table drill proved tags survive cleanup without risking the real tables' history.

---
# Slide L1 — Lifecycle: the phases Task 4 added (20–27)
Phases 1–19 from Tasks 2–3 are unchanged.

| Phase | What happens | Must be true | Typical failure |
|---|---|---|---|
| 20 Gold mode (manual) | mode-gold: Trino + History → 0; starrocks-up; starrocks-ready waits for the revision and a BE query | no Spark app; RAM for FE + BE | operator never scales back (E69); BE blacklisted (E76) |
| 21 Gold run starts | lake_silver → gold_build; sensor wants FE + BE Ready ≥ 120 s | DAG unpaused; gold mode within 30 min | sensor timeout "make mode-gold" |
| 22 dbt pod | KPO in gold-jobs | image pull (ghcr-pull), RBAC, 512 Mi free | pull or scheduling failure; node at 85 % with the pod |
| 23 Refresh + Silver gate | REFRESH × 3; assert_silver_counts (14 numbers) | REFRESH privilege; Silver as expected | missing grant (E72); stale cache; wrong Silver → Gold skipped |
| 24 Models | staging → dims → facts → marts | valid DDL per table type; BE accepts queries | E73, E75, E76 |
| 25 Tests | reconcile vs seeds, bitmap check, generic | seeds loaded | mismatch → downstream skipped, except through ephemeral parents (E78) |
| 26 Verdict | verdict → lake_gold | dbt_build succeeded | no event: Task 5 consumers don't run |
| 27 Freeze + rebuild (manual) | freeze; rebuild = checkout → mode-gold → tag-check → dbt-tag-rebuild → gold-check → normal build | tags on validated snapshots; staging ephemeral | views skip and read current Silver (E77); gate bypassed (E78) |

---
# Slide L2 — What Task 4 tested, and what it didn't
Tested, with evidence: four engines, one number (10,613) · gold-check 22/22 + reconcile vs seeds · asset chain without a manual trigger (success 11-11 03:01:59 UTC) · infrastructure retry (attempt 1 hit the blacklisted BE, attempt 2 PASS=56; after the fix the sensor waited 108 s → 171 s and attempt 1 passed) · mode round trip (7 h at 0, 90 tablets after) · least privilege (bi_ro denied, s3-access-test 5 × 6/6, dbt_rw REFRESH but no more) · rebuild from the tag (PASS=53) and negative run on task3_v1 (gate fails; exposed E78) · tag vs snapshot expiry (scratch drill) · stable keys (second run added 0, kept every key) · demo-check DEMO_TASK=4 ALL PASS · 648/648 evidence files

| Not tested yet | Why it matters |
|---|---|
| dim_sender rebuilt from empty; a new sender arriving | bitmaps are right only if keys stay; key 90 is unproven |
| loss of the BE PVC or FE metadata | rebuild into an empty StarRocks unmeasured; would reassign keys |
| sensor timeout (not in gold mode) | the failure message path is in no log |
| Silver changes while in gold mode | trino_reconcile skips; what stops a daily Spark run next to StarRocks? |
| stale catalog cache without REFRESH | the reason for REFRESH was never shown |
| Silver updated during a gold_build run | which snapshot does Gold reflect? |
| airflow tasks clear -t "^silver$\|^validate_silver$" matched nothing | open since 11-11 |

Five drills worth doing first: (1) rebuild dim_sender from empty in a scratch database and compare keys + bitmap counts; (2) one fake sender in a scratch copy of Silver: key 90, UC8 counts 90; (3) write Silver, query without and with REFRESH; (4) trigger gold_build in pipeline mode and read the timeout message; (5) delete the BE pod during dbt_build: retry, same 22 values.

Speaker notes: The surrogate-key gap is the one I would close first, because it is the only part of Gold that the rebuild procedure does not fully cover.

---
# Part 4 — Errors

---
# Slide 21 — Errors and fixes (E67–E78, grouped)

| Theme | Errors | Lesson |
|---|---|---|
| Checks must gate | E67 reconcile mid-backfill; E74 merge ran after a red check; E78 test edge lost with ephemeral parents | a check sits at a consistent point and must stop what follows (chain with &&, explicit steps) |
| Laptop sleep | E68 TTL not applied after sleep | controllers' timers pause; make k8s-up nudges them |
| Operator model | E69 operator never restores replicas changed behind its back | know what the controller compares (a spec hash); wait for a revision, not a pod |
| Lists in two places | E70 policy file missing from an items: whitelist | a test for every list that must stay in sync |
| Object lifetime / negative tests | E71 a cursor outlived its connection; a "not connected" error counted as a denial | a denial counts only as Access denied |
| RBAC per operation | E72 REFRESH is its own privilege | grant what each operation needs, test it |
| Adapter + template syntax | E73 partition_by is a list; E74 no # inside {{ config() }} | read the adapter's types; parse before PR (make dbt-parse) |
| Table model rules | E75 BITMAP only in PK / Aggregate tables | test DDL in a scratch database first |
| Liveness ≠ readiness | E76 a just-started BE is blacklisted | readiness = a real query succeeds |
| A green run proves only what ran | E77 views silently dropped FOR VERSION AS OF | check the executed nodes, not the summary |

Speaker notes: Most of these were my wrong assumptions, caught by a check. Each one is in 05_errors_and_fixes.md with symptom, root cause, fix and theory.

---
# Slide 22 — Debugging story: E77, a green build that read the wrong data
| Step | Evidence |
|---|---|
| Goal | rebuild Gold from Silver tag task4_v1 (--vars silver_version) |
| Result | PASS=56, all reconcile tests green |
| Doubt | the 3 staging views printed "skip" although their SQL now had FOR VERSION AS OF 'task4_v1' |
| Look | dbt compile showed the clause; SHOW CREATE VIEW did not: StarRocks 3.5.21 drops it from a view's stored definition |
| Cause | dbt-starrocks skips a view whose normalized definition is unchanged → the views kept reading current Silver; PASS only because current = tag today |
| Fix | ephemeral staging in a tag rebuild: the clause is inlined into the table's own SQL; proof: compile shows it twice, PASS=53 |

Speaker notes: The lesson: a green summary proves nothing about nodes that were skipped. I now read the executed-node list, not just the totals.

---
# Slide 23 — Known limits and next steps
- Gold lives in StarRocks internal tables: reproducible from task4_v1 + the dbt image, not snapshotted
- A StarRocks view cannot pin an Iceberg tag (E77): tag rebuilds use ephemeral staging
- Scenario names for STEP are inferred: a leader question; M2/M3 also need the leader's OK
- Open: dbt artifacts to S3 (Should); Write-Audit-Publish with SWAP (Could)
- Next: Task 5, Superset on gold as bi_ro

Speaker notes: Task 5 only reads; everything it shows was defined, built and reconciled here.

---
# Appendix A1 — Versions

| Component | Pin | Why |
|---|---|---|
| StarRocks FE / BE | 3.5.21 by digest (fe-ubuntu @cbc2460a…, be-ubuntu @6ed9e75b…) | lab version; chart default 4.1-latest overridden and checked (starrocks-render) |
| Operator / chart | 1.11.8 / kube-starrocks 1.11.8 (SHA-256 c05804d0…) | newest; proven by rendering + running |
| dbt | dbt-core 1.12.6 + dbt-starrocks 1.12.2 on python:3.11-slim by digest | the adapter only sets a floor (dbt-core ≥ 1.9): CI dbt-lint proves the pair |

---
# Appendix A2 — How to reproduce
- make k8s-up · make starrocks-render && make starrocks-install (once) · make starrocks-init
- make mode-gold · make dbt CMD="build" (PASS=56) · make gold-check (22/22)
- make dbt-docs (lineage HTML, outside Git) · make dbt-tag-rebuild TAG=task4_v1 · make starrocks-tag-check TASK=4
- make demo-task4 · make demo-check DEMO_TASK=4 · make mode-pipeline (back to building)
- Docs: docs/task4/README.md, FREEZE.md, 01_knowledge.md, 06_validation.md, 07_dbt_and_plans.md, docs/metrics.md

Speaker notes: Everything here is reproducible from the repository at tag task4-v1.

---
# Appendix A3 — Sources (the four Task 4 learning packs, PDFs with links, kept locally in docs/task4/: not in Git)
- **Official docs:** StarRocks architecture · table types and their capabilities (E75) · Primary Key table · Iceberg catalog (metadata cache, time travel) · REFRESH EXTERNAL TABLE (E72) · bitmap for exact count distinct · BE blacklist (E76) · StarRocks Kubernetes Operator (E69) · dbt with StarRocks (E73) · dbt-starrocks on GitHub (E77) · dbt build (E78) · managing surrogate keys (dbt blog) · Cosmos Kubernetes execution mode · Kimball dimensional modeling techniques · Kubernetes projected volumes (E70)
- **Papers:** Better bitmap performance with Roaring bitmaps (Chambi, Lemire et al.) · MonetDB/X100: Hyper-Pipelining Query Execution (CIDR 2005), the origin of vectorized execution
- **Talks and courses:** StarRocks Query Optimizer (CMU Database Group seminar) · StarRocks on the Data Engineering Podcast ep. 463 · Zero to dbt (Coalesce) · dbt Learn
- **Problems (40 leads, 9 problems):** serving Gold over Iceberg (catalog, cache) · table design (E75) · exact distinct users · stable surrogate keys · dbt gates that really block (E74, E77, E78) · dbt from Airflow on Kubernetes · operators ignoring out-of-band changes (E69) · alive is not ready (E76) · E70, E73
- **Basics:** StarRocks → dbt → dimensional modeling (Kimball) → Airflow + dbt
- Reading-list rows were picked from search summaries, not read in full: treat them as leads

Speaker notes: As in Task 3, the packs are this deck's bibliography; the numbers come from the repo docs and the terminal logs.
