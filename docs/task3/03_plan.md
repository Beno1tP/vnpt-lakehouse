# Task 3 — Plan: Silver on Spark + Trino querying Iceberg

Goal: turn Bronze (raw strings) into **Silver** (typed, deduplicated, flagged, joined) inside the existing DAG, and query Bronze + Silver with **Trino** through the same Hive Metastore, so that **Trino = Spark = pandas expected values** on every Silver number.

Knowledge: `01_knowledge.md`. Versions/RAM/disk: `02_config.md`. Input plan (2026-11-06) reviewed in §R; this file is the version to execute.

**Execution rule (unchanged from Task 2):** Bee types every command (make, kubectl, helm, git push, gh). Claude writes files/docs/local commits, one stage at a time with "Expect:" lines. Branch + PR per step, `gh pr create --fill --base main` then `sleep 15 && gh pr checks --watch`, merge only on `All checks were successful` (E55). Errors from **E56**.

Inherits: namespaces `lake`, `spark-operator`, `spark-jobs`, `airflow`; DAG `chatbot_bronze`; CI → GHCR by digest; `jobs/validate_bronze.py` pattern (rules SQL + expected JSON); Telegram alerts; remote logs.

---

## B. Architecture

```
lake        : postgres, minio (+ user trino, read-only), hive-metastore           (Task 2)
spark-jobs  : SparkApplication bronze / validate / silver / validate-silver       (silver = new, same image)
airflow     : DAG chatbot_bronze: check_landing → bronze → validate → silver → validate_silver → notify
trino (new) : Trino 483, single node (coordinator schedules on itself), catalog "lake" → HMS + MinIO,
              iceberg.security=read_only; scaled 0 ↔ 1 by make mode-pipeline / mode-query
```

## C. Steps

| Step | Do | Done when |
|---|---|---|
| ✅ **3.0 Expected Silver numbers** | `tools/expected_silver.py` (pandas, reads `~/data/vnpt-lab/*.xlsx` locally, no Spark) → `tests/expected/silver.json` + `silver_daily.json`: per `event_date` rows, client / `/home` / internal-sender counts, `final_intent` split, unmatched predict 116, `user_message` rows + rows with prediction. Uses the Task 1 numbers as a cross-check (21,224 / 10,729 / 10,613 / 10,378 / 116 / 10,588 · 107 · 34) | JSON committed; the totals equal `docs/task1/06_validation.md` |
| ✅ **3.1 Silver job** | `jobs/silver_transform.py --ingest-date D` (name from D6): read both Bronze partitions of D, check one `_batch_id` per table, parse (`to_timestamp_ntz`), dedup, flags, `final_intent`, left join; write 3 tables partitioned by `event_date` with `overwritePartitions()` (D7). Unit tests `tests/unit/test_silver_transform.py` on a local SparkSession or pure functions: parser (AM/PM, 12 AM/PM, 9 digits), `final_intent` (fallback, below/at/above threshold, `UPDATED_INTENT` set), flags, dedup | CI green; **fast loop on Compose** (`make silver DATE=2026-10-16` against the Task 1 Bronze, which has all 18 days in one partition) = `silver.json` totals |
| ✅ **3.2 Silver on K8s + DAG** | `dags/templates/silver_sparkapp.yaml` (driver 1 Gi, executor as bronze); `jobs/validate_silver.py` + `tests/sql/silver_rules.sql` (day-generic: 0 null `event_ts`, unique `id`, `final_intent` ∈ accepted set, `event_date` = D, join coverage = 100 % of client messages with a prediction **or** ≥ parameter, row counts = Bronze counts) + exact per-day checks vs `silver_daily.json`; DAG tasks `silver` (retries 2) and `validate_silver` (retries 1, E41) after `validate` | `make backfill` 07-02 → 07-25 twice → identical Silver counts, all checks pass, 6 empty days still skipped |
| ✅ **3.3 Trino** | MinIO user `trino` + policy `lake-trino-read.json` (in `minio_users.sh`), `TRINO_S3_*` in `secrets.env` → Secret; `infra/helm/trino/values-local.yaml` (image by digest, `server.workers: 0`, include-coordinator, JVM/memory from `02_config.md` §2, catalog `lake` from A2, env from the Secret); `make trino-chart` (download + SHA-256), `trino-install`, `trino-cli`, `trino-ui`, `mode-pipeline`, `mode-query` | `SHOW SCHEMAS FROM lake` → bronze, silver; `make s3-access-test` extended: `trino` can read `lakehouse/`, cannot write; `CREATE TABLE` in Trino → `Access Denied` |
| ✅ **3.4 Reconcile** | `sql/trino/reconcile.sql` = the Silver checks rewritten in Trino SQL; `make trino-check` runs it via `trino-cli` and compares with `silver.json` (same Python comparer as the validator) | Trino = Spark = expected on every number |
| ✅ **3.5 Explore + explain** | `sql/trino/explore.sql`: metadata tables, snapshots equal to Spark's, time travel to the first Silver snapshot, `SHOW STATS`, `ANALYZE` denied, `EXPLAIN ANALYZE` with/without `event_date` filter; one MERGE demo in Spark on a scratch copy (A1 knowledge only) | Plans saved in `07_explain_plans.md`, pruning visible (splits, input bytes) and explained |
| ✅ **3.6 (Could) Trino check in Airflow** | Only if RAM allows (it does not while Spark runs, `02_config.md` §2): a separate small DAG in query mode, or skip. `make trino-check` already covers the requirement | — |
| ✅ freeze · ⏳ from zero (deferred to after the presentation) **3.7 Close** | From zero (`minikube delete` → `make k8s-up` → `make k8s-demo` → `make mode-query` → `make trino-check`); `README.md`; tag `task3-v1`; prune | From-zero passes |

**Weekend slice (if time runs out):** 3.0 → 3.1 → 3.2 → 3.3 → 3.4 for the UC5 numbers (`final_intent` split per day) is the minimum; 3.5 can follow during the week.

## D. Decisions

| ID | Decision | Alternative rejected | Why |
|---|---|---|---|
| D6 | Silver = `jobs/silver_transform.py` in the existing DAG, at the start of Task 3 (Bee 10-25, confirm with leader) | Silver in dbt (Task 4) | Lab doc: Spark ETL writes Iceberg, dbt builds Gold |
| **D7** (accepted 11-09) | Silver partitioned by `event_date`, written with dynamic `overwritePartitions()` | `MERGE INTO` by `id` (input plan) | One export = one event day (Bronze rule); overwrite also removes rows dropped by a corrected export; no SQL extensions needed. MERGE stays in knowledge + one demo |
| **D8** (accepted 11-09) | `event_ts` = `TIMESTAMP_NTZ` + `event_date DATE` | `timestamptz` after converting from `Asia/Ho_Chi_Minh` | Source has no zone; NTZ shows the same wall clock in Spark, Trino, StarRocks, Superset |
| **D9** (accepted 11-09) | `final_intent` uses the row's `NLU_THRESHOLD` column | job parameter | The threshold is in the data (0.8); a parameter could disagree |
| **D10** (accepted 11-09) | Keep DAG id `chatbot_bronze`, add Silver tasks | rename to `chatbot_lakehouse` | A new `dag_id` = new DAG: history, `make backfill/dag-runs/drill-*`, CI tests and Telegram rules all name `chatbot_bronze`. Rename later in one PR if wanted |
| **D11** (accepted 11-09) | Trino read-only twice (MinIO policy + `iceberg.security=read_only`); no `ANALYZE` | Trino with write access | HMS has no auth; Spark owns writes |
| **D12** (accepted 11-09) | Pipeline mode / query mode; Trino 0 while Spark runs | Trino always on | 32 Mi spare during bronze (`02_config.md` §2) |
| **D13** (Bee 11-09) | Airflow schedule stays **UTC** (`CronDataIntervalTimetable("0 0 * * *", UTC)`, run 07:00 ICT); UIs/docs show ICT | ICT timetable | correctness does not depend on it (`ds` labels landing `dt=D` = the Vietnamese date as written); an ICT timetable changes `ds`/run_ids (E38 risk) |
| **D14** (Bee 11-09) | 3.1 tested on Minikube with `make sparkapp APP=silver DATE=…` (one day), no Compose fast loop | Compose loop on Task 1 Bronze | Compose + Minikube share the 10 GiB VM; the K8s template is what runs in production |

## E. Risks and first checks

| Symptom | Likely cause | First check |
|---|---|---|
| `event_ts` nulls | pattern change / NTZ function | `SELECT to_timestamp_ntz('02-JUL-24 03.40.09.895455000 PM','dd-MMM-yy hh.mm.ss.SSSSSSSSS a')` in `make sql` |
| 7 h shift in Trino | column written as `timestamptz` | Trino `DESCRIBE` → must be `timestamp(6)`, not `timestamp(6) with time zone` |
| Silver counts < Bronze | inner join or filter instead of flag | compare `chat_message` rows with Bronze per day |
| Silver job fails on batch check | Bronze tables on different `_batch_id` (crash between commits) | rerun bronze for the day (`airflow tasks clear`, E49) |
| Silver pod `Pending` | Trino still up | `make mode-pipeline`; `kubectl describe node` requests |
| Trino `Table not found` | catalog/schema name or HMS URI | `SHOW SCHEMAS FROM lake`; same URI as Spark |
| Trino S3 403 | keys / policy of `trino` | `make s3-access-test` |
| Trino OOMKilled | heap + overhead > limit | `-Xmx` 75 %, lower `query.max-memory-per-node` |
| Pods `Evicted` | node DiskPressure | `minikube ssh -- df -h /var`; prune images |

## F. Repo paths (➕ new, ✏️ changed)

```
tools/expected_silver.py                         ➕ pandas expected values
tests/expected/{silver,silver_daily}.json        ➕
tests/sql/silver_rules.sql                       ➕ day-generic rules
tests/unit/test_silver_transform.py              ➕
jobs/silver_transform.py, jobs/validate_silver.py  ➕
dags/chatbot_bronze.py                           ✏️ + silver, validate_silver
dags/templates/{silver,validate_silver}_sparkapp.yaml  ➕
infra/k8s/lake/minio-users/{lake-trino-read.json, minio_users.sh}  ➕/✏️
infra/k8s/trino/ (namespace, Secret via secretGenerator)  ➕
infra/helm/trino/values-local.yaml               ➕
sql/trino/{reconcile.sql, explore.sql}           ➕
Makefile: silver, trino-chart, trino-install, trino-cli, trino-ui, trino-check, mode-pipeline, mode-query  ✏️
docs/task3/01 … 07, README.md                    ➕
```

## G. Questions for the leader

1. D6: Silver as a Spark job in the same DAG, before Trino: OK?
2. Source time zone of `CREATED_TIME`: Vietnam local time? (D8 stores it as written either way.)
3. `final_intent`: is the row's `NLU_THRESHOLD` the value the source procedure uses (D9)?
4. Trino in production: ad-hoc only, or also ETL (would make fault-tolerant execution relevant)?

---

## I. Integration with Task 2's demo tooling and lifecycle (2026-11-06, branch `task2/demo-lifecycle`, slides 70–71)

- **Modes reuse the Task 2 targets:** `mode-pipeline` = `make demo-pipeline` + Trino to 0; `mode-query` = `make demo-task2` pattern (scheduler/dag-processor/triggerer to 0, refuses while a run is active) + Trino up. (B3 replaced the earlier airflow-pause/resume idea.) Every mode first deletes `spark-history` if it runs (512 Mi request).
- **Silver gets event logs for free:** `silver_sparkapp.yaml` must copy the `spark.eventLog.*` + `spark-events` volume block from `bronze_sparkapp.yaml` (unit test `test_sparkapp_writes_event_log` should list it).
- **Time-zone decision before 3.1 (11-09):** Airflow's day D interval is [D 07:00 ICT, D+1 07:00 ICT) (UTC schedule), while the data day is the Vietnamese date as written. Option: `CronDataIntervalTimetable("0 0 * * *", timezone="Asia/Ho_Chi_Minh")`; first prove with a DAG test how `ds` renders under it (a wrong `ds` loads the wrong partition, E38 class). Also decide Silver's `event_ts`: Vietnam local without zone (`to_timestamp_ntz`) or an instant (`to_utc_timestamp(…, 'Asia/Ho_Chi_Minh')`).
- **Open decision for step 3.7 (Bee):** the lifecycle (Q53) keeps results until the final presentation, while 3.7 starts with `minikube delete`. Options: (a) `make evidence` + screenshots, then from zero as planned (results are rebuilt, Airflow history / event logs / drill evidence are lost); (b) move the from-zero acceptance to the end of Task 5, once, after the final presentation. To decide at 3.7.

---

## R. Review of the input plan (2026-11-06)

Checked against the repo and Task 1–2 measurements. Kept: Silver content and flags, three Silver tables, Trino 483 single node, catalog file, metadata/time-travel/EXPLAIN demos, risks table, read-only MinIO user, leader questions.

| # | Input plan said | Evidence | Changed to |
|---|---|---|---|
| R1 | Bronze partition = load date; one load touches several event days → MERGE | `tests/sql/bronze_rules.sql` rule `event_day_matches_partition` (passes every day): on K8s `_ingest_date` = event day | D7 overwrite by `event_date`; MERGE as knowledge |
| R2 | `initcap` may be needed; check 0 nulls | rule `created_time_parseable` already 0 nulls ×18 days with the same pattern | not needed; NTZ parse (D8) |
| R3 | Decide zone, store timestamp + date | writing Spark `TIMESTAMP` gives Iceberg `timestamptz` → readers shift | D8: `TIMESTAMP_NTZ` |
| R4 | `NLU_THRESHOLD` = job parameter | it is a column, always 0.8 (`progress.md` §6) | D9 |
| R5 | DAG renamed `chatbot_lakehouse`, file `dags/chatbot_lakehouse.py` | file is `dags/chatbot_bronze.py`; dag_id used by Makefile, CI, drills | D10 keep id |
| R6 | `jobs/silver_build.py --start --end` | D6 named `silver_transform.py`; all jobs take `--ingest-date` (one run = one day; ranges = Airflow backfill) | `silver_transform.py --ingest-date` |
| R7 | `validation/expected_silver.*`, "DuckDB" | repo uses `tests/expected/*.json` + `tests/sql/*_rules.sql`; Task 1 expected values were computed with **pandas**, Task 2 with Spark SQL (D3); no DuckDB anywhere | pandas script + `tests/expected/silver*.json`; "Trino = Spark = pandas" |
| R8 | Trino read-only **and** `ANALYZE` in 3.5 | Trino's Iceberg `ANALYZE` writes a stats file + metadata → needs write | `iceberg.security=read_only`, `SHOW STATS`, ANALYZE denied as proof (D11) |
| R9 | Join coverage ≥ 98 % | Task 1: 10,613 predict rows match the 10,613 client messages → 100 % on this data | exact per-day counts + generic threshold rule |
| R10 | Minikube default disk 20 GB, recreate with `--disk-size` | docker driver: node lives in `docker_data.vhdx` (Task 2 measured 8.3 GB node volume) | check `df` first, no recreate on assumption |
| R11 | 3.6 Trino provider in the DAG (Should) | provider not in the Airflow image; Trino can't be up while Spark runs (32 Mi spare) | Could, separate DAG or skip |
| R12 | Scale `deploy/trino-coordinator` | chart names it `<release>-trino-coordinator` unless `fullnameOverride` | `make mode-*` targets read the real name |
| R13 | Silver driver "as bronze" | bronze driver is 2 Gi only because of pandas reading Excel; Silver reads Iceberg | driver 1 Gi (measure) |
| R14 | Silver joins without batch check | Task 1 drill B: tables can be on different batches after a crash | `_batch_id` check before the join |
