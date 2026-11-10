# Task 3 — Silver with Spark, queried by Trino

The Task 2 pipeline gets a **Silver** layer: a Spark job types, dedups and flags each day of Bronze and writes three Iceberg tables. Then **Trino 483** reads the same Iceberg files through the same Hive Metastore, as a second, read-only engine. Every Silver number is computed three independent ways: pandas on the Excel files, Spark's validator, and Trino SQL. All three agree (191/191). Trino's check also runs **in Airflow**, started by an asset when Silver changes.

Status (2026-11-10): ✅ done; frozen the same day (Iceberg tag `task3_v1` on 2 Bronze + 3 Silver tables, evidence copy, demo profile `make demo-task3` + `make demo-check DEMO_TASK=3` ALL PASS). Git tag `task3-v1`. From-zero acceptance is **deferred** to one full-stack run after Task 5 (decision 11-10, `03_plan.md` §I option b), so the live results survive until the final presentation.

## Architecture (what Task 3 adds to Task 2)

```
DAG chatbot_bronze (daily 00:00 UTC = 07:00 ICT; id kept, D10)
  check_landing → bronze → validate (23) → silver → validate_silver (28) → notify
                                            │            └─ outlet: asset lake_silver
                                            └─ jobs/silver_transform.py: batch check, NTZ parse, dedup, flags,
                                               final_intent, left join → overwritePartitions by event_date (D7)
DAG trino_reconcile (schedule = asset lake_silver)
  wait_no_spark (sensor, reschedule) → trino_up (0→1) → reconcile (REST, 191 values) → trino_down (always → 0)

Iceberg (HMS 3.1.3 + MinIO)   bronze.chatlog, bronze.bot_predict
                              silver.chat_message · silver.nlu_prediction · silver.user_message  (event_ts TIMESTAMP_NTZ, D8)
  writer: Spark (spark-job image)       reader: Trino 483, 1 coordinator = worker, 2 Gi Guaranteed
                                                read-only twice: MinIO policy (user trino) + iceberg.security=read_only (D11)
Namespace trino: chart 1.44.0 (SHA-256 checked), image by digest; RBAC lets Airflow scale only trino-coordinator
Modes (D12): pipeline (Trino 0, Spark can run) · query (scheduler 0, Trino 1) · demo-task3 (view-only + Trino)
```

Silver tables (`01_knowledge.md` A1): `chat_message` (every chat row, flags `is_client`, `is_home`, `is_internal_sender`), `nlu_prediction` (every predict row, `final_intent`, `has_chat_row`: the 116 orphans stay, flagged), `user_message` (client messages + their prediction, left join).

## Run it

Prerequisites: the Task 2 stack (`docs/task2/README.md`), plus `TRINO_S3_*` and `MINIO_TRINO_SECRET` in `infra/k8s/secrets.env`.

```bash
make k8s-up                 # adds namespace trino, MinIO user trino, Secrets, RBAC (idempotent)
make trino-chart && make mode-query && make trino-install   # once: chart download + SHA-256, release trino
make trino-check            # query mode: every Silver number via Trino vs tests/expected (191/191)
make trino-explore          # query mode: EXPLAIN ANALYZE, metadata tables, time travel → 07_explain_plans.md
make mode-pipeline          # back to building (Trino 0, scheduler 1)
```

Other targets: `trino-cli`, `trino-ui` (http://localhost:8081), `sparkapp APP=silver|validate_silver DATE=`, `trino-tag-check TASK=3`. Freeze: `freeze TASK=3` (Spark, pipeline mode) → `evidence TASK=3` + `evidence-export TASK=3` → `demo-task3` → `demo-check DEMO_TASK=3` (tag checked **through Trino**: a Spark check at ~2.6 Gi would not fit next to Trino in that profile). Before presenting: `minikube start ; make k8s-up ; make demo-task3 ; make demo-check DEMO_TASK=3`.

## Versions and why

| Component | Pin | Why |
|---|---|---|
| Trino | 483, `trinodb/trino@sha256:db58cc93…0534` | the lab's version; digest = no drift |
| Trino chart | 1.44.0 (appVersion 483) | newest chart on 483 (1.45.0 = 484); SHA-256 checked (E58 pattern) |
| Silver job | same `vnpt-lake-spark-job` image (Spark 3.5.9, Iceberg 1.11.0) | one image for all Spark jobs |
| Airflow | unchanged 3.3.2; no Trino provider | `reconcile` uses Trino's REST API with the standard library |

## Decisions (`03_plan.md` §D)

| ID | Decision | Rejected |
|---|---|---|
| D6 | Silver = Spark job in the Task 2 DAG, before Trino | Silver in dbt |
| D7 | partition by `event_date`, dynamic `overwritePartitions()` | `MERGE INTO` by id (knowledge only, `07_explain_plans.md` §6) |
| D8 | `event_ts` = `TIMESTAMP_NTZ` + `event_date` | `timestamptz` (the "7 h off" bug in every reader) |
| D9 | `final_intent` uses the row's `NLU_THRESHOLD` | a job parameter |
| D10 | keep DAG id `chatbot_bronze` | rename (loses history, breaks targets/CI) |
| D11 | Trino read-only at two layers; no `ANALYZE` | Trino with write access |
| D12 | pipeline / query modes, never Trino + Spark | Trino always on (32 Mi spare during Bronze) |
| D13 | schedule stays UTC, docs show ICT | ICT timetable (changes `ds`, E38 risk) |
| D14 | Silver tested on Minikube with `make sparkapp` | Compose fast loop |
| 11-10 | 3.6 Trino check in Airflow (asset-triggered DAG) | `make trino-check` only |
| 11-10 | from-zero after Task 5, once | `minikube delete` at 3.7 (would lose history and evidence) |

## Results (details: `06_validation.md`)

| Check | Result |
|---|---|
| Expected values (pandas, 3.0) | totals = Task 1 numbers; per-day file for 18 days |
| `validate_silver` | 28/28 every day; backfill 07-02 → 07-25 twice: 155 per-day lines identical (E63) |
| Trino = pandas = Spark | `make trino-check` 191/191; `trino_reconcile` in Airflow 191/191 in 26 s, Trino back to 0 |
| Read-only | `CREATE TABLE` and `ANALYZE` → `Access Denied`; `s3-access-test` 4 × 6/6 |
| Explore (3.5) | pruning 18 → 1 split; Spark and Trino name the same `metadata.json`; time travel by tag and snapshot |
| Freeze `task3_v1` | 5 tables, rows 21,224 / 10,729 / 21,224 / 10,729 / 10,613 = expected, by Spark and again by Trino |
| Demo profile | demo-check ALL PASS (6 ready, 4 UIs 200, 5 tags); requests 4,964 Mi (68 %) = the budget estimate |
| Automation | daily DAG, asset-triggered `trino_reconcile`, `pg-backup` at the new 06:30 UTC slot (E66) |

## Resource footprint (measured 2026-11-10, `02_config.md` §4)

Requests of Allocatable 7,268 Mi: pipeline idle 4,036 (55 %) · Bronze run 7,236 · query mode ≈ 4,452 · **demo-task3 4,964 (68 %)**, limits 6,620 (91 %); WSL 5.2 GB available. Disk: Trino image +1.04 GB in the node (`/var` 19 G); evidence copy 120 MB outside Git (`~/data/vnpt-lab/evidence/task3-20261110/`: 417 S3 files + 195 Spark event logs). Clean-up: 5 old images removed from the node, 10 old GHCR versions deleted (deployed digests + newest 3 kept). **Windows C: 60 GB free (88 %)** is the disk to watch before Task 4.

## Errors met and lessons (`05_errors_and_fixes.md`, E63–E66)

- **Compare data, not logs** (E63): the backfill diff failed on log timestamps and a growing file count; compare only data lines.
- **A sleeping laptop breaks clocks** (E64, E66): an Airflow 3 task's short-lived JWT expired while the process was frozen. The CronJob controller waits on a monotonic clock that does not advance during suspend, so a slot was never evaluated. Fixes: retry the cheap sensor once; `make k8s-up` nudges the CronJob; backup slot moved to 06:30 UTC (13:30 ICT), proven on 11-10 (`pg-backup-29904870`, 06:30:12 UTC).
- **Airflow 3 DAG bundles are not on `sys.path`** (E65): shared code goes in `plugins/`; the Airflow image is built in three places, so change all three. CI caught it before deploy.
- **Timestamps**: write NTZ for a zone-less source; Spark's `collect()` prints in the pod zone, Trino with `SET TIME ZONE` shows ICT. Always label zones.

## Known limits / next

- Not run in Task 3: from zero (planned once after Task 5); MERGE demo (knowledge only); `ANALYZE` (by design).
- Trino is a single node, ad hoc only (no fault-tolerant execution); HMS still has no auth (Trino trusts the network).
- Leader questions: D6 placement, source zone of `CREATED_TIME`, `NLU_THRESHOLD` semantics, Trino's role in production (`03_plan.md` §G).
- Next: **Task 4**, StarRocks + dbt Gold (`docs/task4/03_plan.md`). Check C: free space first (StarRocks +2–3 GB).
