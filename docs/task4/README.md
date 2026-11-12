# Task 4 — StarRocks (OLAP) with a Gold layer built by dbt

Lab doc, Bước 4: *"Dựng StarRocks trên Minikube · Dùng SQL để tổng hợp dữ liệu · Ghi kết quả vào table dạng gold trên
StarRocks"*. StarRocks 3.5.21 runs on the Minikube cluster of Tasks 2–3, reads the Silver Iceberg tables **in place**
through an external catalog (no copy), and dbt builds a Kimball star plus one mart per use case (UC4–UC8, defined in
`docs/metrics.md`) inside StarRocks. Every Gold number is reconciled row by row with values computed by pandas from
the Excel files; Airflow builds Gold automatically when Silver changes.

Status (2026-11-12): ✅ done and frozen: Iceberg tag `task4_v1` on 2 Bronze + 3 Silver tables, Gold rebuilt **from
that tag** and reconciled, evidence outside Git, demo profile `make demo-task4` + `make demo-check DEMO_TASK=4` ALL
PASS. Full freeze record: `FREEZE.md`. Git tag `task4-v1`. From-zero acceptance: once after Task 5 (decided 11-10).

## Architecture

```
Iceberg on MinIO (HMS 3.1.3)  bronze.* · silver.chat_message / nlu_prediction / user_message (+ step, 4.0a)
        │ written by Spark (chatbot_bronze: … → silver → validate_silver ──► asset lake_silver)
        │ read in place by:  Trino 483 (Task 3, explore)        StarRocks 3.5.21 (Task 4, serve)
        ▼
namespace starrocks   operator 1.11.8 + FE (1/1.5 Gi, -Xmx1024m) + BE (1/2 Gi, mem_limit 1600M); chart 1.11.8 (SHA-256)
                      external catalog lake_iceberg (HMS + MinIO user `starrocks`, read-only)
                      database gold: dbt_rw (all on gold; SELECT + REFRESH on the catalog), bi_ro (SELECT gold: Task 5)
                      Job starrocks-init: runner image + SQL from ConfigMap starrocks-init-sql (16 idempotent statements)
namespace gold-jobs   dbt pods only (dbt_rw Secret, ServiceAccount without token): make dbt / dbt-parse / dbt-docs, DAG
dbt (dbt-core 1.12.6 + dbt-starrocks 1.12.2, image vnpt-lake-dbt by digest)
  stg_* (views over lake_iceberg.silver.*; ephemeral in a tag rebuild, E77)
  → dim_date (24 days) · dim_intent (3) · dim_sender (incremental PK, stable integer sender_key)
  → fct_prediction (10,729, PK + day partitions) · fct_user_message (10,613)
  → gold_uc4 (scenario = step) · uc5 (understood) · uc6 (intents) · uc7 (total users, D−30) · uc8 (new/active, BITMAP users)
  seeds expected_gold_uc4…8 (pandas) → reconcile_uc4…8; Silver gate assert_silver_counts; 34 tests
DAG gold_build (asset lake_silver): starrocks_ready (sensor, FE+BE Ready ≥ 120 s, E76; 30 min timeout) → dbt_build
  (KubernetesPodOperator, `dbt build`, retries 1) → verdict; outlet asset lake_gold
Modes (the cluster state is the single source of truth): pipeline (StarRocks 0, Spark runs) · gold (StarRocks 1) ·
  query (Trino 1); DAGs never scale the other engine; trino_reconcile skips in gold mode
```

## Run it

```bash
make k8s-up                      # namespaces, Secrets, RBAC, MinIO user starrocks, ConfigMap, Airflow (idempotent)
make starrocks-render && make starrocks-install   # once: FE/BE pinned by digest (no 4.1-latest), waits for a BE query
make starrocks-init              # catalog, database gold, users + grants, checks (rerun anytime)
make mode-gold                   # StarRocks up (Spark cannot run); make mode-pipeline = back to Spark
make dbt CMD="build"             # seeds + 13 models + 34 tests (PASS=56 incl. 3 REFRESH hooks)
make gold-check                  # 22 counts + measure sums vs expected
```
Also: `make dbt-parse` (parse your local `dbt/` before a PR), `make dbt-docs` (static lineage HTML, outside Git),
`make starrocks-sql SQL="…"`, `make starrocks-tag-check TASK=4`, `make dbt-tag-rebuild TAG=task4_v1` (Silver gate on
the tag, then build from it), `make demo-task4` + `make demo-check DEMO_TASK=4`.

**`silver_version` / `assert_silver_counts` in a Gold rebuild:** dbt *writes* Gold but *reads* Silver. Freezing Gold
= freezing its input (the Silver tag) + its code (the dbt image digest); the var picks which Silver snapshot the
staging models read, and the gate checks that input before any Gold is built.

## Versions and why

| Component | Pin | Why |
|---|---|---|
| StarRocks FE/BE | 3.5.21 by digest | lab version; the chart's default `4.1-latest` is overridden and checked by `starrocks-render` (R10) |
| Operator / chart | 1.11.8 / kube-starrocks 1.11.8 (SHA-256 `c05804d0…`) | newest at the time; compatibility proven by rendering + running (the changelog stops at 1.9.8) |
| dbt | dbt-core 1.12.6 + dbt-starrocks 1.12.2 on python:3.11-slim by digest | the adapter only sets a floor (`dbt-core>=1.9`): CI `dbt-lint` proves the pair (`dbt parse`) |
| Airflow, Spark, Iceberg, HMS, Trino | unchanged from Tasks 2–3 | one change at a time |

## Decisions

| ID | Decision | Rejected |
|---|---|---|
| D15 | FE 1/1.5 Gi, BE **1**/2 Gi (BE request lowered from 1.5 Gi after measuring 302 Mi idle) + modes | FE 2 + BE 3 Gi always on (9,156 Mi > 7,268 Allocatable) |
| D16 | `gold_build` on the existing asset `lake_silver`; mode gate (sensor) instead of scaling | dbt as the last task of the Spark DAG |
| D17 | init Job with the dbt image as runner; statements in a ConfigMap (11-11) | a `mysql` client image; SQL baked into the image |
| D18 | full rebuilds (`table`), only `dim_sender` incremental (stable keys) | incremental everywhere |
| D19 | expected UC values from pandas → seeds | DuckDB |
| D20 | `traffic_class` (internal > unmatched > home > real) in facts and marts; `user_class` for users | two boolean filters |
| M1–M4 | 116 orphan predictions = `unmatched`; UC7 vs D−30 with NULL % on a zero base; UC8 averages over 24 calendar days; user filter internal/external | (`docs/metrics.md`) |
| 11-11 | dbt pods in their own namespace `gold-jobs` | pods in `starrocks` (pod-create rights = Secret-read rights incl. root) |
| 11-11 | `make dbt-parse` before every dbt PR | only CI parses |
| 11-12 | freeze = Silver tag + dbt image; rebuild from the tag proven; tag-vs-maintenance drill on a scratch table | StarRocks BACKUP SNAPSHOT (needs a repository) |

## Results (details: `06_validation.md`, `07_dbt_and_plans.md`)

| Check | Result |
|---|---|
| Expected values (pandas) | totals = Silver (`silver.json`); 89 users = Task 1's distinct senders; seeds 36 / 36 / 57 / 48 / 48 |
| StarRocks reads Silver | 10,613 = Trino = Spark = pandas (four engines, one number) |
| `dbt build` | PASS=56: 13 models, 6 seeds, 34 tests incl. `reconcile_uc4…uc8` (filter on and off) and `assert_bitmap_users` |
| `gold-check` | 22/22 counts + measure sums (e.g. UC5 10,588 understood / 141 not; UC8 89 new, 112 active, bitmap 89) |
| Automation | Silver change → `lake_silver` → trino_reconcile (pipeline mode) → gold_build (gold mode) → `lake_gold`, no manual trigger |
| Least privilege | MinIO `starrocks` 6/6 read-only; `bi_ro` CREATE/INSERT → privilege error; grants survive rebuilds (wildcard) |
| Plans | Gold table: 1 partition, 1 tablet, ~9 rows; same question on Silver via the catalog: Iceberg scan of 10,729 rows |
| Bitmap | exact 87 + 2 users vs 106 + 6 when summing daily distincts (23 people counted twice) |
| Freeze | `task4_v1` on 5 tables; rebuild from the tag PASS=53, negative run on `task3_v1` fails the gate; tag survives `expire_snapshots(retain_last=1)` (scratch drill) |

## Resource footprint (measured)

Node requests of Allocatable 7,268 Mi: pipeline 4,036 (StarRocks 0) · gold 6,212 (85 %) + dbt pod 512 while it runs ·
**demo-task4 5,092 (70 %)**, limits 8,412 (115 %), cpu 2,400m (30 %). Idle usage: FE 485 Mi, BE 302 Mi, operator 19 Mi.
Disk: node `/var` 24 G (+5 G for FE 1.02 GB / BE 1.33 GB images and dbt builds); PVC sizes are not enforced by
Minikube's hostpath provisioner (BE reports the node disk). Evidence 180 MB + dbt docs 3.3 MB outside Git.

## Errors and lessons (`05_errors_and_fixes.md`, E67–E78)

- **Checks must sit at a consistent point and must gate** (E67, E74, E78): a whole-lake reconcile mid-backfill fails;
  an always-run clean-up as the only leaf hides failures; a merge on a separate line ignores a red check; a dbt test
  stops being a gate when the graph edge to the models disappears. Wire gates with `&&` / explicit steps.
- **Laptop sleep breaks timers** (E68, like E64/E66): controller requeues and TTLs pause; `make k8s-up` nudges them.
- **Know the operator's reconcile model** (E69): the StarRocks operator compares a spec hash; it never restores
  replicas changed behind its back. Wait for a revision (`rollout status`), not a pod name.
- **Lists in several places need a test** (E70: a ConfigMap `items:` whitelist).
- **Object lifetime and negative tests** (E71): a cursor without its connection; a denial counts only as `Access denied`.
- **RBAC is per operation** (E72: REFRESH is its own privilege).
- **Read the adapter's types and the template engine's syntax** (E73 list vs string, E74 no `#` in Jinja).
- **Each StarRocks table model constrains columns** (E75: BITMAP only in PK/Aggregate) — test DDL in a scratch DB first.
- **Liveness ≠ readiness** (E76: the FE blacklists a just-started BE).
- **A green run proves only what actually ran** (E77: `skip` views; StarRocks views drop `FOR VERSION AS OF`).

## Known limits / next

- Gold lives in StarRocks internal tables: reproducible from `task4_v1` + the dbt image, not snapshotted.
- A StarRocks view cannot pin an Iceberg tag (E77); tag rebuilds use ephemeral staging.
- Dashboard (Superset, Task 5) reads `gold` as `bi_ro`; the scenario names of `step` are inferred (leader question).
- Open (`progress.md`): why `airflow tasks clear -t` matched nothing (whole-day clear used); artifact upload to S3
  (dbt output is in `s3://airflow-logs`); Write-Audit-Publish with `SWAP` (Could).
