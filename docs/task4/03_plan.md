# Task 4 — Plan: StarRocks Gold with dbt

Goal: build the **Gold layer** (Kimball star + `gold_uc*` aggregates) **inside StarRocks** from the Silver Iceberg tables, with **dbt** models and tests, so every Gold number equals the pandas expected values and Task 5's dashboard reads fast, pre-computed tables.

Knowledge: `01_knowledge.md`. Versions/RAM/disk: `02_config.md`. Input plan (2026-11-06) reviewed in §R.

Execution rule, PR procedure and error log: as Task 3 (`docs/task3/03_plan.md` header); IDs continue after Task 3's last error.

Inherits from Task 3: `silver.chat_message`, `silver.nlu_prediction`, `silver.user_message`; DAG `chatbot_bronze` (with Silver); `tests/expected/silver*.json`; modes `mode-pipeline` / `mode-query`.

**Not blocked:** the leader decided on 10-23 (`progress.md` §5): 18 days OK, start with the existing intents, re-analyse UC6/UC7 at Task 5. So UC6 = generic top-N on `final_intent` (N parameter, shows 3 values today) and UC7 = window anchored to the latest `event_date` with a `window_days` variable (empty previous window → NULL), built now, confirmed at the Task 5 demo.

---

## B. Architecture

```
starrocks (new): operator + StarRocksCluster (FE ×1, BE ×1, StarRocks 3.5.21), default_replication_num = 1
                 Job starrocks-init (dbt image + small Python script): catalog lake_iceberg, databases raw/gold,
                 users dbt_rw (gold rw, SELECT on lake_iceberg), superset_ro (SELECT on gold) — idempotent
                 catalog lake_iceberg → HMS + MinIO (user starrocks, read-only)
airflow        : chatbot_bronze … → validate_silver (outlet Asset lake://silver/user_message)
                 gold_build (schedule = that Asset): starrocks_ready (sensor, reschedule) → refresh_catalog → dbt_build
                 (KubernetesPodOperator, dbt image from GHCR, env from Secret)
```

## C. Steps

| Step | Do | Done when |
|---|---|---|
| **4.0 Metrics + expected** | `docs/metrics.md`: one row per UC (question, formula, grain, filters, NULL rule, open question); `tools/expected_gold.py` (pandas, same input as `expected_silver.py`) → `dbt/seeds/expected_gold_uc*.csv` | definitions written; leader questions F1/F2 sent |
| **4.1 StarRocks** | `make starrocks-chart` (download + checksum), `infra/helm/starrocks/values-local.yaml` (images by digest, resources from `02_config.md` §2, `fe.conf` JAVA_OPTS + `default_replication_num = 1`, `be.conf mem_limit`, PVCs small), root password from Secret; `make starrocks-install`, `starrocks-sql` (port-forward 9030), `mode-gold` | `SHOW BACKENDS` → 1 alive; `SHOW FRONTENDS` → 1 leader; pods 0 restarts after 10 min |
| **4.2 Catalog + users** | MinIO user `starrocks` (read-only policy, as Task 3); `docker/dbt/` image (CI → GHCR); Job `starrocks-init` | `SELECT count(*) FROM lake_iceberg.silver.user_message` = Trino = `silver.json`; **dbt source rendering tested**: 3-part source or raw views decided; `superset_ro` cannot `CREATE`/`INSERT` |
| **4.3 dbt core models** | `dbt/`: `stg_*` (views), `dim_date`, `dim_sender` (incremental, stable keys), `dim_intent`, `fct_user_message` (Primary Key `message_id`, partition `event_date`); generic tests on keys, flags, `accepted_values` on `final_intent` and `traffic_class`, `relationships` fct → dims | `dbt build --select +fct_user_message` green |
| **4.4 Gold UC models** | `gold_uc4…uc8` (bitmap users for UC7/UC8; flags + `traffic_class` as dimensions); singular tests `reconcile_uc*` (model vs seed, filter on and off); test `dim_sender_keys_stable` (keys before = after a full rebuild) | `dbt build` green, every reconciliation passes |
| **4.5 Orchestrate** | DAG `gold_build` (A6); `validate_silver` gets the Asset outlet; dbt tests fail the task (retries 0 on tests, 1 on the pod for transient errors) | `make mode-pipeline` → backfill → `make mode-gold` → `gold_build` runs by itself from the queued Asset event → green; rerun → identical Gold |
| **4.6 Explain + docs** | `EXPLAIN` a UC query on `gold_uc5` vs the same on `lake_iceberg.silver.user_message`; `dbt docs generate` lineage screenshot; `07_dbt_and_plans.md`; from zero; tag `task4-v1` | lineage + plans in the docs; from zero passes |

**Weekend slice (Sunday morning):** 4.1 → 4.2 → `fct_user_message` + `gold_uc5` + `reconcile_uc5` only, run by `make dbt-build` (a one-off Pod), no DAG yet. Widen to UC4/UC8, then UC6/UC7, then 4.5, during the week.

## D. Decisions

| ID | Decision | Alternative rejected | Why |
|---|---|---|---|
| **D15** (accepted 11-10, BE request revised by measurement) | StarRocks FE 1/1.5 Gi, BE **1**/2 Gi (Burstable, `mem_limit` 1600M) + gold mode (`make mode-gold`; pipeline and query modes scale StarRocks to 0: operator first, then FE/BE StatefulSets) | FE 2 Gi + BE 3 Gi always on (input plan) | 9,156 Mi idle > 7,268 Allocatable (`02_config.md` §2) |
| D16 (proposed) | Separate DAG `gold_build` on an Asset from `validate_silver` | `dbt_build` as the last task of the Spark DAG | StarRocks and Spark cannot run together on 8 GiB; Asset events wait for gold mode; one DAG per layer |
| D17 (proposed) | Catalog, users, raw views by one idempotent `starrocks-init` Job **using the dbt image** | a `mysql` client image | one image less (~0.6 GB disk) |
| D18 (proposed) | `fct_user_message` and `gold_uc*` = full rebuild (`table`); only `dim_sender` incremental | incremental everywhere | 10 k rows rebuild in seconds; full rebuild is idempotent by construction; incremental only where state matters (stable keys) |
| D19 (proposed) | Expected Gold values from pandas → seeds | DuckDB (input plan) | no DuckDB in the repo; pandas already produced Task 1's numbers; an engine independent of Spark/StarRocks |
| D20 (proposed) | `traffic_class` column in every `gold_uc*` | two boolean filters | one Superset native filter |

## E. Risks and first checks

| Symptom | Likely cause | First check |
|---|---|---|
| FE OOMKilled / CrashLoop | default FE heap (~8 GB) | `kubectl logs` + `fe.conf` JAVA_OPTS rendered by the operator |
| `CREATE TABLE` fails: replication > alive BEs | default `replication_num` 3 | `ADMIN SHOW FRONTEND CONFIG LIKE 'default_replication_num'` |
| BE OOMKilled | `mem_limit` above pod limit | `be.conf mem_limit` ≈ 80 % of the limit |
| Catalog: `Unknown host` / S3 errors | short DNS names, path-style off | FQDNs; `enable_path_style_access=true`, `enable_ssl=false` |
| Gold count lags Silver | catalog metadata cache | `REFRESH EXTERNAL TABLE` before `dbt build` |
| Distinct users ≠ expected | summed daily distinct counts, or moved sender keys | `BITMAP_UNION_COUNT` over days; `dim_sender_keys_stable` test |
| dbt can't find the source | 3-part name not rendered | raw view layer (A4) |
| `gold_build` never starts | Asset outlet missing / name differs | Airflow UI → Assets; queued events |
| Spark `Pending` | StarRocks still up | `make mode-pipeline` |

## F. Repo paths

```
docs/metrics.md                              ➕
tools/expected_gold.py                       ➕
dbt/ dbt_project.yml, profiles.yml (env_var only), packages.yml   ➕
  models/staging/stg_*.sql  models/marts/dim_*.sql, fct_user_message.sql  models/gold/gold_uc4…uc8.sql
  seeds/expected_gold_*.csv  tests/reconcile_*.sql, dim_sender_keys_stable.sql  macros/
docker/dbt/{Dockerfile, requirements.txt}    ➕ python slim + pinned dbt-core/dbt-starrocks + COPY dbt/
scripts/starrocks_init.py                    ➕ catalog, databases, users, grants (idempotent)
infra/helm/starrocks/values-local.yaml       ➕
infra/k8s/starrocks/ (namespace, Secret, init Job, dbt Pod template)  ➕
infra/k8s/lake/minio-users/lake-starrocks-read.json  ➕
dags/gold_build.py  ✏️ dags/chatbot_bronze.py (Asset outlet)
.github/workflows: build the dbt image        ✏️
Makefile: starrocks-chart, starrocks-install, starrocks-sql, dbt-build, dbt-docs, mode-gold  ✏️
docs/task4/01 … 07, README.md                ➕
```

## G. Questions for the leader

1. UC6: generic top-N on `final_intent` (3 values today) is acceptable, or is there a bot/sample with ≥ 12 intents?
2. UC7: window anchored to the latest event date with `window_days` (previous window empty → NULL)?
3. Exact definitions of UC4 and UC8 (what is counted, which breakdown)? Is "user" = `SENDER_ID` of client messages?
4. Production: Gold inside StarRocks (internal tables) or written back to Iceberg so Trino can read it too?
5. Does the team build aggregates with StarRocks materialized views or with dbt?

---

## I. Integration with Task 2's demo tooling and lifecycle (2026-11-06, slides 70–71 of Task 2)

- **Modes reuse the Task 2 targets:** `mode-gold` = the `demo-task2` pattern (B3: scheduler, dag-processor, triggerer to 0) only if Airflow must give way to StarRocks (otherwise keep it, since `gold_build` runs in Airflow); `mode-pipeline` = `make demo-pipeline` + StarRocks to 0. Every mode first deletes `spark-history` if it runs (512 Mi).
- **No Spark event logs for dbt:** dbt runs SQL in StarRocks, not Spark; its evidence = `dbt` run results + `dbt docs` lineage, kept with `make evidence`-style files outside Git.
- **Keep results:** StarRocks FE/BE scaled to 0 keep their PVCs; never `minikube delete` before the final presentation (Q53).

---

## R. Review of the input plan (2026-11-06)

Kept: StarRocks architecture, table types, bitmap reasoning, stable `sender_key` via incremental `dim_sender`, external catalog properties, raw-view fallback for 3-part names, seeds as expected values, Kimball 4 steps, flags as dimensions, risks, repo layout.

| # | Input plan said | Evidence | Changed to |
|---|---|---|---|
| R1 | "Blocked by the leader on UC6/UC7" | `progress.md` §5 10-23: leader answered (18 days OK, existing intents, re-analyse at Task 5) | not blocked; generic top-N + window parameter now |
| R2 | FE 2 Gi + BE 3 Gi; "≈ 10 Gi, scale Trino to 0" | idle requests 4,036 Mi + 5,120 = 9,156 > 7,268 Allocatable even with Trino and Spark at 0 | D15 smaller Burstable sizing + gold mode |
| R3 | `dbt_build` as a task after `validate_silver` in one DAG run | StarRocks must be down while Spark runs (32 Mi spare during bronze) → one DAG run cannot contain both | D16 Asset-triggered `gold_build` |
| R4 | Expected values from DuckDB | repo has no DuckDB; Task 1 used pandas | D19 pandas |
| R5 | `replication_num` per model only | FE config `default_replication_num = 1` also covers tables created outside dbt (raw, init) | both |
| R6 | FE `-Xmx1536m` | the FE ships with a large default heap in `fe.conf`; the operator's config must override it, otherwise the pod is OOMKilled | explicit `JAVA_OPTS` in values |
| R7 | Catalog Job (image unspecified) | a `mysql` client image costs another ~0.6 GB | D17 dbt image + Python script |
| R8 | No catalog refresh | external catalog caches Iceberg metadata | `refresh_catalog` task |
| R9 | Two flags for the filter | Superset native filter = one column | D20 `traffic_class` (flags kept too) |
| R10 | Operator v1.11.x, dbt-core ≥ 1.6.2 | not verified in this session | check commands in `02_config.md` §1 |
| R11 | Bitmap justified by scale | 89 senders: `COUNT(DISTINCT)` would be instant | kept, with the honest reason (A2) |
