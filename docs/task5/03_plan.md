# Task 5 — Plan v2: Superset dashboard on the StarRocks Gold layer

Goal (lab doc, Bước 5): *"Dùng Superset query dữ liệu · Build charts"* for the 5 use cases of *"Yêu cầu dashboard"*:
one **Superset 6.1.0** dashboard on the Task 4 Gold tables answering UC4–UC8, where **every number on screen
reconciles** with the expected values, the filters work, and the dashboard is versioned in Git.

v2 (2026-11-12) updates the 11-06 plan to what Task 4 actually built (review §R2). Knowledge: `01_knowledge.md`.
Versions/RAM/disk: `02_config.md`. Execution rule, PR procedure and error log: as Tasks 3–4 (merge chained on the
check, E74; `make dbt-parse`-style local checks before PRs; fake-SHELL recipe check). Errors from **E79**; decisions
from **D21**.

Inherits from Task 4 (`docs/task4/README.md`, `FREEZE.md`): database `gold` in StarRocks with `gold_uc4…uc8`
(`traffic_class` in UC4–6, `user_class` in UC7–8, BITMAP `users` in UC8), `fct_*`, `dim_*`, seeds `expected_gold_uc*`
and `scenario_labels`; user **`bi_ro`** (SELECT on `gold`, wildcard grant that survives rebuilds; password
`STARROCKS_BI_PASSWORD` in `secrets.env`); `make gold-check` (22 counts + sums); asset `lake_gold`; modes and
`demo-task4`.

---

## B. Architecture

```
namespace superset   Superset web ×1 (Gunicorn, 1 / 1.5 Gi), image vnpt-lake-superset (CI → GHCR, by digest)
                     = apache/superset:6.1.0@sha256:16b50bbe… + starrocks 1.3.4 + psycopg2-binary 2.9.13
                     (installed with Superset's own pins as constraints, pip check in CI)
                     Secrets: SUPERSET_SECRET_KEY, metadata-DB URI, admin password, bi_ro password (fixed names)
                     Job superset-init: db upgrade, init, admin user (from the Secret), import infra/k8s/superset/assets,
                     set the StarRocks connection (starrocks://bi_ro:…@starrocks-fe-service.starrocks:9030/default_catalog.gold)
namespace lake       Postgres: database + role `superset` (Job superset-db-init, idempotent; initdb never reruns, E27)
reads                StarRocks FE :9030 as bi_ro → gold.*  (no Trino, no Iceberg directly)
modes                serving = demo-task4 profile + Superset 1 (≈ 6.1 Gi requests); mode-pipeline / mode-gold /
                     mode-query also scale Superset to 0
```

## C. Steps

| Step | Do | Done when | You learn |
|---|---|---|---|
| ✅ **5.0 Plan + pins** | this plan, `02_config.md`, decision renumbering; versions checked 11-12 (Superset 6.1.0 digest, chart 0.22.8, dialect 1.3.4, psycopg2-binary 2.9.13); C: 52 GB free after the vhdx compaction | plan merged | how a BI tool is pinned like any other service |
| ✅ **5.1 Image** | `docker/superset/{Dockerfile, requirements.txt}`: `FROM apache/superset:6.1.0@sha256:…`, `pip install -c <Superset's own pins> starrocks==1.3.4 psycopg2-binary==2.9.13`; CI: build, `pip check`, `python -c "import starrocks, psycopg2"`, `superset version`; images.yml job | CI green; digest in `images.env` | lean base images, driver/dialect = SQLAlchemy plug-in, dependency conflicts caught at build time |
| ✅ **5.2 Metadata DB + Secrets** | generated `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_PASSWORD`, `SUPERSET_DB_PASSWORD`; Job `superset-db-init` in `lake` (role + DB `superset`, `CREATE … IF NOT EXISTS` pattern); namespace `superset` | `\l` shows `superset`; login as `superset` works | why Superset needs its own DB (users, charts, encrypted connection passwords) |
| ✅ **5.3 Install** | 5.3a: own manifests (D27) `infra/k8s/superset/` (Deployment + Service, `superset_config.py` in a ConfigMap, image by digest via kustomize, non-root, no ServiceAccount token, Secret `superset-env`), unit tests + CI (kustomize render, config loaded inside the image). 5.3b: Job `superset-init` (db upgrade, init, admin from Secret, D21) → `make superset-up` → `make superset-ui` | login as admin; 0 restarts after 10 min; requests measured | what a Helm chart generates and when owning 80 lines is simpler |
| ✅ **5.4 Connect** | the init Job sets the StarRocks connection as `bi_ro`; SQL Lab test | SQL Lab `SELECT count(*) FROM gold.fct_user_message` = 10,613; `CREATE TABLE` → denied | engine specs, connection URI, least privilege seen from the BI side |
| ✅ **5.5 Datasets + metrics** | one dataset per `gold_uc*`; metrics defined on the dataset: `SUM(message_cnt)`, share = ratio of sums, not-understood rate = `SUM(not)/SUM(understood+not)`, users = `BITMAP_UNION_COUNT(users)` (never SUM of daily distincts) | every chart reuses dataset metrics | semantic layer in the BI tool vs in dbt |
| ✅ **5.6 Charts + dashboard** | KPI row, UC4 (scenario share), UC5 (stacked bar + rate line), UC6 (top-N sorted bar, intent_group colour), UC7 (Big Number + change, NULL % shown as "no data"), UC8 (new/active per day + averages); native filters: **Traffic** on `traffic_class` (UC4–6, default `real`) and **Users** on `user_class` (UC7–8, default all); date range on `event_date`; freshness (D25) | every number = `06_validation.md` with the filters on **and** off | dashboard design rules (A3), filter scoping |
| ✅ **5.7 Reconcile on screen** | `tools/superset_check.py`: read each chart's data through Superset's chart-data API as a read-only user, compare with the seeds / `gold-check` values | all charts PASS, filter on and off | testing a dashboard, not only its tables |
| ✅ **5.8 As code + close** | `make superset-export` → `infra/k8s/superset/assets/` (YAML, no passwords); drop + re-init → same dashboard; freeze (evidence, screenshots), `demo-task5`, `demo-check DEMO_TASK=5`, README, tag `task5-v1` | a fresh `superset` DB shows the same dashboard; demo-check ALL PASS | dashboards as code, reproducibility |

## D. Decisions

| ID | Decision | Alternative rejected | Why |
|---|---|---|---|
| D21 (done, was D19) | Own init Job (admin from a Secret, import assets, set the connection) | the chart's `init.adminUser` | E35: chart values are stored in the release Secret and can be printed |
| D22 (done, was D20) | **Serving mode = demo-task4 profile + Superset** (scheduler/dag-processor/triggerer 0, Airflow api-server + History up) | Airflow fully at 0 (11-06 plan) | measured: demo-task4 5,092 Mi + Superset 1,024 ≈ 6,116 Mi < 7,268; the Airflow UI stays viewable during the demo |
| D23 (done, was D21) | Freshness from data: a Big Number on `max(event_date)` of Gold; a dbt load-time column (`_built_at`) only if the leader asks for it | hand-typed text | Gold has no `_loaded_at`; adding one costs a dbt image cycle; `max(event_date)` is honest and needs no change |
| D24 (done, was D22) | No Redis cache, no Celery | Redis subchart | one pod less; Gold is pre-aggregated; Bitnami chart images moved (`bitnamilegacy`), another pin |
| D25 (done) | Two native filters (Traffic on `traffic_class`, Users on `user_class`), scoped per chart | one filter for all | UC7/UC8 count users (a person is internal or external), UC4–6 count messages: different columns by design (Task 4 M4) |
| D26 (done) | Dialect + driver installed against Superset's own pins (`-c` constraints), `pip check` in CI | plain `pip install starrocks` | `starrocks` 1.3.4 requires `alembic>=1.14`, `sqlalchemy>=1.4`: an unconstrained install can upgrade Superset's own pins |
| D27 (Bee, 11-12) | **Own kustomize manifests** for Superset web (Deployment, Service, ConfigMap `superset_config.py`), applied by `make superset-up` | (a) chart 0.22.8 with workarounds; (b) Superset Kubernetes Operator v0.3.0 | the chart is deprecated upstream and needed overrides for almost everything; the operator is 0.x (5 months, 4 releases) and adds a controller pod + CRDs on 7 Gi; we keep only what we use, pinned like Postgres/MinIO |

## E. Risks and first checks

| Symptom | Likely cause | First check |
|---|---|---|
| won't start: insecure `SECRET_KEY` | default key | env `SUPERSET_SECRET_KEY` in the pod (`printenv \| cut -d= -f1`) |
| Redis connection errors in logs | a default left in config | `infra/k8s/superset/superset_config.py`, pod log |
| "Could not load database driver: starrocks" | dialect not in the image's venv | `pip list` with the venv's python in the pod |
| image build: dependency conflict | `starrocks` pulls a newer alembic/SQLAlchemy | CI `pip check` output (D26) |
| numbers differ only with a filter on | wrong filter column / scope | dataset columns, filter scoping |
| distinct users differ | `SUM` of daily distincts | dataset metric `BITMAP_UNION_COUNT(users)` |
| dates off by one | a chart uses `event_ts` | time axis = `event_date` |
| pod `Pending` | wrong mode | `make demo-task5` / measure requests |
| imported dashboard: "database not found" | DB name in YAML ≠ name set by the init Job | `infra/k8s/superset/assets/databases/*.yaml` `database_name` |
| after `make k8s-up` the profile is gone | `airflow-install` resets scheduler replicas | rerun the profile (`make demo-task5`) after every `k8s-up` |

## F. Repo paths

```
docker/superset/{Dockerfile, requirements.txt}         ➕
infra/k8s/superset/ (Deployment, Service, superset_config.py, init Job) ➕   infra/k8s/lake/superset-db-init-job.yaml ➕
infra/k8s/superset/assets/                                       ➕ exported YAML (no passwords)
tools/superset_check.py + tests                        ➕
.github/workflows: build + check the superset image    ✏️
Makefile: superset-up, superset-down, superset-init, superset-ui, superset-export,
          superset-check, demo-task5                   ✏️
docs/task5/01 … 07, README.md, img/                    ➕
```

## G. Questions for the leader

1. Scenario names for `STEP` 406665 / 406666 (UC4 shows the inferred labels from `scenario_labels`).
2. UC7 with 24 days of data: absolute change, % shown as "no data" (Task 4 M2) — OK?
3. UC8 averages over the 24 calendar days incl. zero days (M3) — OK?
4. Who views the dashboard (roles); row-level security expected?
5. Default view: Traffic filter `real` (hide `/home` and internal testers) or all?

---

## I. Lifecycle

- `make demo-task5` = `demo-task4` + Superset up; `demo-check DEMO_TASK=5` adds Superset ready, its health endpoint,
  and `superset-check`. Every other mode scales Superset to 0.
- Final presentation: Task 2 (MinIO as demo-viewer, Airflow history, Spark History), Task 3 (Trino), Task 4 (dbt
  lineage, gold-check), Task 5 (dashboard) from the same cluster, switching profiles between parts.
- After it: the one-time from-zero acceptance (Task 3 §I option b), then the cleanup (`02_config.md` §3).

---

## R. Review of the input plan (2026-11-06)

| # | Input plan said | Evidence | Changed to |
|---|---|---|---|
| R1 | Filter on two flags `is_home` / `is_internal_sender` | a native filter targets one column | `traffic_class` (Task 4 D20) |
| R2 | Init creates the admin from a Secret (chart) | E35 | D21 own Job |
| R3 | "Redis off" only as subchart flag | the chart's default config uses Redis | `configOverrides` + check the rendered config |
| R18 (11-12) | Helm chart superset 0.22.8 | `deprecated: true`; always renders a Celery worker (replicas field dropped → 1) and an init script with admin defaults (E80) | D27 own manifests |
| R4 | `superset` DB "created in Postgres (Job)" | initdb scripts never rerun (E27) | explicit idempotent Job |
| R5 | Strip the DB password from exported YAML | exports omit it; the import needs it again | the init Job sets the connection after import |
| R6 | Freshness "loaded <date>" text | — | D23 from data |
| R9 | Weekend: all three tasks | each image costs a CI build + first pull | one slice through all layers first (UC5), then widen |

## R2. Revision after Task 4 (2026-11-12)

| # | 11-06 plan | Evidence | Changed to |
|---|---|---|---|
| R10 | decisions D19–D22 | Task 4 uses D15–D20 | renumbered D21–D24, new D25–D26 |
| R11 | user `superset_ro` | Task 4 created `bi_ro` (SELECT on gold, wildcard grant, tested after a rebuild) | `bi_ro`; no new StarRocks user |
| R12 | StarRocks 2.5 Gi; serving needs Airflow at 0 (≈ 7,750 Mi) | measured StarRocks 2,176 Mi, demo-task4 5,092 Mi with the Airflow api-server up | D22: serving = demo-task4 + Superset ≈ 6,116 Mi |
| R13 | freshness from `max(_loaded_at)` | Gold has no `_loaded_at` | D23 |
| R14 | one filter on `traffic_class` for every chart | UC7/UC8 carry `user_class` (M4) | D25 two scoped filters |
| R15 | `pip install starrocks` | dialect 1.3.4 requires alembic ≥ 1.14 / SQLAlchemy ≥ 1.4 | D26 constraints + `pip check` |
| R16 | UC definitions "to confirm" | defined in `docs/metrics.md` (Task 4 4.0b, M1–M4) | charts follow metrics.md |
| R17 | step 5.7 "demo rehearsed" | Tasks 2–4 freeze with tags, evidence, demo-check | 5.7 automated on-screen reconciliation, 5.8 freeze like Task 4 |
