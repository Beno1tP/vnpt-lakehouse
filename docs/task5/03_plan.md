# Task 5 — Plan: Superset dashboard UC4–UC8

Goal: one **Superset** dashboard on the StarRocks Gold tables answering UC4–UC8, where **every number on screen reconciles** with the expected values, the traffic filter works, and the dashboard is versioned in Git.

Knowledge: `01_knowledge.md`. Versions/RAM/disk: `02_config.md`. Input plan (2026-11-06) reviewed in §R. Execution rule, PR procedure, error log: as Task 3.

Inherits from Task 4: `gold` database (`gold_uc*`, `fct_user_message`, dims) with `traffic_class`; user `superset_ro`; `docs/metrics.md`; expected seeds; modes.

---

## B. Architecture

```
superset (new): Superset web ×1 (thin image from GHCR), Secret: SECRET_KEY, metadata-DB URI, admin password, StarRocks URI
                Job superset-db-init (in lake: role + DB "superset" in Postgres, idempotent)
                Job superset-init (db upgrade, init, admin user, import superset/assets, set-database-uri)
reads        : StarRocks FE :9030 as superset_ro → default_catalog.gold
modes        : make mode-serving (Airflow 0, Trino 0, StarRocks 1, Superset 1) ↔ make mode-pipeline
```

## C. Steps

| Step | Do | Done when |
|---|---|---|
| **5.0 Prereq** | Task 4 `dbt build` green (at least `gold_uc5`); Job `superset-db-init`: role + DB via `psql` with the postgres superuser Secret (`CREATE ROLE … IF NOT EXISTS` pattern; initdb scripts run only on an empty data dir, E27) | `\l` shows `superset`; network login as `superset` works |
| **5.1 Image** | `docker/superset/{Dockerfile, requirements.txt}` (A2); CI builds → GHCR; CI step imports the driver | `import starrocks` passes in CI; digest in `images.env` |
| **5.2 Install** | `make superset-chart` (download + checksum); `infra/helm/superset/values-local.yaml`: image digest, `postgresql`/`redis` subcharts off, Celery/worker off, `configOverrides` for cache = `NullCache` (render with `helm template` and read the generated `superset_config.py`: the chart's default config points at Redis), env from Secrets, resources; chart's own admin/init **off** (E35: values land in the release Secret and NOTES) → `superset-init` Job | `make superset-ui` (port-forward); login as admin works; `kubectl logs` without Redis errors |
| **5.3 Connect** | `set-database-uri` in the init Job (StarRocks Gold as `superset_ro`); SQL Lab test | `SELECT count(*) FROM gold.fct_user_message` = 10,613; `CREATE TABLE` from SQL Lab → denied |
| **5.4 Datasets** | one dataset per `gold_uc*`; metrics in the dataset (`BITMAP_UNION_COUNT(users)`, understood %), not per chart | every chart reuses dataset metrics |
| **5.5 Charts + dashboard** | KPI row, UC4–UC8, native filter `traffic_class` (default `real`), freshness Big Numbers; layout per A3 | every number = expected (`06_validation.md`) with the filter `real` **and** all |
| **5.6 As code** | `make superset-export` → `superset/assets/`; delete dashboard → `superset-init` Job → same dashboard | fresh `superset` DB (drop + db-init + init) shows the same dashboard |
| **5.7 Demo + tag** | screenshots (fallback); `07_demo.md` (script + failure drill); root README end-to-end diagram; tag `task5-v1` | demo rehearsed once end to end |

**Weekend slice (Sunday afternoon):** 5.0 → 5.3 → UC5 dataset + chart + filter, reconciled. Rest during the week.

## D. Decisions

| ID | Decision | Alternative rejected | Why |
|---|---|---|---|
| D19 (proposed) | Own init Job (admin from Secret file, import, set URI) | chart's `init.adminUser` | E35: chart values are stored in the release Secret and may be printed |
| D20 (proposed) | Serving mode with Airflow at 0 | Airflow + Superset together | ≈ 7,750 Mi > 7,268 (`02_config.md` §2) |
| D21 (proposed) | Freshness from data (`max(event_date)`, `max(_loaded_at)`) | hand-written Markdown text | text goes stale silently |
| D22 (proposed) | No Redis cache | Redis subchart | one pod less; Gold is pre-aggregated; Bitnami chart images moved (`bitnamilegacy`), another pin to maintain |

## E. Risks and first checks

| Symptom | Likely cause | First check |
|---|---|---|
| won't start: insecure `SECRET_KEY` | default key | env `SUPERSET_SECRET_KEY` in the pod (`printenv \| cut -d= -f1`) |
| Redis connection errors in logs | chart default config | rendered `superset_config.py` (`helm template`) |
| "Could not load database driver: starrocks" | driver not in the image's venv | `pip list` with the venv's python in the pod |
| numbers differ only with the filter on | a `gold_uc*` without `traffic_class` | `DESCRIBE gold.gold_ucX` |
| distinct users differ | `SUM` of a daily distinct column | dataset metric `BITMAP_UNION_COUNT` |
| dates off by one | a chart uses `event_ts` | time axis = `event_date` |
| pod `Pending` | Airflow still up | `make mode-serving` |
| imported dashboard shows "database not found" | DB name in YAML ≠ name used by `set-database-uri` | `superset/assets/databases/*.yaml` `database_name` |

## F. Repo paths

```
docker/superset/{Dockerfile, requirements.txt}       ➕
infra/helm/superset/values-local.yaml                ➕
infra/k8s/superset/ (namespace, Secrets, init Job)   ➕   infra/k8s/lake/superset-db-init-job.yaml ➕
superset/assets/                                     ➕ exported YAML (no passwords)
.github/workflows: build the superset image          ✏️
Makefile: superset-chart, superset-install, superset-ui, superset-export, superset-import, mode-serving  ✏️
docs/task5/01 … 07, README.md, img/                  ➕
README.md (root): end-to-end diagram                  ✏️
```

## G. Questions for the leader

1. Exact definitions of UC4 and UC8 (same as Task 4 G3).
2. Who views the dashboard (roles); is row-level security expected?
3. Default view: hide `/home` and tester traffic (`real`), or show all?

---

## I. Integration with Task 2's demo tooling and lifecycle (2026-11-06, slides 70–71 of Task 2)

- **`mode-serving` = `make airflow-pause`** (refuses while a SparkApplication exists) + Trino 0 + StarRocks/Superset 1; `mode-pipeline` = `make airflow-resume` + Superset 0. Delete `spark-history` first if it runs (512 Mi).
- **Final presentation = the end of the lifecycle:** show Task 2 (MinIO as `viewer`, Airflow logs, Spark History Server), Task 3 (Trino), Task 4 (dbt lineage), Task 5 (dashboard) from the same cluster; switch modes between parts, because they don't fit in RAM together.
- **After it:** the one-time from-zero acceptance if it was moved here (Task 3 §I, option b), then the cleanup in `02_config.md` §3 (`minikube delete` only then).

---

## R. Review of the input plan (2026-11-06)

Kept: architecture, thin image via CI, Few's rules, UC → chart mapping, dashboards as code, subcharts off, RAM modes, thin-slice strategy, risks.

| # | Input plan said | Evidence | Changed to |
|---|---|---|---|
| R1 | Filter on two flags `is_home` / `is_internal_sender` | a native filter targets one column | `traffic_class` (Task 4 D18) |
| R2 | Init creates the admin from a Secret (chart) | E35: the Airflow chart echoed the admin password in NOTES and stored it in the release Secret | D19 own Job |
| R3 | "Redis off" only as subchart flag | the chart's default config uses Redis for cache/Celery | `configOverrides` + check rendered config |
| R4 | `superset` DB "created in Postgres (Job)" | `progress.md` §2 listed a `superset` DB, but initdb scripts never rerun on the existing data dir (E27) | explicit idempotent Job |
| R5 | Strip the DB password from exported YAML | exports omit the password; the import needs it set again | `set-database-uri` after import |
| R6 | Freshness "loaded <date>" text | — | D21 from data |
| R7 | Serving ≈ 7–7.5 Gi with FE 2 + BE 3 Gi | Task 4 D13 sizing | ≈ 5.7 Gi requests |
| R8 | Superset 6.1.0 / chart 0.22.x | not verified in this session | check commands in `02_config.md` |
| R9 | Weekend: all three tasks | Task 2 took 10-25 → 10-29 with the same per-step PR + CI loop; each new image (dbt, superset) costs a CI build + first pull (Airflow pull took ~10 min) | aim for the UC5 slice through all layers; widen during the week |
