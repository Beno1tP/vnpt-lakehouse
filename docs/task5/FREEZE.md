# Task 5 — Freeze record (git tag task5-v1)

Frozen 2026-11-13 (UTC 2026-11-12 evening). What the dashboard shows, what it is built from, and how to rebuild it.
Counts and identifiers only (no row contents). Evidence copies live outside Git under `~/data/vnpt-lab/evidence/`.

## 1. Data: no new data, Task 4's freeze

Task 5 reads the Task 4 Gold tables unchanged. Gold is rebuilt from the Iceberg tag **`task4_v1`** (see
`docs/task4/FREEZE.md` §1 for the 5 snapshot ids) with the dbt image below. `demo-check DEMO_TASK=5` re-reads the tag
through StarRocks (5 × `[catalog] PASS`: 10,729 / 21,224 / 21,224 / 10,729 / 10,613) and runs `gold-check` (22 × PASS).

## 2. Code and images (`infra/k8s/images.env` at the git tag)

| Image | Digest | Role |
|---|---|---|
| vnpt-lake-superset | `sha256:a0307416f2641b2394a095901e28e00e5e9659ad222ebf4f07298ef0de04946b` | built from 4662349: apache/superset:6.1.0 lean `@sha256:16b50bbe…` + starrocks 1.3.4 + psycopg2-binary 2.9.13 |
| vnpt-lake-dbt | `sha256:a7dc01a29a4d1346af06a7e3416ec5f5f6ffbc42e94a6afcc111a8ae6c9075f6` | builds Gold (Task 4) |
| vnpt-lake-spark / hms / spark-job / airflow | unchanged since Task 4 (`docs/task4/FREEZE.md` §2) | |

Postgres 16 `postgres:16@sha256:1a6ab3f5…` (metadata DB server, and the db-init Job).

## 3. The dashboard: code in Git, state in Postgres

| Object | Identifier | Where it comes from |
|---|---|---|
| Dashboard `VNPT chatbot – Gold` | uuid `3a0a5a6e-fba4-4d99-9df7-cc9f80db0c72`, published, 14 charts | `infra/k8s/superset/assets/dashboards/` (exported, imported by the Job) |
| Connection `StarRocks Gold` | uuid `cc56b846-4ea9-5aaf-a62f-3cc1e25f8f98` (uuid5), `bi_ro`, DML/CTAS/CVAS/upload off | created by the Job (password from Secret), the bundle only matches it |
| Datasets gold_uc4…uc8 | uuid5 of the table name (`6e0a0b84…`, `e1684352…`, `9b329a11…`, `6cfc17ea…`, `73f0cf6f…`) | `datasets.yaml` (14 metrics) via the Job + the bundle |
| Native filters | Traffic (`traffic_class`, default `real`, 6 charts) · Users (`user_class`, no default, 7 charts) | the dashboard YAML (`scope.excluded` chart ids, remapped on import) |
| Users | `admin` (Admin), `checker` (Gamma + role `gold_viewer` = datasource access on the 5 datasets) | the Job, passwords from `secrets.env` |
| Expected values | 33 chart values, filter defaults and scopes | `dashboard_checks.yaml` (`make superset-check`) |

The `superset` database (users, encrypted connection password, the imported objects) is in the daily `pg-backup`
(06:30 UTC = 13:30 ICT, `s3://backup/postgres/<ts>/superset.dump`), but it is **not needed** to rebuild: the drill
below rebuilt everything from Git into an empty database.

## 4. Runs and evidence

| What | Result |
|---|---|
| `make demo-check DEMO_TASK=5` (git `7efc186`) | ALL PASS: 8 ready, 4 × http 200, 5 × catalog, 22 × gold, superset-check PASS, 2 × paused |
| Reproduction drill (`make superset-drill`, scratch DB `superset_drill`, 11-13) | same dashboard uuid, 14 charts, scopes remapped to new chart ids, `superset-check: PASS`; scratch objects + DB removed |
| Node requests in demo-task5 | 6,116 Mi of 7,268 (84 %), limits 9,948 Mi (136 %); Superset 212 Mi in use |
| Evidence (states, no rows) | `~/data/vnpt-lab/evidence/task5-20261112/` (80 KB: superset-check, gold-check, assets check, pods, node, helm, dag runs, git head) |
| Dashboard screenshots (PDF exports) | `~/data/vnpt-lab/evidence/task5/` (5 PDFs, 516 KB: filters real/cleared, Users external/internal, full dashboard) |
| Errors | E79–E87 in `05_errors_and_fixes.md` |

## 5. Rebuild from this freeze (procedure)

1. Cluster + data as in `docs/task4/FREEZE.md` §5 (Gold rebuilt from `task4_v1`; `make gold-check` 22 PASS).
2. `make k8s-up` (namespace, Secrets, metadata DB), then `make demo-task5` (Superset: migrate, Job: admin, connect,
   **import from `infra/k8s/superset/assets/`**, connect, datasets + 20 checks, checker).
3. `make superset-check` → PASS; `make demo-check DEMO_TASK=5` → ALL PASS.

Proven without touching the real metadata DB: `make superset-drill` does step 2 on a scratch database and step 3 against
it (11-13: PASS); `make superset-drill-clean` removes it.
