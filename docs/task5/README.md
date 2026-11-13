# Task 5 — Superset dashboard on the StarRocks Gold layer

Lab doc, Bước 5: *"Dùng Superset query dữ liệu · Build charts"* for the use cases of *"Yêu cầu dashboard"*. Superset
6.1.0 runs on the Minikube cluster of Tasks 2–4, reads the Task 4 Gold tables in StarRocks as the read-only user
`bi_ro`, and shows one dashboard that answers UC4–UC8. Every number on it is checked automatically against the Task 4
expected values, through Superset's own API, as a non-admin user; the dashboard itself is code in Git and was
reproduced from Git on an empty metadata database.

Status (2026-11-13): ✅ done and frozen: `make demo-check DEMO_TASK=5` ALL PASS, reproduction drill PASS, evidence
outside Git. Full freeze record: `FREEZE.md`. Git tag `task5-v1`.

## Architecture

```
StarRocks gold (Task 4, frozen task4_v1)  gold_uc4…uc8 (+ traffic_class / user_class, BITMAP users)
        │  read as bi_ro (SELECT on gold only), connection "StarRocks Gold" (fixed uuid5, DML/CTAS/upload off)
        ▼
namespace superset   Deployment superset (1 pod, Recreate; init containers wait-for-postgres + migrate)
                     image vnpt-lake-superset = apache/superset:6.1.0 (lean, digest) + starrocks 1.3.4 + psycopg2-binary
                     config: our superset_config.py (ConfigMap): SECRET_KEY + metadata DB from env, no Redis/Celery,
                             MySQLdb shim (E82), run_mode log filter (E85)
                     Job superset-init: admin from Secret → connect → import dashboard bundle (Git) → connect →
                             datasets + metrics (datasets.yaml, 20 checks) → role gold_viewer + user checker
                     pod superset-check (make superset-check): 33 values + filters/scoping as checker
namespace lake       Postgres: database `superset` (role superset, Job superset-db-init; daily pg-backup)
Git                  infra/k8s/superset/{superset.yaml, superset_config.py, datasets.yaml, dashboard_checks.yaml,
                     assets/ (exported dashboard: 1 database, 5 datasets, 14 charts, 1 dashboard)}
Modes                demo-task5 = demo-task4 (scheduler 0, asset DAGs paused, StarRocks up) + Superset: 6,116 Mi (84 %)
                     mode-pipeline / mode-gold / mode-query scale Superset to 0
```

## Run it

```bash
make mode-pipeline               # after a restart: engines to 0 first, so k8s-up (scheduler back to 1) fits in RAM
make k8s-up                      # namespace, Secrets, metadata DB (Job superset-db-init) — then re-run the profile:
make demo-task5                  # demo-task4 + superset-up (migrate, Job: admin, import, connection, datasets, checks)
make superset-ui                 # http://localhost:8088 (admin / SUPERSET_ADMIN_PASSWORD in secrets.env)
make superset-check              # every chart through the API as checker, filters on and off
make demo-check DEMO_TASK=5      # before presenting: Task 4 checks + Superset health + superset-check
```
Also: `make superset-export` (dashboard → `infra/k8s/superset/assets/`, checked; review the diff in a PR),
`make superset-drill` / `superset-drill-clean` (rebuild from Git on a scratch metadata DB), `make superset-down`.

## The dashboard (`VNPT chatbot – Gold`, 14 charts)

| Section | Charts | Filter |
|---|---|---|
| KPIs | Predictions · Data up to (freshness, D23) | Traffic / none |
| UC5 understood | stacked bars per day · not-understood rate | Traffic |
| UC4 scenarios | pie · 100 % share per day | Traffic |
| UC6 intents | top intents (horizontal, coloured by intent_group) | Traffic |
| UC7 total users | total users · change vs 30 days · change % ("No data": M2, with description) | Users |
| UC8 new/active users | new + active per day · avg new/day · avg active/day · distinct active (bitmap) | Users |

Native filters (D25): **Traffic** on `traffic_class`, default `real`; **Users** on `user_class`, no default.
The headline: not understood **1.3 %** of all predictions, but **58.0 %** of real traffic (`/home` clicks dominate
the volume and are always "understood"); all real traffic is in the free-text scenario, and fallback is its top intent.

## Versions and why

| Component | Pin | Why |
|---|---|---|
| Superset | 6.1.0, `apache/superset@sha256:16b50bbe…` (lean) | current release; lean image + only what we need |
| StarRocks dialect | `starrocks` 1.3.4, installed with Superset's own versions as constraints (D26) | CI proves nothing of Superset's 157 packages changed (SQLAlchemy stays 1.4.54) |
| Postgres driver | psycopg2-binary 2.9.13 | metadata DB in our Postgres 16 |
| Deployment | own kustomize manifests (D27) | the official Helm chart 0.22.8 is deprecated upstream (E80); the operator is 0.x |

## Decisions

| ID | Decision | Rejected |
|---|---|---|
| D21 | own init Job (admin from a Secret, verified) | the chart's admin (`admin`/`admin`, stored in the release) |
| D22 | serving = demo-task4 + Superset (6,116 Mi measured) | Airflow fully at 0 |
| D23 | freshness = `MAX(event_date)` on screen | a hand-typed date; a dbt `_built_at` column (only if the leader asks) |
| D24 | no Redis, no Celery (rate limits in memory: correct with 1 pod) | Redis subchart |
| D25 | two scoped native filters (messages vs users) | one filter for everything (UC7/8 have no traffic_class) |
| D26 | dialect against Superset's pins + `pip check` + freeze diff in CI | plain `pip install` |
| D27 | own manifests | deprecated chart with overrides; Superset operator v0.3.0 |
| 11-13 | read-only `checker` user for the automated check | admin |
| 11-13 | layout as built (headers, order, pie label) | — (Bee) |

## Results (details: `06_validation.md`, `FREEZE.md`)

| Check | Result |
|---|---|
| Datasets + metrics (Job, StarRocks) | 20/20 PASS, columns = dbt models (5/4/5/6/5) |
| Dashboard via API as `checker` | 33/33 values PASS, Traffic default real + 6 charts, Users + 7 charts, 14/14 charts reviewed |
| Dashboard on screen (PDFs) | 58.0 % / 1.3 %; 89/87/2 users; 3.71 (3.625 ext.) / 4.67; 97.8 % / 2.2 %; 2024-07-25 |
| Least privilege | bi_ro SELECT only (CREATE → Access denied); Superset refuses DML; checker sees only the 5 datasets |
| As code | export has no secret, fixed uuids; **drill: empty metadata DB → same dashboard uuid, scopes remapped to new chart ids, superset-check PASS** |
| Demo | `demo-check DEMO_TASK=5` ALL PASS (8 workloads, 4 UIs, 5 tags, 22 gold, superset-check, 2 paused) |

## Resource footprint (measured)

demo-task5 requests 6,116 Mi of 7,268 (84 %), limits 9,948 Mi (136 %: overcommitted, real usage far below);
Superset web 192–212 Mi idle, 1–229m CPU. Disk: image ~1.3 GB in the node; metadata DB a few MB; assets 156 KB in Git;
evidence 80 KB + 516 KB PDFs outside Git.

## Errors and lessons (`05_errors_and_fixes.md`, E79–E87)

- **Build steps as root leave root-owned files in the runtime user's home** (E79); a comparison check must first
  prove its inputs are valid (empty freeze looked like "every package changed").
- **Render before install; check a chart's status, not only its version** (E80: deprecated chart, "off" = unset = 1 worker).
- **Kubernetes service links collide with app env vars** (E81: Service `superset` → `SUPERSET_PORT=tcp://…`).
- **A check must take the user's code path** (E82: raw SQL passed, Superset's result typing needed MySQLdb).
- **Read the stored structure before interpreting it** (E83: scope = all minus excluded; new charts join every filter).
- **Names matter to tools** (E84: a `superset/` folder made ruff treat it as first-party; a cached lint is no check).
- **Least privilege makes libraries log permission warnings** (E85): read what they do on failure, silence narrowly.
- **Overlays live beside their base** (E86); **commas split `$(if …)` arguments in Make** (E87).

## Known limits / next

- Gold and the dashboard show Task 4's frozen data (24 days): UC7's 30-day comparison is "No data" by definition (M2).
- Leader questions: scenario names for STEP 406665/406666, M2/M3, default filter, who views (roles/RLS).
- From-zero acceptance of the whole stack: once, after the presentation (decided 11-10); then the cleanup.
