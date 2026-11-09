# Task 5 — Knowledge: Superset dashboard on StarRocks Gold

Plan: `03_plan.md` (review of the input plan in §R). Versions/RAM/disk: `02_config.md`.

---

## A1. Superset architecture

| Part | Role |
|---|---|
| Web app (Flask + React, Gunicorn) | UI, REST API, SQL Lab, chart rendering; stateless, scale by replicas |
| **Metadata DB** | Postgres: users, roles, database connections, datasets, charts, dashboards (our `lake/postgres`, DB `superset`) |
| Database connection | A **SQLAlchemy URI** + a Python driver. StarRocks: `starrocks://superset_ro:<pwd>@<fe-service>.starrocks.svc.cluster.local:9030/default_catalog.gold` (package `starrocks`, which uses a MySQL driver underneath) |
| Engine spec | Superset's per-database adapter (time grains, quoting); StarRocks has its own spec, so `event_date` time grains work |
| Dataset | A physical table or a SQL query (virtual dataset) + **metrics** (`COUNT(*)`, `BITMAP_UNION_COUNT(users)`) and column settings |
| Chart → Dashboard | Chart = dataset + visualization; dashboard = charts + **native filters** + layout |
| Cache (Redis, optional) | Result cache; without it every view hits StarRocks (fine: Gold is pre-aggregated) |
| Celery workers (optional) | async SQL Lab, alerts & reports, thumbnails: **off** |
| Security | Roles (Admin / Alpha / Gamma), dataset permissions, row-level security. `SECRET_KEY` signs sessions **and encrypts stored DB passwords**: set from a Secret (`SUPERSET_SECRET_KEY` env); Superset refuses to start with the default. Changing it later needs `superset re-encrypt-secrets` with the old key |

**Where the speed comes from:** Superset only sends SQL. Speed = StarRocks + pre-aggregated `gold_uc*`.

## A2. The image: one driver to add

The official image is "lean" (few drivers). Thin image built **in CI → GHCR** (rule of Tasks 2–4): `FROM apache/superset:<pin>@sha256:…` + pinned `starrocks` SQLAlchemy dialect (+ its MySQL driver, + `psycopg2-binary` only if the metadata-DB driver is missing). The 6.x image keeps Python packages in its own virtual environment (`/app/.venv`): install there as the Superset docs ("Docker builds → adding drivers") describe, then check `python -c "import starrocks, psycopg2"` in CI.

## A3. Dashboard design (Few's rules, applied)

| Rule | Applied |
|---|---|
| One screen, most important top-left | KPI row (messages, users, understood %), then trends, then breakdowns |
| Right chart for the question | trend → line/bar; ranking → sorted horizontal bar; part-of-whole across days → stacked bar (not pie); one number vs previous period → Big Number with comparison |
| No decoration | no 3D, gradients, many colours; colour only for meaning (understood vs not) |
| Context on every number | period, filter state, **data freshness** (Big Number on `max(event_date)` and `max(_loaded_at)` from Gold, not hand-typed text) |
| Honest axes | bars start at 0; NULL comparison (no previous period) shown as "no data", not 0 % |

## A4. Use cases → charts (formulas from `docs/metrics.md`, Task 4)

| UC | Question (as understood) | Gold table | Chart | Note |
|---|---|---|---|---|
| UC4 | message volume over time (confirm) | `gold_uc4` | bar by `event_date`, split by channel | additive |
| UC5 | understood vs not understood per day | `gold_uc5` | stacked bar + understood % line | `final_intent` rule from Silver |
| UC6 | top intents | `gold_uc6` | sorted horizontal bar, top N | generic top-N (leader 10-23); 3 values today |
| UC7 | users in the window vs the previous window | `gold_uc7` (bitmap) | Big Number with comparison | previous window empty → NULL |
| UC8 | distinct users (confirm breakdown) | `gold_uc8` (bitmap) | bar/line of `BITMAP_UNION_COUNT` | never SUM daily distinct counts |

**The filter:** one dashboard native filter on `traffic_class` (Task 4 D18: `real` / `home` / `internal`), default = `real`. It works only because every `gold_uc*` table carries `traffic_class`. Show both states in the demo: the ~97.8 % `/home` share of client messages is the key data finding.

Bitmap metrics through a filter: the dataset metric `BITMAP_UNION_COUNT(users)` re-merges the bitmaps of the selected rows, so distinct users stay exact for any date range and filter value.

## A5. Dashboards as code

Export (UI or `superset export-dashboards`) → ZIP of YAML (database, datasets, charts, dashboard) → unzip into `superset/assets/`. Import on a fresh Superset (`superset import-dashboards`) in a Job. The database YAML does not carry the password; after import the Job sets the URI from the Secret (`superset set-database-uri -d "StarRocks Gold" -u …`, which updates the connection with that name). Check both commands with `superset --help` in the 6.x image first: CLI flags have changed between major versions. Proof of "as code": delete the dashboard (or the whole `superset` DB) → import Job → same dashboard.
