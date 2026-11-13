# Slide 1 — Task 5: a Superset dashboard on the StarRocks Gold layer
VNPT AI · Data Engineering onboarding lab · Task 5 of 5
One dashboard for UC4–UC8, every number checked through Superset's own API, the dashboard itself as code
Bee (Pham Quang Minh) · 2026-11-13

Visual: dark title slide; small orange line "VNPT AI · DE onboarding lab · Task 5".

Speaker notes: Task 4 produced Gold: small, tested, pre-aggregated tables in StarRocks. Task 5 is the last mile: a BI tool that people use. The engineering question is not "can I draw charts" but "how do I know every number on the screen is right, and can I rebuild this dashboard from Git". Same structure as before: knowledge, the stack, running it, errors.

---
# Slide 2 — What Task 5 does with the data
In one sentence: Superset reads the Gold tables as a read-only StarRocks user and shows one dashboard that answers UC4–UC8; a script reads every chart back through Superset's API and compares it with the expected values.

Lab target flow: Gold (StarRocks) → Superset query → charts → dashboard

What Task 5 implements:
- Superset 6.1.0 on Minikube (own image built by CI, own manifests), metadata DB in our Postgres
- Connection "StarRocks Gold" as bi_ro (SELECT on gold only), DML / CTAS / upload off
- 5 datasets (one per gold_uc*) with 14 named metrics, defined as code and checked in StarRocks (20 checks)
- Dashboard "VNPT chatbot – Gold": 14 charts, native filters Traffic (traffic_class, default real) and Users (user_class)
- make superset-check: 33 chart values + filter defaults and scopes, as a non-admin user
- The dashboard exported to Git and re-imported into an empty metadata database (drill): same dashboard

Agenda: Knowledge (Superset architecture, semantic layer, filters, design rules, dashboards as code) · The stack · Running it (what the dashboard says, checks, drill) · Errors · Next

Visual: gold_uc4…8 in StarRocks → "bi_ro (read-only)" → Superset (datasets + metrics → charts → dashboard with 2 filters) → a browser; below, Git "assets/" ⇄ Superset (export / import) and a "superset-check" robot reading the charts.

Speaker notes: Everything the dashboard shows was defined in Task 4 (docs/metrics.md) and computed there. Superset adds no business logic beyond ratios of sums; that is deliberate.

---
# Slide 3 — The problem
- People need the answers on one screen, filterable, without SQL
- Risks of a BI layer:
  - Wrong numbers that look right (a SUM of daily distinct users, an average of daily rates)
  - A filter that silently does not apply to a chart, or applies to the wrong one
  - A dashboard that exists only in one database: lost with it, not reviewable
  - A BI user that can write to the warehouse
- Requirements: every number checked against independent values · filter defaults and scopes checked · dashboard in Git, rebuildable · read-only at two layers · fits in the laptop's RAM next to StarRocks and Airflow

Visual: a dashboard with three red question marks: "is this right?", "does the filter apply here?", "where is this stored?".

Speaker notes: A dashboard is usually tested by someone looking at it. Here it is tested by a command, and the looking (PDF exports) is the second, independent check.

---
# Part 1 — Knowledge

---
# Slide 4 — Superset architecture

| Part | Role |
|---|---|
| Web app (Flask + React, Gunicorn) | UI, REST API, SQL Lab, chart rendering; stateless, scale by replicas |
| Metadata DB (Postgres) | users, roles, connections (password encrypted with SECRET_KEY), datasets, charts, dashboards |
| Connection | a SQLAlchemy URI + a driver: starrocks://bi_ro:…@starrocks-fe-service…:9030/default_catalog.gold |
| Engine spec | Superset's per-database adapter (time grains, quoting, result typing) |
| Dataset | a table (or a SQL query) + metrics + column settings: the semantic layer |
| Chart → dashboard | chart = dataset + visualization + query; dashboard = charts + native filters + layout |
| Cache: Redis (optional) | result cache; off here (Gold is pre-aggregated) |
| Celery workers (optional) | async SQL Lab, alerts and reports, thumbnails; off here |
| Security | roles Admin / Alpha / Gamma, dataset permissions, row-level security |

- Where the speed comes from: Superset only sends SQL; speed = StarRocks + pre-aggregated gold_uc*
- SECRET_KEY signs sessions and encrypts stored connection passwords: from a Secret; rotating it needs superset re-encrypt-secrets

Visual: browser → Superset web (Gunicorn) → two arrows: Postgres (metadata) and StarRocks FE :9030 (data); Redis and Celery drawn dashed/greyed.

Speaker notes: Superset itself holds no data, only definitions. That is why the metadata DB, not the warehouse, is what "dashboard as code" must reproduce.

---
# Slide 5 — Datasets and metrics: the semantic layer
- A metric is a named SQL expression on a dataset; every chart reuses it, so the definition lives in one place

| Dataset | Metrics (expression) | Rule |
|---|---|---|
| gold_uc5 | not_understood_rate = SUM(not_understood_cnt) / NULLIF(SUM(understood_cnt + not_understood_cnt), 0) | a ratio of sums, never an average of daily rates |
| gold_uc8 | active_users = BITMAP_UNION_COUNT(users) | distinct users over any range: never a SUM of daily distincts |
| gold_uc8 | avg_new_per_day = SUM(new_users) / COUNT(DISTINCT event_date) | over calendar days (M3) |
| gold_uc7 | total_users = SUM(cumulative_users) | cumulative: read per day, never summed over days |
| gold_uc7 | change_pct = 100 × (SUM(cum) − SUM(before)) / NULLIF(SUM(before), 0) | NULL on a zero base (M2) |

- 5 datasets, 14 metrics, defined in datasets.yaml and loaded by the init Job; the dataset's columns must equal the dbt model's output columns (CI)

Visual: one metric definition box with arrows into four charts that use it.

Speaker notes: Where to put metric logic is a real design choice: in dbt (fixed per table) or in the BI semantic layer (recomputed per filter). Ratios and distinct counts must be recomputed per filter, so they belong here, as ratios of the pre-aggregated sums.

---
# Slide 6 — Native filters: defaults and scope
- A native filter targets one column of one dataset; Superset applies it to every chart in its scope whose dataset has that column
- Ours (D25):
  - Traffic on traffic_class (real / home / internal / unmatched), default real → UC4, UC5, UC6, KPI Predictions
  - Users on user_class (internal / external), no default → UC7, UC8
- Two filters because the questions count different things: UC4–6 count messages, UC7–8 count people (a person is internal or external; a message is /home or real)
- How Superset stores the scope: rootPath ROOT_ID (the whole dashboard) minus a list of excluded chart ids
- Consequence: a chart added later joins every filter automatically, unless excluded (E83). The check verifies scopes after every change

Visual: the dashboard grid with two coloured overlays: Traffic (blue) over the message charts, Users (green) over the user charts; "Data up to" in neither.

Speaker notes: The default matters as much as the filter. With all traffic the not-understood rate is 1.3 %; with real traffic it is 58 %. A dashboard that opens on "all" hides the problem.

---
# Slide 7 — Dashboard design rules (Stephen Few, applied)

| Rule | Applied |
|---|---|
| Most important first | KPI row: Predictions, Not-understood rate, Total users, Avg active / day, Data up to |
| Right chart for the question | trend → bars per day; ranking → sorted horizontal bars (UC6); share per day → 100 % stacked bars; one number → Big Number |
| Colour only for meaning | understood vs not; intent group (business / fallback / bot_khong_hieu) |
| Context on every number | filter state visible; freshness from data (MAX(event_date) = 2024-07-25, D23), not typed text |
| Honest values | NULL comparison shown as "No data" with an explanation, not 0 % or 100 % |

Visual: a wireframe of the dashboard: KPI row on top, then UC5, UC4, UC6, UC7, UC8 sections.

Speaker notes: The UC7 change % is the honest-value rule in practice: with 24 days of data there is nothing 30 days earlier, so the chart says "No data" and its description says why.

---
# Slide 8 — Dashboards as code
- Superset's export: a ZIP of YAML (databases, datasets with their metrics, charts, dashboard with layout and filters); passwords masked as XXXXXXXXXX
- Import matches objects by UUID: same UUID → update, new UUID → create
- Our rules:
  - Fixed UUIDs (uuid5) for the connection and the datasets, so an export re-imports onto the objects the init Job creates
  - The export lives in Git (infra/k8s/superset/assets/), reviewed in a PR like code; a check rejects secrets, wrong UUIDs, unreviewed charts
  - The init Job imports it on every start; then it re-sets the connection password (the export only has the mask)
  - Proof: import into an empty metadata database → same dashboard (the drill)
- Workflow for a change: build in the UI → add to dashboard_checks.yaml → make superset-export → PR → CI checks the export

Visual: UI → export (YAML) → Git PR → CI check → ZIP ConfigMap → init Job import → Superset; a loop arrow.

Speaker notes: Building charts in the UI and keeping the result in Git is the pragmatic middle way: hand-writing chart JSON is fragile, clicking without export is not reproducible.

---
# Part 2 — The stack of Task 5

---
# Slide 9 — Architecture

| Namespace | Runs |
|---|---|
| superset | Deployment superset (1 pod, Recreate; init containers wait-for-postgres + migrate); Job superset-init; pod superset-check on demand |
| lake | Postgres: database superset (Job superset-db-init, idempotent; in the daily pg-backup) |
| starrocks | unchanged; Superset connects to the FE as bi_ro |

- Image vnpt-lake-superset = apache/superset:6.1.0 (lean, by digest) + starrocks 1.3.4 + psycopg2-binary 2.9.13, built by CI, deployed by digest
- Config superset_config.py (ConfigMap): SECRET_KEY and DB from env, small pool, SQL Lab limits, no Redis/Celery
- Secrets: superset-env (web), superset-init-credentials (admin + bi_ro + checker: Job only), superset-checker-credentials (check pod only)
- Job superset-init: admin → connect → import from Git → connect → datasets + 20 checks → role gold_viewer + user checker

Visual: the three namespaces; inside superset the web pod with its two init containers, the Job, the check pod; arrows to Postgres and StarRocks.

Speaker notes: Migrations run in the web pod's init container: one replica plus the Recreate strategy means two migrations can never run at the same time.

---
# Slide 10 — How the pieces talk

| From → To | How | Credentials |
|---|---|---|
| browser → Superset | HTTP :8088 (port-forward) | admin (UI); checker (read-only, used by the API check) |
| Superset → Postgres | psycopg2 :5432 | role superset (Secret superset-env) |
| Superset → StarRocks FE | starrocks dialect over PyMySQL, MySQL protocol :9030 | bi_ro, password stored encrypted in the metadata DB |
| check pod → Superset | REST API: login (JWT), dashboard, charts, POST /api/v1/chart/data | user checker (Gamma + gold_viewer) |
| init Job → Superset / Postgres | Superset CLI + Python in an app context | admin, bi_ro, checker passwords (Job only) |

Speaker notes: Each component gets only its own secret: the web pod never sees the admin password, the check pod never sees anything but checker's login.

---
# Slide 11 — Why these, and not others

| Role | Ours | Alternatives | Why ours here |
|---|---|---|---|
| BI tool | Superset 6.1.0 | Metabase, Grafana, Power BI | named by the lab; open source; SQL Lab; semantic layer |
| Deployment | own kustomize manifests (D27) | Helm chart 0.22.8, Superset operator v0.3.0 | the chart is deprecated upstream and needed overrides for almost everything (E80); the operator is 0.x |
| Driver | starrocks dialect against Superset's own pins (D26) | plain pip install | CI proves none of Superset's 157 packages changed |
| Cache / async | none (D24) | Redis + Celery | one pod less; Gold is pre-aggregated |
| Admin | own Job, password from a Secret, verified (D21) | the chart's admin/admin | chart values end up in the release Secret (E35 in Task 2) |
| Check user | read-only checker (Gamma + datasets) | admin | proves a non-admin sees the dashboard |

Speaker notes: The deployment decision is the interesting one: when the overrides outnumber what you keep, owning 80 lines of YAML is simpler and safer than a deprecated chart.

---
# Slide 12 — Read-only at two layers, and modes
- Layer 1, StarRocks: bi_ro has SELECT on gold only → CREATE TABLE = Access denied (checked by the Job)
- Layer 2, Superset: the connection has DML / CTAS / CVAS / file upload off → SQL Lab refuses CREATE before StarRocks sees it ("does not allow for DDL/DML")
- checker: role gold_viewer = datasource access on the 5 datasets only

| Profile | Runs | Requests of 7,268 Mi |
|---|---|---|
| demo-task5 (D22) | view-only Airflow + StarRocks + Superset | **6,116 Mi (84 %)**, limits 136 % |
| + scheduler running | | ≈ 7.2 Gi: too tight (after a restart: mode-pipeline before k8s-up) |

Speaker notes: Superset web uses about 200 MiB with the dashboard open; the request of 1 GiB is the safety margin for Gunicorn under load.

---
# Part 3 — Running it: what the dashboard says, and how I know

---
# Slide 13 — What the dashboard says (Traffic = real, Users cleared)

| Question | Answer |
|---|---|
| UC5 not understood | **58.0 %** of real predictions (127 / 219); only 1.3 % of all (141 / 10,729) |
| UC4 scenarios | all real traffic is free text (NLU); the start scenario is only /home clicks (97.8 % of all) |
| UC6 top intents on real traffic | fallback 97 > xin_chao 92 > bot_khong_hieu 30 |
| UC7 total users | 89 (87 external + 2 internal); change vs 30 days: +89; change %: no data (M2) |
| UC8 users | 3.71 new / day, 4.67 active / day; 89 distinct active over the period (a per-day sum would say 112) |
| Freshness | data up to 2024-07-25 |

Visual: the dashboard screenshot (PDF export) with the 58.0 % Big Number circled.

Speaker notes: The finding for the business: the bot's real problem is invisible in the overall numbers because /home clicks are always "understood" and dominate the volume. On real questions it fails more often than it succeeds.

---
# Slide 14 — Three independent readings of every number
1. The init Job runs every dataset metric in StarRocks: 20 / 20 equal to Task 4's gold-check values
2. On screen: PDF exports with Traffic real / cleared and Users external / internal, read by hand
3. make superset-check: logs in as checker, reads each chart's own saved query through POST /api/v1/chart/data with each filter value, post-processing removed, compares unrounded: **33 / 33**
- Plus structure: Traffic default real; scopes = exactly 6 message / 7 user charts; Data up to in none; 14 / 14 charts reviewed
- Unrounded matters: 87 / 24 = 3.625 shows as 3.63; my first expected value "3.62" was Python's float rounding, Superset was right

Visual: three columns Job · screen · API, each with a tick, all pointing at the same table of values.

Speaker notes: The API check is the one that runs every time: it is part of make demo-check DEMO_TASK=5. The screen check found the cases the API cannot: a missing default, an unreadable label.

---
# Slide 15 — The drill: rebuild the dashboard from Git
- Kustomize overlay infra/k8s/superset-drill/: the real manifests, renamed *-drill, DB_NAME = superset_drill in every container (CI checks it)
- make superset-drill: real web to 0 → scratch database superset_drill → migrate → Job: admin, connect, import from Git, connect, datasets → superset-check against the drill

| Evidence | Result |
|---|---|
| metadata db | superset_drill only |
| dashboard | same uuid 3a0a5a6e…, 14 charts, published |
| filter scopes | excluded ids [1, 8, 10, 9, 14, 12, 11, 13] vs real [13, 14, 3, 4, 5, 6, 7, 8]: the importer remapped them, same chart names |
| superset-check | PASS (33 values + structure) |

- make superset-drill-clean: only *-drill objects and the scratch database removed; the real Superset untouched

Visual: two Superset boxes side by side, "real" and "drill", both fed by the same Git folder; the drill's database box labelled "empty → same dashboard".

Speaker notes: This is the proof that the dashboard is code, not a database row: an empty database plus Git gives the same dashboard, filters and numbers.

---
# Slide 16 — Demo profile and freeze
- make demo-task5 → make demo-check DEMO_TASK=5: ALL PASS (8 workloads ready, 4 UIs 200, 5 × task4_v1 via StarRocks, 22 × gold-check, superset-check, 2 asset DAGs paused)
- Freeze: Task 5 adds no data; it shows task4_v1 through the Task 4 dbt image; the dashboard = Git (assets/), the superset DB is in the daily backup but not needed to rebuild
- Evidence outside Git: state files (80 KB) + 5 PDF exports (516 KB)
- Git tag task5-v1

Speaker notes: All five tasks are frozen with tags and can be shown from the same cluster, switching profiles between parts.

---
# Part 4 — Errors

---
# Slide 17 — Errors and fixes (E79–E87)

| Error | Symptom | Lesson |
|---|---|---|
| E79 | CI: uv cache "Permission denied", then "a Superset package was changed" | build steps as root leave root-owned files in the runtime user's home; a comparison must first prove its inputs are valid |
| E80 | chart render: deprecated, a Celery worker always rendered, admin defaults | render before install; check a chart's status, not only its version |
| E81 | 'tcp' is not a valid port number | Service "superset" injects SUPERSET_PORT=tcp://…: disable service links |
| E82 | No module named 'MySQLdb' on valid queries only | a check must take the user's code path (Superset's result typing) |
| E83 | 33/33 values PASS, 9 scope FAILs | read the stored structure before interpreting it; exclude-list scopes catch new charts |
| E84 | CI import-order error my local lint missed | a folder named like a package changes tools; a cached lint is no check |
| E85 | permission warnings on every connection | read what a library does on failure; silence narrowly |
| E86 | kustomize "cycle detected" | overlays live beside their base |
| E87 | an evidence file missing | commas split $(if …) arguments in Make; check files exist |

Speaker notes: Five of the nine were my own wrong assumptions, caught by CI or a check before they reached the dashboard. That is the point of building the checks first.

---
# Slide 18 — Debugging story: E81 and E82, two hidden defaults
| | E81 | E82 |
|---|---|---|
| Symptom | pod CrashLoopBackOff: 'tcp' is not a valid port number | SQL Lab: a wrong column showed StarRocks' error, a correct query showed No module named 'MySQLdb' |
| Clue | the crash came from Gunicorn's bind, not Superset | only successful queries failed |
| Cause | Kubernetes injects <SERVICE>_PORT=tcp://ip:port for every Service; run-server.sh reads SUPERSET_PORT | MySQLEngineSpec.get_datatype imports MySQLdb (mysqlclient, a C extension, not in the image) to type result columns |
| Fix | enableServiceLinks: false + explicit SUPERSET_PORT | PyMySQL's install_as_MySQLdb() shim in the config; CI checks the shim, the Job calls get_datatype in a real app |
| Lesson | defaults of the platform collide with the app's variables | a raw SQL check proves the connection, not the BI tool's result path |

Speaker notes: Both were invisible in a quick test: the pod looked like it was starting, and the connection check passed. Only the user's path showed them.

---
# Slide 19 — Known limits and next steps
- 24 days of data: UC7's 30-day comparison is "No data" by definition
- Leader questions: scenario names for STEP 406665/406666, M2/M3, default filter, who views the dashboard (roles, row-level security)
- One Superset pod, no cache: fine for a demo; production would add replicas, Redis, Celery for reports
- Next: the final presentation from one cluster (Tasks 2–5, switching profiles); after it the one-time from-zero acceptance and the cleanup

Speaker notes: The lab ends with a full chain: Excel → Bronze → Silver → Gold → dashboard, every layer reconciled with independent numbers, every task frozen and reproducible from Git.

---
# Appendix A1 — Versions

| Component | Pin | Why |
|---|---|---|
| Superset | 6.1.0, apache/superset@sha256:16b50bbe… (lean) | current release |
| starrocks dialect | 1.3.4, against Superset's own pins (SQLAlchemy stays 1.4.54) | D26 |
| psycopg2-binary | 2.9.13 | metadata DB driver |
| Image | vnpt-lake-superset@sha256:a0307416… (built from 4662349) | CI → GHCR, by digest |

---
# Appendix A2 — How to reproduce
- After a restart: minikube start && make mode-pipeline && make k8s-up && make demo-task5 && make demo-check DEMO_TASK=5
- make superset-ui (http://localhost:8088) · make superset-check · make superset-export (review the diff in a PR)
- make superset-drill · make superset-drill-clean · make superset-up
- Docs: docs/task5/README.md, FREEZE.md, 01_knowledge.md, 06_validation.md, 07_demo.md, 05_errors_and_fixes.md

Speaker notes: Everything here is reproducible from the repository at tag task5-v1.
