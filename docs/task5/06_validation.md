# Task 5 — Validation: every number on screen

Expected values: Task 4 seeds (`dbt/seeds/expected_gold_*.csv`), definitions `docs/metrics.md`, gold-check. Three
independent readings of the same numbers: (1) the Job runs every dataset metric in StarRocks (20 checks); (2) Bee
read the dashboard on screen (PDF exports, evidence/task5); (3) `make superset-check` reads every chart through
Superset's API as the non-admin `checker`, unrounded, with each filter value (33 checks + structure).

## 1. Dashboard values (filter cleared / real · Users cleared / external / internal)

| Chart | Expected | Screen (PDF) | API (`superset-check`) |
|---|---|---|---|
| KPI · Predictions | 10,729 / **219** | 219 (real) | ✅ both |
| UC5 · Understood vs not (sums) | 10,588 + 141 / **92 + 127** | bars ≤ 60, mostly not understood (real) | ✅ both |
| UC5 · Not-understood rate | **1.3 %** (0.013142) / **58.0 %** (0.579909) | 1.3 % / 58.0 % | ✅ both |
| UC4 · Messages by scenario | start 10,494 (97.8 %) + free text 235 / free text 219 (100 %) | one slice free text (real) | ✅ both |
| UC4 · Scenario share per day | same sums; each day 100 % | 100 % free text (real) | ✅ both |
| UC6 · Top intents | xin_chao 10,588, fallback 107, bot_khong_hieu 34 / fallback 97, xin_chao 92, bot_khong_hieu 30 | fallback > xin_chao > bot_khong_hieu (real) | ✅ both |
| Data up to | 2024-07-25 | 2024-07-25 | ✅ |
| UC7 · Total users (last day) | 89 / 87 / 2 | 89 / 87 / 2 | ✅ 3 |
| UC7 · Change vs 30 days | +89 / +87 / +2 | +89 / +87 / +2 | ✅ 3 |
| UC7 · Change % | NULL (M2) | "No data … NULL for the latest time record" | ✅ 3 (None) |
| UC8 · New and active per day (sum new) | 89 / 87 / 2; peak active 28 on 07-18 | peak ≈ 28 | ✅ 3 |
| UC8 · Avg new users / day | 3.708333 / **3.625** / 0.083333 | 3.71 / 3.63 / 0.08 | ✅ 3 |
| UC8 · Avg active users / day | 4.666667 / 4.416667 / 0.25 | 4.67 / 4.42 / 0.25 | ✅ 3 |
| UC8 · Distinct active users (bitmap) | 89 / 87 / 2 (sum of daily values would give 112 / 106 / 6) | 89 / 87 / 2 | ✅ 3 |

Note: 87/24 = 3.625 shows as 3.63; Claude's first expected value "3.62" came from Python's float rounding. The API
check compares unrounded values for that reason.

## 2. Filters and scoping (read from the dashboard's stored configuration)

| Check | Result |
|---|---|
| Traffic: column `traffic_class`, default `['real']` | ✅ |
| Traffic scope = KPI Predictions, UC4 ×2, UC5 ×2, UC6 | ✅ (stored as ROOT_ID minus 8 excluded chart ids) |
| Users: column `user_class`, no default; scope = 7 UC7/UC8 charts | ✅ |
| Data up to in no filter's scope | ✅ |
| Every chart on the dashboard is in `dashboard_checks.yaml` and vice versa | ✅ 14/14 |

## 3. Access

| Check | Result |
|---|---|
| bi_ro `CREATE TABLE` (Job, through Superset's connection) | Access denied ✅ |
| SQL Lab `CREATE TABLE x` | "This database does not allow for DDL/DML" ✅ (Superset's own barrier) |
| checker (Gamma + gold_viewer) sees the published dashboard and its data | ✅ (all checks run as checker) |

## 4. As code

| Check | Result |
|---|---|
| Export has no password / secret keys, URI masked `XXXXXXXXXX` (`tools/superset_assets.py check`, unit test on the commit) | ✅ |
| Dataset and connection uuids are the fixed uuid5 values | ✅ |
| Import onto the same Superset is idempotent (password restored, scopes intact) | ✅ superset-check PASS |
| **Fresh metadata DB → same dashboard** (drill on `superset_drill`) | ✅ uuid `3a0a5a6e…`, 14 charts, scopes remapped (excluded `[1, 8, 10, 9, 14, 12, 11, 13]` vs real `[13, 14, 3, 4, 5, 6, 7, 8]`), superset-check PASS |
