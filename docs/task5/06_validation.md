# Task 5 — Validation: every number on screen

Expected values: Task 4 seeds (`dbt/seeds/expected_gold_*.csv`), definitions `docs/metrics.md`. Read each number from the dashboard (hover / "View as table"), not from SQL Lab.

| Chart | Number | Filter `real` | Filter all | Expected (real / all) | Status |
|---|---|---|---|---|---|
| KPI | client messages | | | / 10,613 | |
| KPI | distinct users | | | | |
| KPI | understood % | | | | |
| UC4 | messages on 2024-07-24 | | | | |
| UC5 | understood / not on 2024-07-24 | | | | |
| UC6 | top intent + count | | | | |
| UC7 | users last window / previous | | | | |
| UC8 | distinct users 07-20 → 07-25 (bitmap, not sum) | | | | |
| Freshness | max `event_date` | 2024-07-25 | 2024-07-25 | 2024-07-25 | |

## As code

| Check | Result |
|---|---|
| Export → `superset/assets/` contains no password (`grep -ri password superset/assets`) | |
| Delete dashboard → import Job → same charts and filter | |
| Fresh `superset` DB → init Job → same dashboard | |
