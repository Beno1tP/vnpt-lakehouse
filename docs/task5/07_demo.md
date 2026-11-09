# Task 5 — Demo script and failure drill (step 5.7)

Before: `make mode-serving`; open Superset; screenshots in `docs/task5/img/` as fallback.

| # | Minutes | Show | Say |
|---|---|---|---|
| 1 | 2 | Root README diagram: landing → Bronze → Silver (Spark/Iceberg, Airflow) → Trino / StarRocks Gold (dbt) → Superset | one copy of data in Iceberg; engines read it through one HMS |
| 2 | 2 | Dashboard, filter `all` → `real` | the data finding: ~97.8 % of client messages are `/home`; real traffic is small |
| 3 | 3 | One number traced through layers (UC5 on 07-24): Superset → `gold_uc5` → `fct_user_message` → Silver (Trino) → Bronze → pandas expected | reconciliation at every layer, automated as tests |
| 4 | 2 | Drill (below) | failure is visible and recoverable |
| 5 | 1 | What I'd improve | event-driven landing, off-cluster backups, RLS, Gold back to Iceberg (leader Q) |

## Failure drill: Gold unavailable

| Step | Expected |
|---|---|
| `kubectl -n starrocks delete pod <be-pod>` during the demo | charts show a query error (not stale or zero numbers) |
| operator recreates the BE; data on its PVC | after ~1 min, refresh → same numbers |
| Result | |
