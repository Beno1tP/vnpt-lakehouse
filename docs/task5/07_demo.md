# Task 5 — Demo script

Before: `make demo-task5` (then nothing else runs: scheduler 0, asset DAGs paused), `make demo-check DEMO_TASK=5`
must print ALL PASS, then `make superset-ui` in a second terminal.

After a laptop restart use this order (2026-11-13): `k8s-up` restarts the Airflow scheduler/dag-processor/triggerer;
with StarRocks and Superset still up that is ≈ 7.2 Gi of 7,268 Mi requests, so Airflow's migration Job waited
Pending ~70 s for memory. Scale the engines down first:

```bash
minikube start && make mode-pipeline && make k8s-up && make demo-task5 && make demo-check DEMO_TASK=5
```

Fallback: the PDFs in `~/data/vnpt-lab/evidence/task5/`.

| # | Minutes | Show | Say |
|---|---|---|---|
| 1 | 2 | Root README diagram: landing → Bronze → Silver (Spark/Iceberg, Airflow) → Trino / StarRocks Gold (dbt) → Superset | one copy of the data in Iceberg; every engine reads it through one HMS; Gold is rebuilt from a frozen tag |
| 2 | 2 | Dashboard with Traffic = real, then clear the filter | not understood 58 % of real traffic vs 1.3 % overall: `/home` clicks hide the problem; all real traffic is free text, fallback is its top intent |
| 3 | 2 | Users filter external / internal; UC8 distinct users 89 vs the per-day sum 112 | distinct users are not additive: bitmap union in StarRocks; UC7 % is "No data" by definition (24 days of data, M2) |
| 4 | 2 | `make superset-check` in a terminal | every number on screen is checked through Superset's API as a read-only user, filters on and off, plus the filter scoping |
| 5 | 1 | `infra/k8s/superset/assets/` in Git + the drill result (FREEZE.md §4) | the dashboard is code: rebuilt from Git on an empty database, same dashboard |
| 6 | 1 | What I'd improve | leader questions (scenario names, M2/M3, roles/RLS), event-driven landing, off-laptop backups |

## Failure drill (optional): Gold unavailable

| Step | Expected |
|---|---|
| `kubectl -n starrocks delete pod starrocks-be-0` during the demo | charts show a query error, not stale or zero numbers |
| the StatefulSet recreates the BE (data on its PVC) | after ~1–2 min (`make starrocks-ready`), refresh → same numbers; `make superset-check` PASS |

Not run before the freeze (it would restart StarRocks during the frozen state); run it only as a live demo.
