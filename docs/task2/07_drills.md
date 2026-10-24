# Task 2 — Failure drills

| Drill | How | Expect | Result |
|---|---|---|---|
| Kill driver pod mid-run | `kubectl -n spark-jobs delete pod <driver>` | SparkApplication `FAILED` → Airflow retry → success, no duplicates | |
| Delete an executor pod | `kubectl -n spark-jobs delete pod <exec>` | Spark requests a new executor, job finishes | |
| Restart scheduler mid-run | `kubectl -n airflow rollout restart …` | Run resumes / task re-adopted; no double SparkApplication | |
| Backfill twice | `make backfill FROM=2024-07-02 TO=2024-07-25` ×2 | identical per-day counts | |
| Forced failure → alert (2.6) | bad date param | one Telegram message, no data contents | |
