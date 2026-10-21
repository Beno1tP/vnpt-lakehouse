# Task 1 — Operations runbook (how to run, check and debug the stack)

For whoever **runs** the stack: Bee, the leader, and later the Airflow DAG (Task 2). How the stack was built: `08_standard_build.md`. Every command typed so far, with error branches: `09_command_paths.md`.

## 1. Operator interface

Run `make` (= `make help`) from the repo root in WSL. Every target is idempotent unless marked **destructive**.

| Target | What it does | Notes |
|---|---|---|
| `make build` | Build the 2 thin images (`vnpt-lake-spark:…-r1`, `vnpt-lake-hms:…-r1`) | Only after a version change in `docker/*/Dockerfile` or `requirements.txt`; then `docker builder prune -f` |
| `make secrets` | Render `hive-site.xml` + `pg_password` from `ENV_FILE` into `infra/compose/secrets/` (mode 600) | Run automatically by `make up` |
| `make up` | Start everything (`--remove-orphans`) | Also the fix after a reboot (E23) |
| `make smoke` | `SHOW NAMESPACES IN lake` must return `default` | Proves Spark → Iceberg → HMS → Postgres |
| `make bronze DATE=…` | Load / reload one `_ingest_date` partition | Fails (non-zero) if rows written ≠ rows read |
| `make validate DATE=…` | 17 checks against `tests/expected/bronze.json` (+6 with `VALIDATE_ARGS=--check-idempotency`) | PASS/FAIL per check, exit non-zero on any FAIL |
| `make acceptance DATE=…` | smoke → load → validate → reload → validate + idempotency | Keeps data; adds 2 snapshots |
| `make acceptance-from-zero CONFIRM=yes DATE=…` | **Destructive**: clean → up → acceptance | Refuses without `CONFIRM=yes` |
| `make ps` · `make stats` · `make logs SVC=…` · `make sql` | Status, memory, logs, SQL shell | |
| `make down` | Stop, keep data | |
| `make clean` | **Destructive**: stop and delete both data volumes | |

Variables (defaults = local profile; override per run, e.g. `make bronze DATE=2026-10-16 SPARK_MASTER=local[4]`):

| Variable | Default | Meaning |
|---|---|---|
| `ENV_FILE` | `infra/compose/.env` | Versions, image pins, credentials (git-ignored) |
| `DATE` | today | Ingest date (partition) to load / validate |
| `SPARK_MASTER` | `local[2]` | Spark master (local mode now; cluster in Task 2) |
| `SPARK_DRIVER_MEMORY` | `2g` | Driver heap (container limit 3g) |
| `SPARK_DEFAULTS_FILE` (in `.env`) | `./spark/spark-defaults.conf` | Per-environment Spark config (catalog URI, S3 endpoint) |

## 2. Daily procedure

```bash
cd ~/vnpt-lakehouse
make up && make smoke                       # after boot: start + prove the chain answers
make bronze DATE=2026-10-16                 # load
make validate DATE=2026-10-16               # check
make acceptance DATE=2026-10-16             # full proof (before a demo, after a change)
```

## 3. How validation works

```
tests/sql/bronze_checks.sql      15 blocks "-- check: <table>.<key>" → one number each (counts only)
tests/expected/bronze.json       expected value per <table>.<key> (from 06_validation.md)
jobs/validate_bronze.py          runs each check, + column counts, + (optional) last-snapshot assertions
                                 → "[validate] PASS|FAIL name expected=… actual=…", exit 1 on any FAIL
jobs/bronze_ingest.py            in-job assertion: raise if rows in partition ≠ rows read
```

Two levels of assertion: the **job** refuses to report success for a wrong load; the **validator** checks the business numbers and the commit (idempotency: latest snapshot `overwrite`, added = deleted = expected rows; `both.distinct_batch_ids = 1` catches the drill-B case where the two tables come from different loads).

**Add a check:** one block in `bronze_checks.sql` (`-- check: chatlog.my_key` + a query returning one number) and one key `"my_key": <value>` in `bronze.json`. The validator fails if a check has no expected value.

**Vacuous checks:** on an empty partition, "0 non-null values in empty columns" and the column counts still pass. That is why every table also has a non-vacuous check (`rows`, `distinct_id`); keep at least one per table when adding checks.

## 4. When something fails

| Symptom | Likely cause | Do |
|---|---|---|
| `make smoke` prints nothing / fails | HMS or Spark not running (after a reboot: E23) | `make ps` → `make up` → `make smoke` |
| `hive-metastore` not `(healthy)` | Postgres down, wrong secret, schema problem | `make logs SVC=hms-schema-init`; `$DC logs hive-metastore 2>&1 \| tail -30` |
| `hms-schema-init` exit 1 with "refusing to init" | `-info` failed for a reason other than "no schema" (connection, auth, other version) | Read the printed error; fix Postgres/secret; never force `initSchema` on an existing schema |
| `make bronze` raises "rows in partition != rows read" | The write did not land completely | Rerun `make bronze` (idempotent); if it repeats, check MinIO/HMS logs |
| `validate` FAIL on `rows`/`distinct_*` | Wrong or changed source file, wrong `DATE` | Check `RAW_DATA_DIR`, the source files, the `DATE` |
| `validate` FAIL on `both.distinct_batch_ids` | Crash between the two table commits (drill B) | Rerun `make bronze DATE=…`, then `make validate` |
| `validate` FAIL on `last_snapshot_*` | Rerun appended instead of replacing, or another writer committed | Inspect `SELECT * FROM lake.bronze.<t>.snapshots`; do not run two loads at once |
| Background `docker exec` "Stopped" | Reads the terminal (E22) | `exec -T … < /dev/null` (the make targets already do) |

## 5. Results (first runs, 2026-10-21)

| Run | Result |
|---|---|
| `make acceptance DATE=2026-10-16` | smoke OK → load → **17/17 PASS** → reload → **23/23 PASS** → `[acceptance] PASSED`, exit 0 |
| Negative test `make validate DATE=2026-10-17` (no data) | **13 FAIL**, `4/17 checks passed` (the 4 vacuous ones), exit 2 → the gate really stops a run |
| `acceptance-from-zero` without `CONFIRM` | Refuses, exit non-zero |

## 6. Local now, production later

| Concern | Local (now) | Production (Task 2) |
|---|---|---|
| Where it runs | Docker Compose, Spark `local[2]` | Kubernetes, Spark cluster mode (Spark Operator) |
| Settings | Make variables + `.env` + `SPARK_DEFAULTS_FILE` | Helm values / ConfigMaps per environment |
| Credentials | Compose secret files (HMS, Postgres); MinIO/S3 keys still env | Kubernetes Secrets mounted as files |
| Images | Built locally (`make build`) | Built by CI, pushed to GHCR, referenced by digest |
| Acceptance | `make acceptance` | Airflow DAG: load task → validate task (same scripts, exit code = task status) |
