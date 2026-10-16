# Task 1 — Plan

**Scope (from the lab doc):** stand up MinIO · Spark reads the 2 Excel files · writes them as Iceberg tables in MinIO.
**Leader's guidance:** each task is a standalone mini-project on the same data; understand what happens, what can fail, how to fix it, and the theory.
**Out of scope for Task 1:** K8s, Airflow, Trino, StarRocks, Silver/Gold (Silver is a stretch goal).

| # | Step | Done when | Time (approx.) |
|---|---|---|---|
| 1.0 | Prereqs: Docker Desktop (WSL2 backend), `.wslconfig` (memory=11GB, processors=8, swap=4GB), Ubuntu WSL terminal with `make`, VS Code + WSL extension, Git repo on GitHub with `.gitignore` | `docker run hello-world` works in WSL | 1 h |
| 1.1 | Copy the Excel files to `~/data/vnpt-lab/` (WSL filesystem); fill `.env` | Files visible; `.env` not tracked by Git | 15 min |
| 1.2 | `make up` → MinIO + bucket | Console at `localhost:9001` shows bucket `lakehouse` | 30 min |
| 1.3 | Postgres + Hive Metastore healthy | Metastore log shows `Starting Hive Metastore Server` with no schema errors; port 9083 open | 1–2 h (likely errors E03–E05) |
| 1.4 | Spark image builds; `make sql` → `SHOW NAMESPACES IN lake;` | Query returns without catalog errors | 1 h |
| 1.5 | Smoke test: `CREATE TABLE lake.tmp.t (id int) USING iceberg; INSERT 1 row; SELECT` | Row returns; files visible in MinIO | 30 min |
| 1.6 | `make bronze DATE=<today>` | Counts match `06_validation.md` | 1 h |
| 1.7 | Rerun the same DATE | Counts unchanged; a new snapshot exists (overwrite, not append) | 15 min |
| 1.8 | Explore Iceberg: `.snapshots`, `.files`, `.partitions`, time travel, rollback | Screenshots/notes in `04_process.md` | 1 h |
| 1.9 | Failure drills: stop MinIO mid-write; wrong password; stop Metastore | Each logged in `05_errors_and_fixes.md` with cause + fix | 1 h |
| 1.10 | (Stretch) Silver: parse timestamps, `final_intent`, flags | Silver counts match `06_validation.md` | 2–3 h |
| 1.11 | Write `docs/task1/README` summary for the leader: architecture, theory, results, errors met | Readable in 5 minutes | 30 min |

**Task 1 risks:** jar or version mismatch (Metastore ↔ S3A, Spark ↔ Iceberg) · MinIO image availability · Windows path and line-ending issues (`core.autocrlf=input`) · RAM if other apps are open (check `make stats`).
