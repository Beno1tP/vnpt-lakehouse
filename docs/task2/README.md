# Task 2 — Spark on Kubernetes (Minikube), orchestrated by Airflow

The Task 1 Bronze job runs on **Minikube** through the **Kubeflow Spark Operator**, one run per day of data, scheduled, retried, validated and alerted by **Airflow 3.3.2**. Images are built by **GitHub Actions**, stored in **GHCR** and deployed **by digest**; every change went through a PR with green CI. The whole stack comes up from zero with two commands.

Status (2026-10-27): ✅ done. From zero (`minikube delete` → `make k8s-up` → `make k8s-demo`): 24 daily runs, 18 days loaded with exact counts (23/23 checks each), 6 empty days skipped, 0 failed runs, 0 scheduler restarts. Tag `task2-v1`. Silver is the first step of Task 3 (decision D6).

## Architecture

```
GitHub (private repo)                         PR → CI: ruff + format, 52 unit tests, Dockerfile checks,
                                                    kustomize render (+ digest guard), DAG integrity in the Airflow image
                                              main → images.yml: build → Trivy (report) → ghcr.io/beno1tp/<image>:<git sha>
                                                     digest → PR bumps infra/k8s/images.env → make k8s-up / airflow-install
Minikube v1.39.0 · K8s v1.37.0 · 8 GiB, Allocatable 7268 Mi (E25) · containerd
  lake           postgres 16 (DBs metastore + airflow) · minio · hive-metastore 3.1.3 (schema-init initContainer)
                 Jobs bucket-init, minio-users (6 service users + policies) · CronJob pg-backup (01:00 UTC → s3://backup)
  spark-operator Kubeflow Spark Operator 2.2.1 (controller + webhook), pinned by digest
  spark-jobs     SparkApplication bronze / validate / maintenance → driver pod + 1 executor (image vnpt-lake-spark-job)
  airflow        Airflow 3.3.2 (chart 1.22.0, LocalExecutor, parallelism 2) · image vnpt-lake-airflow (DAGs + plugin baked)
                 task logs → s3://airflow-logs · Telegram alerts (failure after last try + 2 h DeadlineAlert)

DAG chatbot_bronze (daily, CronDataIntervalTimetable 00:00 UTC, max_active_runs 1, dagrun_timeout 3 h)
  check_landing (S3, skip if no export) → bronze (SparkKubernetesOperator, retries 2) → validate (23 checks, retries 1) → notify
DAG lake_maintenance (Sunday 02:00 UTC): expire_snapshots (7 d, keep 5) · rewrite_data_files · rewrite_manifests ·
  remove_orphan_files (72 h) · row-count assert · time-travel check

Source: landing simulator splits the two Excel files by the day written in CREATED_TIME → s3://landing/<table>/dt=YYYY-MM-DD/
```

## Run it

Prerequisites: Windows + WSL2 Ubuntu 22.04, Docker Desktop (WSL integration), minikube v1.39.0, kubectl, helm, gh ≥ 2.17 (E50), the two Excel files in `~/data/vnpt-lab/`, a GHCR token with **only** `read:packages`. Stop the Task 1 Compose stack first (`make down`): both share the 10 GiB WSL VM.

```bash
cp infra/k8s/secrets.env.example infra/k8s/secrets.env   # set every change-me value (passwords, Telegram, GHCR_PULL_TOKEN)
make k8s-up      # cluster + secrets + lake + operator + Airflow + admin user; idempotent (rerun = ~20 s)
make k8s-demo    # landing → backfill 2024-07-02..07-25 → wait → per-day report → fails on any failed run or scheduler restart
make airflow-ui  # http://localhost:8080, user admin (password from secrets.env)
```

Freeze (B2): `freeze TASK=2` / `freeze-check TASK=2` (Iceberg tag `task2_v1`, rows = expected), `evidence`, `evidence-export` (copies to `~/data/vnpt-lab/evidence/`, outside Git). Demo (freeze step 1, 11-09): `minio-ui` (user `demo-viewer`, read-only), `history-ui` (Spark History Server, event logs on PVC `spark-events`), `airflow-viewer` + `airflow-viewer-check` (Viewer role). Day-2 targets (all in `make help`): `backfill [FROM TO REPROCESS DRY]`, `dag-runs`, `dag-report`, `sparkapp APP= DATE=`, `maintenance [DRY=yes]`, `backup-now`, `restore-metastore TS=`, `catalog CMD=`, `drill-driver|executor|scheduler`, `s3-access-test`, `ghcr-prune [CONFIRM=yes]`, dev: `venv lint fmt test all hooks`. Rerun one day = **clear** its run (`airflow tasks clear chatbot_bronze --start-date D --end-date D --yes`), not a new trigger (E49). In `make dag-runs` the run for day D is named `…__D+1` (run_id follows `run_after` = interval end); its `logical_date` = `ds` = D (E38). After a laptop restart: `minikube start` first (E40).

## Versions and why

| Component | Pin | Why |
|---|---|---|
| Minikube / Kubernetes | v1.39.0 / v1.37.0 | newest at start; kubectl 1.37.1 within skew |
| Spark Operator | 2.2.1, `controller:2.2.1@sha256:865ff4da…` | last release built on Spark 3.5 (2.3+ = Spark 4.0); its own spark-submit must not be 4.0 against our 3.5.9 jobs |
| Airflow / chart | 3.3.2 / 1.22.0 (D1) | no chart ships 3.3 yet; 1.22.0 only gates templates at `>=3.0.0`, so 3.3.2 renders the same objects; chart from the Apache CDN + SHA-512 (E29) |
| Provider cncf.kubernetes | 10.22.0 (in the image) | the constraint-tested set of 3.3.2 |
| Spark / Iceberg / HMS | 3.5.9 / 1.11.0 / 3.1.3 | unchanged from Task 1 (HMS 4 breaks Spark 3.5's Hive client) |
| Images | `vnpt-lake-{spark,hms}` version-tagged bases, `vnpt-lake-{spark-job,airflow}` tagged with the git SHA; deployed by digest (`infra/k8s/images.env`) | no laptop builds (D5); a digest cannot move |

## Decisions

| ID | Decision | Alternative rejected |
|---|---|---|
| D1 | chart 1.22.0 + Airflow 3.3.2 | Airflow 3.2.2 (chart default) |
| D2 | daily runs over the calendar span 07-02 → 07-25: 18 load, 6 skip (`AirflowSkipException`) | 18 manual triggers; one full-file run |
| D3 | expected per-day counts from Spark SQL on the validated Task 1 tables | a new tool (DuckDB) |
| D5 | GitHub Actions → GHCR → Minikube pulls (private, `read:packages` token) | local build + `minikube image load` (kept as fallback `k8s-job-image`) |
| D6 | Silver = Spark job at the **start of Task 3**, before Trino (to confirm with the leader) | Silver in this DAG now; dbt staging only |
| — | one retry layer: Airflow retries, SparkApplication `restartPolicy: Never` | retries at both levels (3 × 3) |
| — | loading a day is explicit (logical date / backfill); runs without one fail fast (E47) | a `day` field in the trigger form |
| — | LocalExecutor, `parallelism: 2`, scheduler 1 Gi request / 1.5 Gi limit (E52) | Celery/KubernetesExecutor (more pods, more RAM) |

## Deviations from the lab document

| Lab doc | Here | Reason |
|---|---|---|
| Minikube `--memory=7168 --cpus=4` | `--memory=8192 --cpus=6` + `kubelet.system-reserved=memory=3686Mi` | Airflow (4 components) + lake + one Spark run need ~6.6 Gi of requests; the reservation makes Allocatable match the real 8 GiB cap (E25) |
| Medallion Bronze → Silver → Gold | Task 2 = Bronze on K8s under Airflow | step 2's goal is "the step-1 job on Kubernetes, orchestrated by Airflow"; Silver opens Task 3 (D6) |
| pytest topic "watermark" | not coded | Airflow data intervals + idempotent per-day `overwritePartitions` replace a stored watermark |
| Airflow "latest" chart | chart 1.22.0 with Airflow 3.3.2 | D1 |

## Resource footprint (measured 2026-10-27, after the from-zero demo)

RAM: node 4,393 Mi used idle (60 % of Allocatable). Largest pods: scheduler 615 Mi (during a maintenance task), HMS 517, triggerer 516, dag-processor 393, kube-apiserver 379, api-server 353, Postgres 117, MinIO 106, operator 93. Node memory **requests**: 4,036 Mi idle (3,524 measured + 512 for the E52 scheduler raise). A bronze run adds driver 2 Gi (1g heap + 1g overhead for pandas) + executor 1.1 Gi → **7,236 of 7,268 Mi**: it fits with 32 Mi to spare, so a second Spark app at the same moment (e.g. maintenance) waits `Pending` until the first ends (safe, not an OOM). validate needs 2.6 Gi, maintenance 2.6 Gi.

**Proof that Airflow orchestrates and Spark works** (10-27, rerun of 07-24 by `tasks clear`, `04_process.md`): node memory requests **4,036 Mi idle → ~7,236 Mi while bronze runs → 4,036 Mi after**; the `spark-jobs` namespace is empty before and after (`delete_on_termination`), while the Airflow pods stay ≈ 1.6 Gi and flat. Result unchanged: 12,970 / 6,485, 23/23. Use node *requests* as evidence: metrics-server samples too slowly for 20–60 s Spark pods. CPU requests show ~8 allocatable cores despite `--cpus=6` (CPU side of E25, harmless).

Disk (Docker Desktop's `docker_data.vhdx`): Minikube node volume **8.3 GB** (node image store + PVC data), kicbase image ~1.9 GB (part of 7.6 GB `Images`, which also holds the Task 1 images), `~/.minikube` 0.86 GB, dev `.venv` 0.33 GB → **≈ 11 GB for Task 2**. First pull of the Airflow image from GHCR took ~10 min. Cleanup after the task: `minikube delete` (frees the node volume, **deletes all data**), `docker builder prune`, compact the vhdx (Task 1 `04_process.md` 1.15).

## Results (details: `06_validation.md`)

| Check | Result |
|---|---|
| One day / big days via SparkApplication | 12/6 for 07-02; 12,970/6,485 and 7,552/3,892 (116 unmatched) for 07-24/25 |
| Backfill 24 days, twice | identical per-day counts, 23/23 ×18, 6 skipped |
| Day-generic DQ rules (13) + exact reconciliation (10) | 23 checks per day, all pass |
| Least privilege | `make s3-access-test`: allowed and **denied** operations per MinIO user |
| Automation (nobody triggered) | daily `chatbot_bronze` scheduled run (empty day skipped) ✅, weekly `lake_maintenance` ✅, `pg-backup` CronJob ✅ (10-29 slot, caught up 4 h late after sleep; 10-28 missed, cluster down) (`06_validation.md` §3) |
| From zero | `k8s-up` 12.5 min (run 1; run 2 longer: Airflow image pull ~10 min) + `k8s-demo` 16 min, PASSED, 0 restarts |

## Failure drills (details: `07_drills.md`)

| Drill | Outcome |
|---|---|
| 1 Kill the driver mid-run | Airflow retry after 5 min, new SparkApplication, exact counts |
| 2 Delete the executor | Spark replaces it in the same attempt, no Airflow retry |
| 3 Restart the scheduler | first failed (E41: 10 s grace) → 120 s grace + validate retries 1 → success |
| 4 Backfill twice | identical |
| 5 Forced failure / deadline | one ❌ + one ⏰ in Telegram, metadata only |
| 6 Lose the catalog DB | Spark: table not found, files intact → restore from `pg_dump` → 23/23 |
| 7 Re-register from `metadata.json` | a commit newer than the backup (RPO) recovered with `register_table` |
| 2.5c Maintenance | snapshots 50 → 6, files 50 → 22, metadata 50 → 11, rows unchanged, old time travel gone (expected) |
| From zero ×2 | run 1 found E52 (scheduler OOM ×9), run 2 clean |

## Production gaps (Task 1 review, `03_plan.md` §H)

| # | Gap | Closed in Task 2 | Still open (production answer) |
|---|---|---|---|
| 1 | CI/CD | PR checks (lint, format, tests, Dockerfiles, kustomize + digest guard, DAG integrity + strict template render), image build/scan/push, digest bump PRs, deploy guard (clean `main` only), pre-commit | dev → staging → prod promotion; Trivy is report-only |
| 2 | Orchestration | data-interval timetable, retries, backfill, `execution_timeout`, 2 h DeadlineAlert, failure alert | — |
| 3 | Runtime | requests/limits on every pod, Allocatable sized (E25), scheduler sized (E52) | pandas on the driver (one Excel day fits); production: read the source DB / Parquet at landing |
| 4 | Availability | K8s restarts failed pods (drills 1–3) | one replica of everything = no HA |
| 5 | Backup / recovery | nightly `pg_dump` CronJob, restore drill, `register_table` drill | **backups live in the same MinIO**: the from-zero run deleted them with the cluster → off-cluster copy needed |
| 6 | Data quality | 13 day-generic rules + exact counts, fails the run | — |
| 7 | Publish safety | — | Write-Audit-Publish with an Iceberg branch (Could) |
| 8 | Observability | Telegram alerts, remote task logs in MinIO, `dag-report` | metrics/dashboards (Prometheus/Grafana), Spark History Server |
| 9 | Security | 6 MinIO service users with policies, Secrets as files, RBAC per namespace, `read:packages`-only pull token, images by digest | TLS, HMS auth, secret rotation, NetworkPolicy (kindnet does not enforce it) |
| 10 | Table maintenance | weekly DAG + `make maintenance` | — |
| 11 | Ingestion | daily landing files, reprocess = clear the day | JDBC/CDC, late data |
| 12 | Change management | branches + PRs (#1–#37), tags | branch protection (paid plan for private repos) |

## Errors met and lessons (details: `05_errors_and_fixes.md`, E25–E59)

- **Resources are what the kubelet believes** (E25 Allocatable, E52 LocalExecutor workers): measure, then size; check restart counts, a green run can hide OOM kills.
- **Airflow 3 changed defaults**: timedelta/cron schedules give zero-length intervals (E38), manual runs may have no logical date and templates render strictly (E47), one run per logical date (E49), DagBag API (E34), hostname for logs (E37).
- **Deployed = reviewed**: deploy only from clean `main` (E39), digests in a PR (E31/E32), chart NOTES leaking a password (E35).
- **Readers must follow the source of truth**: remote logs moved → the report moved (E51); duplicated driver lines explained (operator reconnect).
- **Guard rails beat memory**: Iceberg's 24 h orphan guard (E46), `ORPHAN_HOURS` check in make, CI renders templates without a date.
- **Wait for what exists**: `gh pr checks` right after `gh pr create` saw no checks and the PR merged without CI (E55) → `sleep 15` first; StatefulSet pods that never became Ready are not replaced (E54).
- **Secrets and commands**: token pasted in chat → revoked (E43); no placeholders or prose in command blocks (E45); old gh from Ubuntu (E50); pin by `requires_python`, not only date (E48).

## Known limits / next

- Task 3 starts with **Silver** (`jobs/silver_transform.py`, typed/deduped/flagged, `final_intent`), added to `chatbot_bronze` (→ `chatbot_pipeline`), then Trino on Bronze + Silver. Required pytest topic: Oracle timestamp parsing.
- Open with the leader: D6 (Silver placement), public vs private GHCR images.
- **Design notes (10-27):**
  - *Scale.* The pattern is standard (Spark Operator + Airflow, Iceberg + catalog + S3, CI → registry → deploy by digest, idempotent partitions); the deployment is lab-sized. Job code, DAG logic and CI carry over; the platform layer is replaced, not enlarged: multi-node managed K8s + autoscaling, KubernetesExecutor/Celery + HA scheduler, HA Postgres / S3 / ≥ 2 HMS (or a REST catalog), GitOps deploy, off-cluster backups, Vault/External Secrets.
  - *First bottleneck for more data:* the Excel read (`pd.read_excel` on the driver, `bronze_ingest.py:46`): single-threaded and bounded by driver memory, more executors do not help. Everything after Bronze reads Parquet in parallel.
  - *Landing stays xlsx* (what the source delivers; Bronze is where raw input becomes Parquet). Alternatives: simulator writes Parquet (converter at the edge; record the xlsx name for lineage), or `binaryFile` + `mapInPandas` to parse one file per executor task. Production: Parquet/CSV export or JDBC/CDC at the source. Note: the Excel is parsed twice today (simulator + Bronze).
  - *Event-driven option:* start a run when the landing file arrives (`S3KeySensor` / Airflow 3 asset) instead of at midnight; only if Task 3+ needs it.
- Off-cluster backup copy; WAP; History Server — only if Task 3+ needs them.

## Files

| Path | What |
|---|---|
| `dags/` | `chatbot_bronze.py`, `lake_maintenance.py`, `alert_smoke.py`, `templates/*_sparkapp.yaml` (shared by Airflow and make) |
| `plugins/lake_alerts.py` | Telegram failure + deadline callbacks |
| `jobs/` | `bronze_ingest.py`, `validate_bronze.py`, `landing_simulator.py`, `maintenance.py`, `catalog_tool.py` |
| `tests/` | `unit/` (52), `dags/test_dag_integrity.py`, `sql/bronze_rules.sql`, `expected/bronze_daily.json`, `k8s/` (s3 access check, dag report) |
| `infra/k8s/` | kustomize (namespaces, lake, rbac, secrets generator), `images.env`, `render-secrets.py`, `secrets.env.example` |
| `infra/helm/` | values for Airflow and the Spark Operator |
| `.github/` | `workflows/ci.yml`, `workflows/images.yml`, `scripts/` (Trivy counts, GHCR prune) |
| `docs/task2/` | 01 knowledge · 02 config/budgets · 03 plan · 04 process · 05 errors · 06 validation · 07 drills · 08 study notes (52 Q&A, global + per stack) · slides (Part 5 = deep dive, slides 34–69; sources in appendix A3) |
