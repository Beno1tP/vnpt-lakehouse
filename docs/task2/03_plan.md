# Task 2 — Plan v2: architecture, steps, image strategy, production gaps, risks, repo paths

Knowledge: `01_knowledge.md`. Versions, budgets, decisions: `02_config.md`. History: `04_process.md`.

**v2 (2026-10-25 evening):** merges Claude desktop's plan update (Part G "no image builds on the laptop", Part H "production gaps Task 2 closes") into v1, keeps the review fixes of v1 and the facts measured in steps 2.0–2.2. Reconciliation of the two plans: §R at the end.

**Execution rule (Bee, reconfirmed 2026-10-25):** Bee types **every** command in WSL (install, apply, helm, make, git push, PRs, and the checks). Claude writes files, docs and local commits, gives one stage at a time with "Expect:" lines, and reads the pasted output. Claude does not run cluster, Docker or GitHub commands, not even read-only checks.

---

## B. Target architecture

```
GitHub (private repo Beno1tP/vnpt-lakehouse)
  PR  → CI checks (lint, unit tests, Dockerfile check, kustomize render)
  main→ build 4 images → Trivy scan → push ghcr.io/beno1tp/<image>:<git sha> (+ digests recorded)
                                                   │ pull (imagePullSecrets: ghcr-pull, read:packages token)
Minikube (v1.39.0, K8s v1.37.0, 8 GiB / Allocatable 7268 Mi)
  lake          : postgres (StatefulSet; DBs metastore + airflow) · minio (StatefulSet) · hive-metastore (Deployment,
                  schema-init initContainer) · Job bucket-init · Job minio-users (service user for Spark) · CronJob pg-backup
  spark-operator: Kubeflow Spark Operator 2.2.1 (controller + webhook), watches spark-jobs
  spark-jobs    : SparkApplication bronze-<date> / validate-<date> → driver pod → 1 executor pod
  airflow       : Airflow 3.3.2 (chart 1.22.0, LocalExecutor, external Postgres), image vnpt-lake-airflow (DAGs baked)
                  DAG chatbot_bronze: check_landing → bronze → validate → notify ; DAG lake_maintenance (weekly)
```

## C. Steps and status

| Step | Do | Done when | Status |
|---|---|---|---|
| **2.0 Prep** | minikube v1.39.0, K8s v1.37.0, `kubelet.system-reserved` (E25), metrics-server, namespaces, per-day expected counts (Spark SQL, D3) | node Ready, Allocatable 7268 Mi, expected table saved | ✅ 10-25 |
| **2.0b CI + PR habit** (G, H#1, H#12) | From now on: work on a branch, open a PR, merge when CI is green. `.github/workflows/ci.yml` on every PR: `ruff`, `pytest` (unit tests of pure functions: `load_checks`, `expected_for_day`, `source_path`, `event_day`), `docker build --check` for every Dockerfile, `kubectl kustomize` render with dummy secrets. `.github/workflows/images.yml` on push to `main` (+ manual): build the 4 images (G2), **Trivy** scan (fail on CRITICAL with a fix), push to GHCR tagged with the git SHA, write digests to the job summary | a PR shows green checks; after merge, 4 packages in GHCR with SHA tags; scan reports in the run | ✅ 10-25 (3 images; airflow image comes with 2.3) |
| **2.0c Pull from GHCR** (G3) | Token with `read:packages` only (typed by Bee, never committed); Secret `ghcr-pull` in `lake`, `spark-jobs`, `airflow`; manifests and templates switch from local names to `ghcr.io/beno1tp/…@sha256:…`; `imagePullPolicy: IfNotPresent`; then remove the old locally-loaded images from Minikube (`minikube image rm`) | HMS, bucket-init, SparkApplications run from GHCR images; `minikube image ls` has no `docker.io/library/vnpt-lake-*` | ✅ 10-25 |
| **2.1 Storage + catalog** | Postgres, MinIO, buckets, HMS (schema-init initContainer), Secrets as files | cross-namespace DNS + ports from spark-jobs; schema 3.1.0 | ✅ 10-25 |
| **2.1b MinIO service user** (H#9) | Job `minio-users`: user `spark-jobs` with a policy *read `landing`, read/write `lakehouse`*; Secret `s3-credentials` switches from root keys to this user; landing simulator keeps a writer user for `landing` | a SparkApplication still COMPLETES; the same keys **cannot** write to `landing` or create a bucket (negative test) | ✅ 10-25 |
| **2.2 Spark Operator + SparkApplication** | operator 2.2.1 (webhook on), landing simulator, `bronze_ingest.py --source s3://landing`, validator per-day mode, `make sparkapp` | COMPLETED; validate 10/10; rerun 16/16 (overwrite, added = deleted); big days OK; Task 1 acceptance still PASSED | ✅ 10-25 |
| **2.3 Airflow** | Chart 1.22.0 + Airflow 3.3.2 (D1); image `vnpt-lake-airflow` = `FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8…` + `COPY dags/` built by CI (D5, replaces D4); LocalExecutor; external Postgres; fernet/api/jwt Secrets by name; redis/statsd/pgbouncer/flower off; no 100Gi PVCs; migrations without Helm hooks; chart from CDN + SHA-512 (E29); `registry.secretName: ghcr-pull` | UI via port-forward; `hello_lake` runs and prints its data interval | ✅ 10-25 |
| **2.4 DAG** (H#2, H#6) | `chatbot_bronze` (`timedelta(days=1)`, `max_active_runs=1`, `execution_timeout`): `check_landing` (skip if no file, D2) → `bronze` (SparkKubernetesOperator, retries=2) → `validate` (retries=0) → `notify`. RBAC: Role in `spark-jobs` (sparkapplications, pods, pods/log) for Airflow's ServiceAccount. **Validator gets day-generic rules** (H#6) next to the exact per-day reconciliation | one triggered run end to end; logs in the UI; a forced retry does not fail with `AlreadyExists`; rules pass on all 18 days | ⏭ 2.4a ✅ (rules), 2.4b DAG in PR |
| **2.5 Backfill + drills** | backfill 2024-07-02 → 07-25 (18 load, 6 skip), twice; drills: kill the driver mid-run, delete an executor, restart the scheduler; measure pod memory peaks on a long run (TODO from 2.2e) | backfill ×2 → identical counts per day; each drill recovers without duplicates | ✅ 10-26 (backfill ×2 identical; drills 1–3 pass; E38, E39, E41 found and fixed) |
| **2.5b Backup + restore** (H#5) | CronJob `pg-backup` (01:00 UTC, `pg_dump -Fc` with the server's postgres:16 image) of `metastore` + `airflow` → `s3://backup/postgres/<ts>/`, MinIO user `backup-writer` (no delete); make `backup-now`, `drill-lose-metastore CONFIRM=yes`, `restore-metastore TS=`; drill 1: drop the HMS DB, restore, tables visible; drill 2: `CALL lake.system.register_table(…)` from the last `metadata.json` | both drills restore the tables with the same counts | ✅ 10-26 (drill 6: catalog DB lost → restored, 23/23; drill 7: lost commit re-registered from metadata.json) |
| **2.5c Maintenance DAG** (H#10) | weekly `lake_maintenance` (Sunday 02:00 UTC) running `jobs/maintenance.py`: metadata retention (10 previous metadata.json), `expire_snapshots` (7 days, keep ≥ 5), `rewrite_data_files` + `rewrite_manifests`, `remove_orphan_files` (72 h, `prefix_listing` via S3FileIO), row-count assert, time-travel check; `make maintenance [DRY=yes]`; run it **after** the time-travel proof; record snapshots/files before and after | snapshot count and file count drop; counts unchanged | ✅ 10-26 (chatlog: snapshots 50→6, files 50→22, metadata 50→11, rows 21,224 unchanged; time travel to the oldest snapshot gone = expected) |
| **2.6 Alert + logs** (H#8) | Dedicated Telegram "Alert group" (bot `bee_lake_alerts_bot`, chat ID in secrets.env); `plugins/lake_alerts.py`: task `on_failure_callback` (after the last try) + DAG **DeadlineAlert 2 h after queued** (AsyncCallback in the triggerer; Airflow 3 replacement for SLAs), metadata only; `alert_smoke` DAG proves both paths; Airflow **remote task logs to MinIO** (bucket `airflow-logs`, MinIO user `airflow-logs`); Spark event logs + History Server on demand (Could) | a forced failure → one message, no data contents; old task logs readable after a scheduler restart | ✅ 10-26 (alert_smoke: ⏰ + ❌ received; log read from `s3://airflow-logs` after restart; History Server = Could) |
| **2.7 Image hygiene** (G) | deploy by digest everywhere; Trivy already in 2.0b; delete old GHCR versions | manifests reference digests only | ⏳ |
| **2.8 Docs + tag** | README (RAM, disk, decisions, gaps closed/open), errors from E25, `git tag task2-v1` | `make k8s-up && make k8s-demo` from zero | ⏳ |
| Could | Write-Audit-Publish with an Iceberg branch (H#7) | — | — |

**Silver: Task 3, before Trino (D6, decided by Bee 10-25, to confirm with the leader).** The lab doc requires a Medallion pipeline (Bronze → Silver → Gold, Spark ETL → Iceberg, dbt → Gold) but names no step for Silver; step 2's goal is the step-1 job on Kubernetes under Airflow, so Task 2 ends with Bronze. Task 3 starts with a Spark Silver job added to the same DAG, then Trino queries Bronze + Silver; Task 4's dbt `staging` reads Silver.

## G. Image strategy: no image builds on the laptop (D5)

The Dockerfiles stay in the repo; **GitHub Actions builds them, GHCR stores them, Minikube pulls them.** Saves laptop CPU, the build cache and the second copy in Docker Desktop, and is the production flow. Task 1 keeps working with its local images (optional later: G5).

### G1. Options re-rated against "no local build"

| Option | No local build? | Verdict |
|---|---|---|
| **A. GitHub Actions → GHCR, pull into Minikube** | ✅ | **Chosen for Task 2** |
| B. `FROM apache/spark` + pip, built locally | ❌ | Task 1 (`task1-v2…`); its Dockerfiles are A's input |
| C. `tabulario/spark-iceberg` | ✅ | No: 3.5.5 / 1.8.1, demo image (Jupyter, root), REST catalog |
| D. Official images + `--packages` at start | ✅ | Dev only (Task 1 v1): runtime downloads, not reproducible |
| E. Shared base image | — | Used: `spark-job` = `spark` base + code layer |

### G2. Images built by CI

| Image | Dockerfile | Rebuilt when | Note |
|---|---|---|---|
| `ghcr.io/beno1tp/vnpt-lake-spark` | `docker/spark/` | Spark/Iceberg/pip versions change | base of spark-job |
| `ghcr.io/beno1tp/vnpt-lake-hms` | `docker/hive/` | HMS/jar versions change | |
| `ghcr.io/beno1tp/vnpt-lake-spark-job` | `docker/spark-job/` (`BASE_IMAGE` = the spark image above, by digest) | `jobs/`, `tests/expected/`, `tests/sql/` | small code layer |
| `ghcr.io/beno1tp/vnpt-lake-airflow` | `docker/airflow/` (new): `FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8…` + `COPY dags/` | `dags/` | no pip: provider cncf.kubernetes 10.22.0 already in the image |

Workflow: `push` to `main` with `paths` filters + `workflow_dispatch`; `permissions: contents: read, packages: write`; login with `GITHUB_TOKEN`; actions pinned by commit SHA; `cache-from/to: type=gha`; `spark-job` `needs:` the spark build. GHCR names are lowercase (`beno1tp`).

### G3. Pulling into Minikube

- The repo is **private** (GitHub API returns 404 without login, checked 10-25), so its packages are private: create a **fine-grained or classic token with only `read:packages`**, then per namespace that pulls (`lake`, `spark-jobs`, `airflow`): `kubectl -n <ns> create secret docker-registry ghcr-pull --docker-server=ghcr.io --docker-username=Beno1tP --docker-password=<token>` (typed, never committed).
- References: Pods/Jobs/Deployments `imagePullSecrets: [{name: ghcr-pull}]`; SparkApplication `spec.imagePullSecrets: [ghcr-pull]`; Airflow chart `registry.secretName: ghcr-pull`.
- Alternative (ask the leader first): make the 4 packages public; they contain code and dependencies, no data, no secrets.

### G4. Cost and limits

| Item | Fact | Impact |
|---|---|---|
| GHCR storage/bandwidth | GitHub docs: container registry storage and bandwidth "currently free", with ≥ 1 month notice before billing (desktop plan sources; re-check before relying on it) | OK for the lab |
| Actions minutes | private repo on GitHub Free: monthly quota (Settings → Billing → check the number) | base images are rebuilt rarely; code images are minutes |
| Laptop disk | Minikube must still store each image it runs. Switching from the loaded local images to GHCR images stores **new** layers (CI builds are not byte-identical): ≈ +5.6 GB until the old ones are removed in 2.0c; Airflow image ≈ +2–2.5 GB in 2.3 | measure before/after each pull |
| Iteration speed | commit → CI (minutes) → pull is slower than a local build | develop job logic on the Task 1 Compose stack (fast loop), then PR |

### G5. Optional: Task 1 without local builds
Point Task 1 `.env` at the GHCR images, `docker login ghcr.io` with the read-only token, remove the local `vnpt-lake-*` images; `make build` stays for offline work.

### G6. Fallback if CI is blocked
Build locally, load with `minikube image load`, delete `~/.minikube/cache/images` (what steps 2.0e/2.2c did; measured costs in `04_process.md`).

## H. Production gaps: what Task 2 closes

Source: the 12-gap review of Task 1 (10-22). Rule: take only what is cheap in RAM and time and teaches a production habit; document the rest with the production answer.

| # | Gap | Task 2 action | Priority | Step |
|---|---|---|---|---|
| 1 | CI/CD | PR checks + main build/scan/push (G). Dev → staging → prod promotion: not in lab | Must | 2.0b, 2.7 |
| 2 | Orchestration | the DAG: daily intervals, retries, backfill, `load → validate` dependencies. Airflow 3 removed the old SLA feature → `execution_timeout` + failure alert (check deadline alerts in 3.3 docs) | Must | 2.4, 2.5 |
| 3 | Runtime / scale | driver + executor pods with requests/limits (done in 2.2). pandas-on-driver stays a **known limit** (one day of Excel is fine); production answer: read the source DB or convert to Parquet at landing | Must / document | 2.2 ✅ |
| 4 | Availability | not in lab. Talk note: K8s **restarts** failed pods (recovery), one replica is still not high availability | Document | 2.8 |
| 5 | Backup / recovery | CronJob `pg_dump` + restore drill + `register_table` drill. Caveat: backup on the same MinIO / same disk ≠ disaster recovery | Should | 2.5b |
| 6 | Data quality | rules valid for any day: rows written = rows read (in job ✅), rows > 0, `ID` not null + unique in the partition, `SOURCE` ∈ {client, bot}, expected column set (schema drift), both tables same `_batch_id` (✅). Exact per-day counts stay as the lab reconciliation | Must | 2.4 |
| 7 | Publish safety | WAP with an Iceberg branch (verify branch-write syntax for Iceberg 1.11 + Spark 3.5 first) | Could | after 2.5 |
| 8 | Observability | Telegram on failure (metadata only); remote task logs to MinIO; Spark event logs + History Server on demand. Prometheus/Grafana: not in lab (RAM) | Should | 2.6 |
| 9 | Security | MinIO service user + policy for Spark; Secrets as files; least-privilege RBAC per namespace (operator's Role ✅, Airflow Role in 2.4); GHCR token `read:packages` only. Not in lab: TLS, HMS auth, rotation, NetworkPolicy (Minikube's kindnet does not enforce it) | Should | 2.1b, 2.4 |
| 10 | Table maintenance | weekly maintenance DAG, after the time-travel proof | Should | 2.5c |
| 11 | Ingestion source | daily landing files ≈ one export per day; reprocess a day = rerun its interval. Watermarks, late records, JDBC/CDC: later | Document | A5 |
| 12 | Change management | branches + PRs from 2.0b, CI must pass; tag per release (`task2-v1`); rollback = redeploy the previous digest. Branch protection on a private repo needs a paid plan → keep the PR habit without enforcement | Should | 2.0b |

RAM: the additions run briefly (Jobs, CronJob, maintenance = one Spark run) or on demand (History Server ~0.5 GiB) → the budget in `02_config.md` §2 holds.

## D. Risks and first checks

| Symptom | Likely cause | First command |
|---|---|---|
| Pod `Pending` | not enough memory for its request | `kubectl describe pod …` → Events |
| `OOMKilled` / exit 137 | limit too low (driver: pandas in overhead) | `kubectl describe pod` → Last State |
| `ImagePullBackOff` / `ErrImagePull` | GHCR token missing/wrong, `ghcr-pull` not in that namespace, tag/digest typo | `kubectl describe pod` → Events; `kubectl -n <ns> get secret ghcr-pull` |
| `ErrImageNeverPull` | `imagePullPolicy: Never` but the image is not in Minikube | `minikube image ls` |
| `CrashLoopBackOff` | app error at start (config, DNS, secret) | `kubectl logs --previous` |
| `CreateContainerConfigError` | referenced Secret/ConfigMap missing | `kubectl describe pod` → Events |
| Spark: `pods is forbidden` | driver ServiceAccount lacks RBAC | `kubectl auth can-i create pods -n spark-jobs --as=system:serviceaccount:spark-jobs:spark-operator-spark` |
| Airflow: `sparkapplications … forbidden` | Airflow SA has no Role in `spark-jobs` | same `auth can-i` for the Airflow SA |
| S3 `AccessDenied` after 2.1b | policy too narrow for the service user | MinIO console → user → policy |
| Spark pods without config/keys | webhook down (`failurePolicy: Fail` rejects instead) | `kubectl -n spark-operator get pods` |
| DAG not visible | import error | `airflow dags list-import-errors` in the dag-processor pod |
| runs process the wrong day | zero-length intervals (A3 gotcha) | print `data_interval_start/end` in the first task |
| CI red on `kustomize` | renderer refuses `change-me` values | CI writes dummy values before rendering |

## E. Questions for the leader

1. Is the Kubeflow Spark Operator what production uses, or native spark-submit / another platform?
2. Production Airflow version and executor?
3. OK to simulate daily landing files split by event day?
4. ~~Silver inside Task 2?~~ 10-23 "Bronze only for now"; **10-25: Silver is required** (lab doc: Medallion Bronze → Silver → Gold, Spark ETL to Iceberg). Which step owns it is not written: proposal D6 = Spark job in the Task 2 DAG; alternatives: dbt staging in Task 4, or part of the step-1 Spark job.
5. **New:** may the 4 lab images be public on GHCR (code + dependencies only), or must they stay private (pull token)?

## F. Repo paths (new = ➕, changed = ✏️, done = ✅)

```
.github/workflows/ci.yml                    ➕ 2.0b PR checks
.github/workflows/images.yml                ➕ 2.0b build → Trivy → GHCR on main
pyproject.toml                              ➕ 2.0b ruff + pytest config
tests/unit/                                 ➕ 2.0b test_validate_bronze.py, test_bronze_ingest.py, test_landing_simulator.py
tests/expected/bronze_daily.json            ✅
tests/sql/bronze_daily_counts.sql           ✅
docker/spark/ docker/hive/                  Task 1, unchanged (CI input)
docker/spark-job/Dockerfile                 ✅ (BASE_IMAGE becomes the GHCR spark image in CI)
docker/airflow/Dockerfile                   ➕ 2.3 (D5)
jobs/bronze_ingest.py                       ✅ --source
jobs/validate_bronze.py                     ✅ per-day mode; ✏️ 2.4 day-generic rules
jobs/landing_simulator.py                   ✅
dags/hello_lake.py                          ✅ (2.3 smoke)
dags/chatbot_bronze.py, dags/templates/     ➕ 2.4
dags/lake_maintenance.py                    ➕ 2.5c
infra/k8s/kustomization.yaml, 00-namespaces ✅
infra/k8s/lake/{postgres,minio,bucket-init-job,hive-metastore}.yaml   ✅
infra/k8s/lake/minio-users-job.yaml         ➕ 2.1b
infra/k8s/lake/pg-backup-cronjob.yaml       ➕ 2.5b
infra/k8s/spark-jobs/{bronze,validate}-sparkapp.yaml, landing-simulator-pod.yaml   ✅ (✏️ 2.0c image refs)
infra/k8s/spark-jobs/rbac-airflow.yaml      ➕ 2.4
infra/helm/spark-operator/values-local.yaml ✅
infra/helm/airflow/values-local.yaml        ✅ prepared (✏️ 2.3: CI image, registry secret; ConfigMap DAG mount removed)
docs/task2/01…07                            ✅ living docs
```

## R. Reconciliation: desktop plan v2 vs. what is verified here

| Desktop plan v2 says | Status here | Evidence |
|---|---|---|
| Operator "webhook off unless needed" | **Kept on**: the webhook injects volumes/env/secrets; our runs depend on it | operator `sparkpod_defaulter.go`; 2.2c driver pod had `envFrom s3-credentials` |
| Minikube 8192 MiB, total ≈ 7.5–8 GiB | 8192 MiB with `system-reserved` → **Allocatable 7268 Mi**; requests budget ≈ 6.6 GiB with Airflow + one Spark run | E25; `04_process.md` 2.3 prep |
| Disk: measure with `minikube ssh -- docker system df` | node runtime is **containerd** → `minikube image ls`, `crictl`, `docker system df` on the host | 2.0d |
| Expected counts with DuckDB | **Spark SQL** on Task 1 Bronze (D3; Task 2 tech = Minikube + Airflow + Spark) + pandas split agrees | 2.0b, 2.2b |
| Separate Job hms-schema-init | **initContainer** (automatic ordering, same check-then-create script) | 2.1c |
| core-site.xml reused from infra/compose | kustomize cannot load files outside `infra/k8s` → copy with FQDN | 2.1c |
| `make landing` = port-forward + local script | one-off pod + `kubectl cp` (no Python deps on the laptop) | 2.2b |
| Errors "E24 onward" | E24 already used → Task 2 from **E25** (now E25–E29) | `05_errors_and_fixes.md` |
| `make k8s-images` = `minikube image pull` at git SHA | ✅ adopted (2.0c); replaces `make k8s-job-image` (local build) | — |
| Airflow image baked with pinned provider | ✅ adopted as **D5** (built by CI, replaces D4 ConfigMap); provider = bundled **10.22.0** (constraints of 3.3.2), no pip | `constraints-3.3.2` |
| Chart via `helm repo add apache-airflow` | download from the Apache CDN + SHA-512 (E29) | 2.3 prep |
| Silver question open | answered 10-23 | progress.md |
