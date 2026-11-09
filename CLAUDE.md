# CLAUDE.md — context for Claude Code (VNPT AI lakehouse lab)

**Start every session by reading `docs/HANDOFF.md` and `docs/progress.md`.**

## Who / how to work
- Bee (Pham Quang Minh), Data Engineer fresher at VNPT AI. Wants direct, concise answers with technical justification, and challenges unsupported claims, so back claims with command output, docs or versions.
- Guide **step by step**: one stage at a time, say what to expect, ask for output before continuing. Ask before deleting anything or making big design changes.
- Bee starts in **Windows cmd** (not PowerShell), then `wsl` into Ubuntu-22.04 for all project commands. Verify user/home with `whoami; pwd` (observed `quang`, `/home/quang`; Bee calls it "user").

## Lab scope and data
- Onboarding lab only. Tasks 1–5 are standalone mini-projects on the same data (1 Spark→Iceberg on MinIO, 2 K8s+Airflow, 3 Trino, 4 StarRocks/dbt Gold, 5 Superset). Current: **Task 3** (Task 1 done: tags task1-v1-lab, task1-v2-standard, task1-v3-operable; **Task 2 done: tag task2-v1, summary docs/task2/README.md**; `infra/compose/` stays unchanged and must keep running).
- Data = **trial/imaginary chatbot logs for freshers, not real people**. Still keep it on the laptop: no uploads, no row contents in alerts/Git/public logs.
- Raw files: `~/data/vnpt-lab/` (outside the repo).

## Machine limits
- Windows 11 + WSL2 (10 GiB RAM / 8 CPU visible), 16 GB physical RAM, i7-13700HX.
- **Disk is a concern**: the setup already used 30–50 GB. State disk cost of every proposal; prune after each task.
- Every container/pod needs memory limits; JVM `-Xmx` ≈ 70–75% of the limit.

## Infra preferences
- Prefer **pulled public images configured via docker-compose** (env, mounted configs/jars, `--packages`) over custom builds; build only when unavoidable and keep it thin.
- Pin versions (tags or digests). Change one version at a time.

## Stack / versions (see docs/progress.md §2)
Python 3.11 · Spark 3.5.9 (Scala 2.12, Java 17) · Iceberg 1.11.0 runtime, format v2 · MinIO via `cgr.dev/chainguard/minio` (pinned digest) · **Hive Metastore 3.1.3** (HMS 4 breaks Spark 3.5's Hive 2.3 client: E16) on Postgres 16 · hadoop-aws must equal the HMS image's hadoop-common (3.1.0 for apache/hive:3.1.3) · later: Airflow 3.3.2, Trino 483, StarRocks 3.5.21, dbt, Superset.

## Data facts
- `bot_dvc_hcm.xlsx` (chat log, key `ID`) and `bot_ddvc_hcm_bot_predict.xlsx` (NLU result, `ID_CHATLOG` → chat `ID`); sheet `Export Worksheet`.
- `CREATED_TIME` is Oracle text (`02-JUL-24 03.40.09.895455000 PM`); ~98% of client messages are `/home`; 2 internal email sender IDs; 116 predict rows without a chat row.
- `final_intent`: fallback stays fallback; confidence < NLU_THRESHOLD → `bot_khong_hieu`; else INTENT_NAME.
- Expected numbers: `docs/task1/06_validation.md`.

## Engineering rules
- Idempotent jobs (`overwritePartitions` / `MERGE`), parameterised by date/interval.
- Bronze = raw strings + load metadata; Silver = typed/deduped/flagged; Gold = dbt marts.
- No secrets in Git or images; `.env` and `infra/compose/secrets/` are git-ignored. HMS/Postgres credentials are Compose secret files rendered by `make secrets` (never in `SERVICE_OPTS`); MinIO/S3 keys are still env vars (phase 2), so still treat `docker inspect` output and logs as sensitive.
- Log every error in `docs/task1/05_errors_and_fixes.md` (symptom → root cause → fix → theory) and every step in `docs/task1/04_process.md`; update `docs/progress.md` at session end.

## Task 2 (Spark on Minikube + Airflow) — DONE (task2-v1); docs/task2/README.md, details in 01–07
- Pins (checked 2026-10-25): minikube v1.39.0, K8s v1.37.0, Kubeflow Spark Operator **2.2.1** (last on Spark 3.5; 2.3+ = Spark 4.0), Airflow 3.3.2 on chart 1.22.0 (D1 decided: chart only gates at >=3.0.0), provider cncf.kubernetes 10.22.0 (bundled in apache/airflow:3.3.2-python3.12; image vnpt-lake-airflow with DAGs baked, built by CI, D5). Airflow chart from dlcdn + SHA-512 (`make airflow-chart`, E29).
- **Bee types every command** (minikube, kubectl, helm, docker, make, git push, PRs, and all checks) in WSL. Claude writes files/docs/local commits, guides one stage at a time with Expect lines, and does NOT run cluster/Docker/GitHub commands, not even read-only checks.
- Plan v2 (docs/task2/03_plan.md): no image builds on the laptop: GitHub Actions builds, GHCR stores (`ghcr.io/beno1tp/…`, tag = git SHA, deploy by digest), Minikube pulls with Secret `ghcr-pull` (repo is private; token `read:packages` only). Branch + PR for every change from step 2.0b; CI must be green before merge.
- Task 2 also closes production gaps (03_plan.md §H): CI/PR, day-generic DQ rules, MinIO service user, backup/restore drill, maintenance DAG, alerts/logs.
- **Silver is required** (lab doc: Medallion Bronze → Silver → Gold; Spark ETL writes Iceberg, dbt builds Gold in Task 4). D6 (Bee, 10-25, confirm with the leader): Silver = Spark job `jobs/silver_transform.py` added to the Airflow DAG at the **start of Task 3, before Trino**; Task 2 stays "the step-1 job on Minikube under Airflow" (Bronze). Lab doc: `~/data/Engineering Onboarding Labs.docx`.
- Minikube `--driver=docker --kubernetes-version=v1.37.0 --memory=8192 --cpus=6 --extra-config=kubelet.system-reserved=memory=3686Mi` (E25: kubelet sees 10.7 GiB host RAM, cap is 8 GiB → Allocatable 7268 Mi); node runtime containerd (use `crictl`, not docker, inside the node); Docker Desktop + Ubuntu share the 10 GiB WSL VM: stop the Task 1 stack while Task 2 runs; requests must fit node Allocatable.
- Minikube has its own image store (inside docker_data.vhdx); images come from GHCR (`imagePullPolicy: IfNotPresent`, SHA tags/digests, never `latest`); local build + `minikube image load` only as fallback. `minikube delete` destroys PVC data.
- Namespaces: lake (postgres, minio, hive-metastore), spark-operator, spark-jobs, airflow. Cross-namespace DNS: `<svc>.<ns>.svc.cluster.local`.
- Secrets from git-ignored `infra/k8s/secrets.env` (kustomize secretGenerator) and rendered `infra/k8s/secrets/hive-site.xml`; mounted as files. MinIO root keys only for MinIO, bucket-init, minio-users; consumers use service users (2.1b): spark-jobs (`s3-credentials`), landing-writer, hive-metastore.
- Spark Operator **webhook stays on** (it applies volumes, env, ConfigMaps to driver/executor pods).
- One retry layer: Airflow retries; SparkApplication `restartPolicy: Never`; validation `retries=1` (E41).
- Airflow 3: daily intervals only via `CronDataIntervalTimetable("0 0 * * *", timezone="UTC")` (a bare cron string → CronTriggerTimetable and a `timedelta` → DeltaTriggerTimetable both give zero-length intervals: E38). Deploy only from clean `main` (`make deploy-guard`, E39). After a laptop restart: `minikube start` first (E40). Day = date as written in `CREATED_TIME`; daily runs 2024-07-02 → 07-25 (D2 = A): 18 days load, 6 days without a landing file are skipped.
- Task 2 tech = Minikube + Airflow + Spark only (no DuckDB etc.); Trino is Task 3. Expected counts via Spark SQL (D3).
- Excel input comes from the MinIO landing zone `s3://landing/<table>/dt=YYYY-MM-DD/`, never a hostPath.
- Deployed image digests: `infra/k8s/images.env` (+ kustomize `images:`), bumped by PR after an images run.
- Errors: docs/task2/05_errors_and_fixes.md (E25–E62); Task 3 errors start at **E63**. Never `make -n` a target whose recipe calls `$(MAKE)`: make runs those lines anyway (E60). Times: Bee is in Vietnam; write `HH:MM UTC (HH:MM ICT)`, never mix zones; data `CREATED_TIME` is Vietnam local; Airflow schedules in UTC (daily run 07:00 ICT).
- Bring-up: `make k8s-up` (idempotent, also from zero) + `make k8s-demo` (landing → backfill → report; fails on failed runs or scheduler restarts). Airflow CLI from make runs in the **api-server** container (`AF`), never the scheduler (E52). Scheduler: LocalExecutor `parallelism: 2`, 1Gi/1536Mi. Rerun a day = `airflow tasks clear`, not trigger (E49); in `dag-runs` day D's run_id is `…__D+1` (named after `run_after`), logical_date = D. `localhost:8080 refused` from kubectl = no context → `minikube start` (E53). Apply `infra/k8s/` **only with `-k`** (`make k8s-up`), never `-f` on single files: generated Secret names have hash suffixes (E54); after fixing a StatefulSet template, delete its never-Ready pod. `dag-report` reads `s3://airflow-logs` (E51). Demo (freeze step 1): `make minio-ui` (MinIO user `demo-viewer`, read-only, no backup), `make history-ui` (event logs on PVC `spark-events`), `make airflow-viewer` / `airflow-viewer-check`; freeze (B2): `make freeze TASK=n` / `freeze-check TASK=n` (Iceberg tag `taskn_v1`), `make evidence` + `evidence-export` (outside Git); demo profiles (B3): `make demo-task2` (view-only, no scheduler) / `demo-pipeline` / `demo-check`; never `minikube delete` before the final presentation.
- Bee's gh is 2.103.2 (official apt repo), modern flags work. PR blocks always: `gh pr create --fill --base main`, then `sleep 15 && gh pr checks --watch`; merge only after `All checks were successful` (E55: checks not yet registered → merged without CI).

## Commands
- `DC="docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env"` then `$DC up -d …`, `$DC logs -f <svc>`, `$DC ps -a`
- `make` (help) / `make build` / `make up` (also after a reboot, E23) / `make smoke` / `make bronze DATE=YYYY-MM-DD` / `make validate DATE=…` / `make acceptance DATE=…` / `make sql` / `make down`. Runbook: `docs/task1/10_operations.md`
