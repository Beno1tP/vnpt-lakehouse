# Task 2 — Process log

One entry per work block. Newest first. Keep it factual: what you did, what you saw, what you learned.

## Template
```
### YYYY-MM-DD HH:MM — <step id> <short title>
Did:
Observed (numbers, logs, screenshots path):
Learned (theory link):
Next:
Time spent:
```

---

### 2026-10-25 — Lab doc re-read: Silver required (D6)
Did (Claude): read `~/data/Engineering Onboarding Labs.docx` (copied by Bee). Findings: architecture = **Lakehouse Medallion (Bronze → Silver → Gold)**: Oracle → Excel → **S3 landing zone** → **Spark ETL → Iceberg on MinIO** → **dbt builds Gold** for StarRocks/Trino/Superset; **Airflow orchestrates everything**. Step 1 goal = raw Iceberg tables (Bronze). Silver has no step of its own. D6 first proposed in Task 2 (2.5d); **Bee decided Task 3, before Trino**, because step 2's goal is only "the step-1 Spark job on K8s, orchestrated by Airflow"; to confirm with the leader. Other requirements to track: pytest topics (constants, table_exists, **watermark**, **parse Oracle timestamp**, pandas → spark types, S3 reads), `make lint` / `make test` / `make all`, CI jobs lint/test/**dbt-lint** (dbt → Task 4), **pre-commit hooks**, dbt tests incl. `dbt_test_gold` task in the DAG (Task 4), Prometheus + Grafana optional, Minikube `--memory=7168 --cpus=4` (we run 8192/6, Allocatable 7268 Mi).

### 2026-10-25 — 2.4b first DAG runs OK; E38 schedule fix prepared
Observed (Bee): 3 runs `success`. 2024-07-02: check_landing → bronze (24 s, `12 rows read, 12 rows in partition`, `6 / 6`) → validate (`23/23 checks passed`) → notify, driver log lines streamed into Airflow (`[spark-kubernetes-driver] …`). 2024-07-07: `chatlog=no, bot_predict=no` → check_landing **skipped** → all downstream skipped (D2). Scheduled run: `ds=2026-10-25 data_interval=[16:29:09, 16:29:09)` → **E38** (timedelta = DeltaTriggerTimetable in Airflow 3). Fix on this branch: CronDataIntervalTimetable at midnight + CI rule.

### 2026-10-25 — 2.4b deployed; first chatbot_bronze runs (in progress)
Did (Bee): `make k8s-secrets` (MINIO_AIRFLOW_LANDING_SECRET + airflow-connections.env), deleted + re-applied the minio-users Job, `kubectl apply -k`, `make airflow-install` (revision 4), unpause + trigger 2024-07-02 and 2024-07-07; PR #10 merged (main `1e652df`).
Observed: Role + RoleBinding created; Job: `user airflow-landing: policy lake-landing-reader`; `auth can-i create sparkapplications` → **yes**, `can-i list secrets` → **no**; `chatbot_bronze` and `hello_lake` listed; Variable `spark_job_image` = spark-job `f56c2321…`. Scheduled run created on unpause: `scheduled__2026-10-25T16:29:09…`, **logical_date = run_after = 2026-10-25T16:29:09** (not midnight, not the interval start) → success (skipped: no export). Manual runs: logical_date 2024-07-02 / 07-07, data interval = [trigger time, trigger time).
Finding for 2.5: with `timedelta(days=1)` + catchup False, scheduled intervals start at the unpause time (16:29), and Airflow 3 sets the scheduled run's logical date to its run_after → `ds` of a scheduled run is the day the interval ENDS. Decide before the backfill: align to midnight (`CronDataIntervalTimetable("0 0 * * *", "UTC")`) and take the day from data_interval_start for scheduled/backfill runs.

### 2026-10-25 — 2.4b PR #9 merged; Airflow digest promoted (PR #10)
Did (Bee): PR #9 4/4 green (DAG integrity imported chatbot_bronze inside the 3.3.2 image), merged (main `d107f0e`); images run 40207562055: airflow `d107f0e` → `sha256:20b2929d…7c07` (spark-job rebuilt with unchanged code → not promoted).

### 2026-10-25 — 2.4a done (23/23 ×3 days); 2.4b DAG prepared on branch `task2/dag`
Did (Bee): PR #8 merged (main `821f981`); validate 2024-07-02 / 07-24 / 07-25 with the new image: **23/23 each** (10 reconciliation + 13 rules), incl. `event_day_matches_partition` = 0 and `unmatched_to_chat` = 116 on 07-25. **Step 2.4a done.**
Checked (provider cncf.kubernetes 10.22.0 source): SparkKubernetesOperator `random_name_suffix=True` by default → each try gets a new name (no `AlreadyExists`, review item A4 closed); `application_file` is Jinja-rendered (`template_ext` yaml); `delete_on_termination=True` deletes the app after streaming the driver log; `reattach_on_restart` finds the driver by labels (needs pods get/list/patch).
Prepared (Claude): `dags/chatbot_bronze.py` (timedelta(days=1), start 2024-07-02 UTC, catchup False, max_active_runs 1, dagrun_timeout 1h, retries 2 / 5 min, execution_timeout 20 min; `check_landing` (S3Hook, skip day without files, fail if only one table) → `bronze` → `validate` (retries 0) → `notify`); SparkApplication templates moved to `dags/templates/*_sparkapp.yaml` as **one** Jinja template for both Airflow and `make sparkapp` (sed fills `{{ ds }}` and `{{ var.value.spark_job_image }}`); MinIO user `airflow-landing` (read landing only); Secret `airflow-connections` (`AIRFLOW_CONN_MINIO_LANDING`, `AIRFLOW_CONN_KUBERNETES_IN_CLUSTER`) via chart `extraEnvFrom`; `AIRFLOW_VAR_SPARK_JOB_IMAGE` from images.env via `make airflow-install`; Role `airflow-spark-submitter` in spark-jobs bound to SA `airflow-scheduler`; DAG integrity rules for `lake` DAGs (execution_timeout everywhere, validate never retries). Tests 27 passed; kustomize + helm template render OK.

### 2026-10-25 — 2.4a PR #7 merged; spark-job digest promoted (PR #8)
Did (Bee): PR #7 4/4 green, merged (main `efa10a8`); images run 40203340278: spark-job `efa10a8` → `sha256:f56c2321…88cd` (airflow also rebuilt, DAGs unchanged → its digest is not promoted).

### 2026-10-25 — 2.3 done (E37 fixed); 2.4a prepared: day-generic DQ rules
Did (Bee): PR #6 merged (main `52d11b3`), `make airflow-install` → revision 3 (hostname_callable), manual trigger `--logical-date 2024-07-03`.
Observed: UI log view works (`http://10.244.0.63:8793/log/…` = pod IP). `[hello] ds=2024-07-03 interval=[2026-10-25 14:23:28.535927+00:00, 2026-10-25 14:23:28.535927+00:00)` → **manual runs: ds = logical date, data interval = [trigger time, trigger time)** → the DAG must take the day from `ds` (01_knowledge A3). **Step 2.3 done.**
Prepared (Claude, branch `task2/dq-rules`): `tests/sql/bronze_rules.sql` — 13 rules that return violation counts (expected 0) for any day: partition not empty, ID not null + unique, SOURCE ∈ {client, bot}, CREATED_TIME parseable, event day = partition (new with the daily split), ID_CHATLOG not null, one batch for both tables; validator `--rules`; validate SparkApplication passes it; unit test.

### 2026-10-25 — 2.3c password rotated, admin outside Helm; E37 task logs
Did (Bee): new admin password (`read -rs`), `make k8s-secrets`, `make airflow-install` (from this branch) → revision 2 (no credentials in NOTES, no deprecation warnings), `make airflow-admin` → `User "admin" created`, `helm get values | grep -c password` → 0. UI login OK; `hello_lake` ran (a scheduled run `scheduled__2026-10-25T14:13:01…`: catchup=False + unpaused).
Observed: UI log view failed (E37: scheduler pod hostname not resolvable). Fix: `hostname_callable` → pod IP (same PR #6).

### 2026-10-25 — 2.3b Airflow installed (E35, E36)
Did (Bee): PR #5 checks 4/4 green; `make airflow-install` (chart from the CDN, SHA-512 OK; volume before 8.79 GB).
Observed: `STATUS: deployed`, Airflow 3.3.2, revision 1. **E35:** the chart NOTES printed the admin password (it came from `--set-file createUserJob.defaultUser.password`) → password must be rotated. E36: 3 deprecation warnings.
Prepared (Claude, branch `task2/airflow-admin`): `createUserJob.enabled: false`, `make airflow-admin` (user created inside the scheduler pod from the secret file), deprecated key removed, `enableServiceLinks: false`, `recommendedProbeSetting: true`. `helm template` renders 4 Deployments + 1 Job (migrations), no create-user Job.

### 2026-10-25 — 2.3a PR #4 merged (Airflow image), digests promoted (PR #5)
Did (Bee): PR #4: first run red (E34, Airflow 2 `DagBag(include_examples=…)`), fixed, 4/4 green incl. the new DAG integrity check inside the 3.3.2 image; merged (main `a226c4f`); images run 40199225449 success.
Digests: spark `aee0deb9…` and hms `9446f7d1…` (`built=false`); spark-job `a226c4f` → `sha256:4e873c12…388e1f`; **airflow `a226c4f` → `sha256:6fbd3133…25d5a4`**.
Prepared (Claude, branch `task2/airflow-deploy`): `images.env` gets `AIRFLOW_IMAGE_REF` and the new spark-job digest — promotion of built images to "deployed" happens only through a reviewed PR.

### 2026-10-25 — 2.1b applied (PR #3); 2.3 Airflow image prepared on branch `task2/airflow-image`
Did (Bee): PR #3 3/3 green, merged (main `2a616ed`); `make k8s-secrets` (3 MinIO user passwords generated once), `kubectl apply -k`, minio-users Job, HMS rollout, `make s3-access-test`, bronze + validate 2024-07-02, `make landing`.
Observed: Job: 3 policies created, users `spark-jobs`/`landing-writer`/`hive-metastore` attached; **s3-access-test 4/4 + 4/4** (spark-jobs: write landing **denied**, create bucket **denied**; landing-writer: lakehouse **denied**); bronze 12/12, 6/6 and validate **10/10** with the spark-jobs user; landing 36 OK with landing-writer; HMS restarted on its own user. Airflow Secrets showed `configured` (re-render; Airflow not installed yet). **Step 2.1b done.**
Prepared (Claude): `docker/airflow/Dockerfile` (FROM apache/airflow:3.3.2-python3.12 by digest + `COPY dags/`, context dags/ only, no pip); CI job `dags` builds it on the runner (not pushed) and runs `tests/dags/test_dag_integrity.py` inside it (imports, catchup False, max_active_runs 1, tags, tz-aware start_date); images.yml job `airflow` (push `vnpt-lake-airflow:<sha>`, Trivy, DIGEST line); values: `images.airflow.repository` = GHCR, digest passed by `make airflow-install` from `AIRFLOW_IMAGE_REF` in images.env, `imagePullSecrets: [ghcr-pull]` (chart's `registry:` is deprecated), default image kept on the official base for chart helper containers, DAG ConfigMap mount and `make airflow-dags` removed (D4 → D5). `helm template`: 13 containers on `ghcr.io/beno1tp/vnpt-lake-airflow@sha256:…`, 6 pods with ghcr-pull, no dags volume.

### 2026-10-25 — 2.0c done; 2.1b prepared on branch `task2/minio-user`
Did (Bee): deleted the 6 old SparkApplications; `make sparkapp DATE=2024-07-02` + validate from `ghcr.io/beno1tp/vnpt-lake-spark-job@sha256:2e5f830e…` (imageID confirmed): 12/12, 6/6, **10/10**; `make landing` from the image (baked script): 36 OK, 21,224 / 10,729; `minikube image rm` of the 4 local images. Node volume 10.29 → **8.74 GB** (−1.55 GB: CI images share the Spark base; old copies gone). `minikube image ls | grep vnpt-lake` printed nothing: images pulled **by digest only** have no tag, and `minikube image ls` lists tags → check with `minikube ssh -- sudo crictl images --digests`. **Step 2.0c done.**
Prepared (Claude): 2.1b (gap #9) — three MinIO service users with their own policies: `spark-jobs` (read landing, rw lakehouse), `landing-writer` (rw landing), `hive-metastore` (rw lakehouse); root keys left only for MinIO itself, bucket-init and the new `minio-users` Job. Job: Chainguard `minio-client` (no shell; digest `sha256:1a1b84b3…`, image created 2026-10-24 — only `latest` is public) copies `mc` into an emptyDir, a bash script in the Spark base runs it: policies (re)created, users (re)set, attach only if missing → idempotent. Renderer generates the 3 passwords once; `s3-credentials` now = spark-jobs user; new `landing-writer-credentials`, `hms-s3-credentials`. `make s3-access-test`: pods with each user's keys prove allowed **and** denied operations. Unit tests 23 (policy least-privilege checks, renderer writes service-user keys, not root); kustomize render on a clean tree OK.

### 2026-10-25 — 2.0c-2 PR #2 merged; HMS + bucket-init from GHCR (E32, E33)
Did (Bee): PR #2 (CI red once: E32 images.env ignored by `*.env`; fixed, 3/3 green), merged (main `ffe69c9`); images run 40191763230: both bases `built=false` (18 s each), spark-job `ffe69c9` → `sha256:583f95a0…` (not deployed; code unchanged). Baseline: Minikube volume 10.29 GB, 4 local `docker.io/library/vnpt-lake-*` images in the node. First apply failed (E33: Airflow Secrets in kustomization before 2.3) → added `AIRFLOW_ADMIN_PASSWORD`, `make k8s-secrets` (fernet/api/jwt keys generated once), applied again.
Observed: 4 Airflow Secrets created early (for 2.3); HMS rolled out from `ghcr.io/beno1tp/vnpt-lake-hms@sha256:9446f7d1…` (pulled with `ghcr-pull`), `HMS schema 3.1.0 exists, nothing to do`; bucket-init from the GHCR spark base: both buckets `exists`.

### 2026-10-25 — 2.0c-1 pull token + Secrets; 2.0c-2 prepared on branch `task2/ghcr`
Did (Bee): classic token `minikube-ghcr-pull` with only `read:packages`; `read -rs` + Secret `ghcr-pull` (dockerconfigjson) in lake, spark-jobs, airflow (E30: my check command). Digests from the run summary (E31: not in the log):
vnpt-lake-spark `3.5.9-iceberg1.11.0-r1` `sha256:aee0deb9…cad5c`; vnpt-lake-hms `3.1.3-hadoopaws3.1.0-r1` `sha256:9446f7d1…b91ff`; vnpt-lake-spark-job `041717a` `sha256:2e5f830e…ca243` (base = spark @aee0deb9…).
Prepared (Claude): `infra/k8s/images.env` (3 refs by digest, read by make) + kustomize `images:` (same digests for HMS and bucket-init; CI test keeps them equal); HMS, bucket-init, landing pod, SparkApplication templates → GHCR + `imagePullSecrets: ghcr-pull` + `IfNotPresent`; landing pod uses the spark-job image (script and expected file baked, only the Excel files are copied in); `make k8s-job-image` kept as fallback (G6) with `LOCAL_JOB_IMAGE`; `make help` shows the k8s-* targets (regex now allows digits); workflow prints digests to the log and Trivy counts (JSON → `.github/scripts/trivy_counts.py`). Tests: 19 passed, ruff clean.

### 2026-10-25 — 2.0b CI green, PR #1 merged, first image build
Did (Bee): pushed `task2/ci`, `gh pr create` → PR #1; `gh pr checks` → 3/3 green (lint + unit tests, kustomize render, Dockerfile checks); `gh pr merge --merge --delete-branch` (main `fc93829..041717a`); `gh run watch 40189208252`.
Observed: images run **success**: base vnpt-lake-hms 3m1s, base vnpt-lake-spark 3m43s (in parallel), spark-job 2m8s → ≈ 9 runner-minutes, ~6 min wall clock. Classic token with `read:packages` created (GHCR does not accept fine-grained tokens).
Next: 2.0c `ghcr-pull` Secrets, digests from the run summary, switch manifests to GHCR.

### 2026-10-25 — 2.0b prepared on branch `task2/ci` (Claude)
Did (Bee): pushed main (`2788fa9..fc93829`, 19 commits — the last direct push to main); `gh auth status` logged in as Beno1tP.
Prepared (Claude, branch `task2/ci`): `pyproject.toml` (ruff 0.16.10 with an explicit rule set E/F/I/B/SIM/DTZ, line 120; pytest paths); `tests/unit/` (17 tests: check parsing, expected files consistent with Task 1 totals and the 24-day range, `expected_for_day`, `source_path`, `event_day`, renderer refuses change-me / generates keys once / URL-encodes / mode 600 using a temp dir, SparkApplication templates valid after placeholder fill, workflow YAML parses); small lint fixes (tz-aware DAG start_date, import split, `with open`, long lines). `.github/workflows/ci.yml` (PR + main: lint + unit tests on Python 3.10, kustomize render with dummy secrets, `docker build --check` ×3). `.github/workflows/images.yml` (main + manual: base images spark/hms built only when their version tag is missing in GHCR; spark-job tagged with the git SHA, FROM the spark base by digest; Trivy CRITICAL/HIGH report-only; digests in the run summary). Actions pinned by commit SHA (checkout v7.0.1, setup-python v7.1.0, setup-buildx v4.4.1, login v4.7.0, build-push v7.4.0, trivy v0.36.0). Local run in a scratch venv: ruff clean, 17 passed.
Decision: Trivy **reports, does not block** for now (apache/hive 3.1.3 and Spark bases carry upstream CVEs we cannot fix); gate later on our own layers.

### 2026-10-25 — Plan v2 (operable/standard approach)
Did (Claude, docs only): merged Claude desktop's plan update into `03_plan.md` v2: Part G (D5: images built by GitHub Actions, stored in GHCR, pulled by Minikube; no laptop builds), Part H (12 production gaps → Task 2 actions: CI/PR, day-generic DQ rules, MinIO service user, backup/restore + register_table drills, maintenance DAG, alerts/remote logs), step table with status, reconciliation §R (desktop plan points already corrected by measurements: webhook on, Allocatable 7268 Mi, containerd, Spark SQL expected counts, initContainer, E25+). D4 (DAG ConfigMap) superseded by D5. Repo is private (GitHub API 404 without login) → GHCR pull token needed.
Rule reconfirmed by Bee: Bee runs every command incl. checks; Claude only writes files/docs and guides. Step 2.3 (Airflow) was **not** executed; its prepared files are revised in 2.3.
Next: 2.0b CI + PR habit.

### 2026-10-25 — 2.2e Big days + 2.3 prepared
Did (Bee): `make sparkapp` + validate for 2024-07-24 and 2024-07-25.
Observed: 07-24 12,970 / 6,485 rows in ~21 s, validate 10/10; 07-25 7,552 / 3,892, validate 10/10 incl. `unmatched_to_chat` 116. No OOM with driver 1g + 1g overhead. `kubectl top` in `watch` caught only the node after the runs (2400 Mi): metrics-server samples every ~15–60 s, too slow for 20-s pods → peak pod memory not measured (TODO: measure in 2.5 drills on a longer run).
Prepared (Claude): Airflow per D4 (DAGs as ConfigMap, official image by digest, no build): `infra/helm/airflow/values-local.yaml` (LocalExecutor, external Postgres secret, fernet/api/jwt secrets by name, redis/statsd/pgbouncer/flower off, worker + triggerer persistence off so the scheduler stays a Deployment without a 100Gi PVC, migration/create-user Jobs without Helm hooks for `--wait`, parallelism 4, resources on all containers); renderer generates fernet/api/jwt keys once (appended to secrets.env) and writes the Airflow secret files; 4 fixed-name Secrets in kustomize; `dags/hello_lake.py`; make `airflow-chart` (CDN + SHA-512, E29), `airflow-dags`, `airflow-install`, `airflow-ui`. `helm template` with these values: 4 Deployments (api-server, scheduler, dag-processor, triggerer) + 2 Jobs, 13 containers all on `apache/airflow@sha256:9df9c8…`, dags volume mounted; Airflow requests 1,632 Mi → node ≈ 3.5 Gi, + one Spark run 3.1 Gi = 6.6 of 7.27 Gi.

### 2026-10-25 — 2.2d Validate on K8s + rerun idempotency (step 2.2 done for one day)
Did (Bee): `make k8s-job-image` (tag 74c5a42), `make sparkapp APP=validate DATE=2024-07-02`, `make sparkapp DATE=2024-07-02` (reload), `make sparkapp APP=validate DATE=2024-07-02 IDEMPOTENCY=yes`; Task 1 `make validate DATE=2026-10-16` (regression for the validator change).
Observed: validate **10/10** (8 checks + 2 column counts, 7 SKIP); reload 12/12, 6/6 (no duplicates); with idempotency **16/16** (last snapshot `overwrite`, added = deleted = 12 / 6); each app ~21 s. Task 1 validate **17/17** (full mode unchanged).

### 2026-10-25 — 2.2c-B First SparkApplication COMPLETED; 2.2d prepared
Did (Bee): `make k8s-job-image` (9.1 s build, one `COPY *.py` layer; `~/.minikube` back to 856 MB); `make sparkapp DATE=2024-07-02`.
Observed: PENDING → RUNNING → COMPLETED in **21 s** (04:18:19 → 04:18:40), 1 attempt; executor `…-exec-1` COMPLETED and removed; driver `Completed` (pod limits/requests 2Gi = 1g + 1g overhead); webhook injected `S3_ENDPOINT`, `AWS_REGION` and envFrom `s3-credentials`; `[bronze] bronze.chatlog: 12 rows read, 12 rows in partition 2024-07-02`, `bot_predict: 6 / 6` = `bronze_daily.json`. Only WARNs: native-hadoop library, k8s client closed at shutdown (both benign). Operator 2.2.1 (Spark 3.5.5 submit) → our 3.5.9 driver: no skew problem.
Prepared (Claude): validator per-day mode (`--expected bronze_daily.json` → expected values of that day; checks without a per-day value print SKIP; full mode unchanged), `columns` added to `bronze_daily.json`; job image now also bakes `tests/` (named build context, tag = last commit of jobs/ tests/ docker/spark-job/); `validate-sparkapp.yaml`; `make sparkapp APP=bronze|validate DATE= [IDEMPOTENCY=yes]`.

### 2026-10-25 — 2.2c-A Task 1 regression after the bronze_ingest.py change
Did (Bee): `make up && make acceptance DATE=2026-10-16`; `make down` (Minikube kept running alongside).
Observed: `[smoke] OK`; bronze 21,224 / 10,729; validate **17/17**, then **23/23** with idempotency; `[acceptance] PASSED`, exit=0. Task 1 behaviour unchanged by `--source` (default path).

### 2026-10-25 — 2.2b landing applied; 2.2c prepared
Did (Bee): `make k8s-secrets` (now also s3.env), `kubectl apply -k` (secret/s3-credentials created), `make landing`; listing pod.
Observed: 36 `[landing] … OK` lines, 21,224 / 10,729 written = read, 18 days each, pod deleted by the trap; listing: **36 files, 4,057,683 bytes** in `s3://landing`.
Prepared (Claude): `bronze_ingest.py --source` (default = Task 1 folder; `s3://landing` → `<table>/dt=<date>/<file>` via pyarrow, `_source_file` = full S3 path; Task 1 code path unchanged); `docker/spark-job/Dockerfile` (FROM thin Spark image + `jobs/*.py`, build context `jobs/` only so secrets never reach the builder); `make k8s-job-image` (tag = last commit of jobs/, refuses uncommitted jobs/, docker build → minikube image load → delete tar cache); `infra/k8s/spark-jobs/bronze-sparkapp.yaml` (driver 1g + 1g overhead, 1 executor 768m + 384m, `restartPolicy: Never`, TTL 1 day, sparkConf = Task 1 spark-defaults with FQDNs, envFrom s3-credentials); `make sparkapp DATE=` (delete old app → apply → poll state → driver `[bronze]` lines → exit non-zero unless COMPLETED). Server dry-run of the SparkApplication accepted.

### 2026-10-25 — 2.2b prepared: landing simulator (Claude)
Did: `jobs/landing_simulator.py` (pandas split by the date written in `CREATED_TIME`, one xlsx per day and table, sheet name kept, replaces the table's landing prefix → idempotent, optional `--expected` check); `infra/k8s/spark-jobs/landing-simulator-pod.yaml` (one-off pod, Spark image, 30 min deadline); `make landing` (apply pod → `kubectl cp` files → exec → delete pod via trap); secret `s3-credentials` in spark-jobs with a **stable name** (`disableNameSuffixHash`: SparkApplications/Airflow templates reference it from outside kustomize); renderer writes `secrets/s3.env`.
Observed (local test in the Spark image, `--network none`, output to scratch): 18 days per table, **every day = `bronze_daily.json`** (pandas split agrees with the Spark SQL counts: two engines), 21,224 / 10,729 written = read; round trip (read all daily files back, sort by ID) → columns and all cells **identical** to the source; 36 files, 4.1 MB. Test output removed.

### 2026-10-25 — 2.2a Spark Operator 2.2.1 installed
Did (Bee): `helm repo add spark-operator https://kubeflow.github.io/spark-operator`; `helm upgrade --install … --version 2.2.1 -f infra/helm/spark-operator/values-local.yaml --wait`.
Observed (Claude, read-only): release `deployed`, chart spark-operator-2.2.1; controller + webhook Running (image `ghcr.io/kubeflow/spark-operator/controller:2.2.1`, 10 Mi / 13 Mi); CRDs sparkapplications + scheduledsparkapplications (v1beta2); SA/Role/RoleBinding `spark-operator-spark` in spark-jobs; `auth can-i create pods` → yes; mutating webhook `spark-operator-webhook` (3 webhooks); no errors in controller log. Node requests 1892 Mi of 7268 (26 %), limits 3036 Mi. Disk: node volume 6.833 → 8.596 GB (+1.76 GB since 2.0e: Postgres, MinIO, busybox, operator images + PV data).

### 2026-10-25 — 2.1 done: HMS restart + cross-namespace test; 2.2 prepared
Did (Bee): applied E28 fix (HMS restarted); busybox `net-test` pod in `spark-jobs`.
Observed: second start → `HMS schema 3.1.0 exists, nothing to do` (no .beeline warning); bucket-init re-created after its TTL and reran. From `spark-jobs`: `hive-metastore.lake.svc.cluster.local` → 10.97.127.92 (= Service ClusterIP), `thrift 9083: open`, MinIO `/minio/health/ready` → `HTTP/1.1 200 OK`. Read-only (Claude): Postgres `VERSION` = 3.1.0, 74 HMS tables; 0 exceptions in HMS log; top: HMS 408 Mi, MinIO 64 Mi, Postgres 69 Mi. **Step 2.1 done.**
Prepared: `infra/helm/spark-operator/values-local.yaml` (chart 2.2.1, jobNamespaces=[spark-jobs], webhook on, 2 workers, resources, metrics off). Chart 2.2.1 present in https://kubeflow.github.io/spark-operator index; its Spark Role grants pods/configmaps/services/PVCs.

### 2026-10-25 — 2.1c Hive Metastore applied (E28)
Did (Bee): `kubectl apply -k infra/k8s`; rollout.
Observed: rolled out in < 1 min; schema-init: `No HMS schema yet -> schematool -initSchema` … `schemaTool completed`, `Metastore schema version: 3.1.0`; warning E28 (`/home/hive/.beeline`, harmless, emptyDir added); HMS log `Starting Hive Metastore Server`, no exception. `kubectl top`: hive-metastore 401 Mi, minio 64 Mi, postgres 69 Mi.

### 2026-10-25 — 2.1b MinIO + buckets applied; 2.1c prepared
Did (Bee): `kubectl apply -k infra/k8s`; rollout + `kubectl wait` job.
Observed: minio-0 Running in ~20 s; bucket-init: 5× `NETWORK_CONNECTION … curlCode: 7` (connection refused while MinIO started; the retry loop absorbed it), then `bucket lakehouse: created`, `bucket landing: created`; PVC data-minio-0 Bound 5Gi. `statefulset.apps/postgres configured` without a pod restart (no-op apply; `kubectl diff` showed no Postgres change).
Prepared (Claude): `lake/hive-metastore.yaml` (Service + Deployment, `Recreate`, schema init as **initContainer** with Task 1's check-then-create script + wait for Postgres, fsGroup 1000 = uid/gid hive (checked with `id`), hive-site.xml secret + core-site.xml ConfigMap via subPath, tcp probes 9083, 768Mi/1Gi, `-Xmx768m`), `lake/hive/core-site.xml` (FQDN endpoint). Deviation from plan: initContainer instead of a separate Job (ordering is automatic; idempotent).

### 2026-10-25 — 2.1a-fix Postgres re-initialised; 2.1b prepared
Did (Bee): `make k8s-secrets` (new Secret hash: password changed), deleted StatefulSet + PVC, `kubectl apply -k`. Checks (Claude, read-only): log only `airflow role and database created`; secret files `-r--r----- 0 999`; `psql -h postgres.lake.svc.cluster.local -U airflow` → `airflow|airflow`. **E27 fixed.**
Prepared (Claude): `lake/minio.yaml` (StatefulSet, Task 1 digest, httpGet probes `/minio/health/ready|live`, 256/512 Mi, PVC 5 Gi; minikube PV dirs are 0777 so no chown step); `lake/bucket-init-job.yaml` + `bucket_init.py` (pyarrow S3FileSystem in the already-loaded Spark image instead of amazon/aws-cli: saves ~650 MB; fsGroup 185, `imagePullPolicy: Never`, TTL 600 s). Dry-run + `kubectl diff`: only new MinIO objects.

### 2026-10-25 — 2.1a Postgres applied (E26, E27)
Did: removed `~/.minikube/cache/images` (1.5 GB; `~/.minikube` back to 856 MB); created `infra/k8s/secrets.env`; `make k8s-secrets`; `kubectl apply -k infra/k8s`.
Observed: postgres-0 Running in 32 s, PVC `data-postgres-0` Bound 2Gi `standard`, headless Service; DBs airflow, metastore, postgres. Network login as `airflow` failed: **E27** (secret file unreadable by uid 999 → empty password). E26 = my quoting mistake in a check command.
Fix: fsGroup 999 + mode 0440 + stricter init script; re-init Postgres (empty) by deleting StatefulSet + PVC.

### 2026-10-25 — 2.1a prepared: secrets renderer + Postgres manifest (Claude)
Did: `infra/k8s/secrets.env.example`, `infra/k8s/render-secrets.py` (+ `make k8s-secrets`) → `infra/k8s/secrets/{postgres.env, minio.env, hive-site.xml}` (git-ignored, 0600, one Secret per consumer); `infra/k8s/lake/postgres.yaml` (headless Service + StatefulSet, Task 1 digest, password via `POSTGRES_PASSWORD_FILE`, 256/512 Mi, PVC 2 Gi); init script creates role + DB `airflow`; `infra/k8s/kustomization.yaml` (secretGenerator/configMapGenerator).
Observed: test with dummy secrets in a scratch copy: renderer refuses `change-me` values, kustomize builds 4 Namespaces + ConfigMap + Secret (hash suffixes) + Service + StatefulSet; `kubectl apply -k --dry-run=server` accepted; `git check-ignore` confirms secrets.env and secrets/ ignored, example tracked.
Next: Bee renders secrets and applies.

### 2026-10-25 — 2.0e Namespaces + thin images in Minikube (step 2.0 done)
Did: `kubectl top node` (191m CPU, 537 Mi); `kubectl apply -f infra/k8s/00-namespaces.yaml` (lake, spark-operator, spark-jobs, airflow Active); `minikube image load` both thin images.
Observed: `minikube image ls` → `docker.io/library/vnpt-lake-spark:3.5.9-iceberg1.11.0-r1`, `docker.io/library/vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1`. Disk: node volume 1.209 → **6.833 GB (+5.62 GB** for 4.16 GB of images: containerd keeps compressed blobs + unpacked snapshots); `~/.minikube` 856 MB → **2.3 GB**: `cache/images/amd64/` holds tar copies (spark 840 MB, hms 719 MB). Total Task 2 disk so far ≈ **+10.9 GB** (kicbase image 1.89 + node volume 6.76 + ~/.minikube 2.3) — already at the top of the 9–10 GB estimate before Airflow/operator/Postgres/MinIO.
Learned: `minikube image load` from the Docker daemon caches a tar in `~/.minikube/cache/images` (a third copy, on the Ubuntu disk); safe to delete once the image is in the node (re-created on the next load).
Next: delete the image cache; 2.1a secrets + Postgres.

### 2026-10-25 — 2.0d-fix Recreate with kubelet.system-reserved (E25)
Did: `minikube delete`; `minikube start … --extra-config=kubelet.system-reserved=memory=3686Mi`; metrics-server.
Observed: no re-download (kicbase + preload cached); Allocatable 11217116Ki → **7442652Ki (7268 Mi)**, `kubepods.slice/memory.max` = 7621275648; 9 kube-system pods Running within 60 s; metrics-server not ready until its first scrapes ("no metrics to serve"). Disk unchanged vs 2.0d.
Learned: Allocatable = Capacity − reserved − eviction threshold; minikube's kubelet config has `evictionHard: 0%`, so my expected 7168 Mi was 100 Mi low.
Next: 2.0e namespaces + load thin images.

### 2026-10-25 — 2.0d minikube start
Did: `minikube start --driver=docker --kubernetes-version=v1.37.0 --memory=8192 --cpus=6`; metrics-server addon.
Observed: kicbase v0.0.51 (507.6 MiB download) + K8s v1.37.0 preload (347.6 MiB); runtime **containerd 2.3.4** (not Docker inside the node); node Ready after ~80 s, 9 kube-system pods Running. Minikube container 538 MiB idle. Disk: `~/.minikube` 856 MB (Ubuntu disk); Docker images 5.685 → 7.576 GB (+1.89 GB kicbase), volumes 74 MB → 1.209 GB (+1.135 GB node volume) → **≈ +3.9 GB** so far. **E25**: Allocatable reports 10.7 GiB / 8 CPU while the container cap is 8 GiB.
Learned: containerd runtime → inspect images with `minikube image ls` / `minikube ssh -- sudo crictl images`, not `docker` inside the node; `minikube image load/build` still work.
Next: recreate with `kubelet.system-reserved` (E25), then 2.0e namespaces + image load.

### 2026-10-25 — 2.0c Task 1 down, minikube v1.39.0 installed
Did: `make down` (volumes kept); resource check; downloaded `minikube-linux-amd64` v1.39.0 + `.sha256`, `sha256sum --check` OK, `sudo install` to `/usr/local/bin`, temp dir removed.
Observed: Docker `CPUs=8 Mem=11486326784` (10.70 GiB); Ubuntu `free -m` total 10,954 MiB, used 2,132; `/` 6.2 GB used; `minikube version: v1.39.0` (commit 7a9f6a84). Binary 136 MiB.
Decision: Docker shows ≥ 9.5 GiB → start Minikube with `--memory=8192 --cpus=6` (`02_config.md` §2 rule); fall back to 7168/4 if Windows/WSL swaps.
Next: 2.0d `minikube start`.

### 2026-10-25 — 2.0b Per-day expected counts
Did: pushed 5 commits (`6151651..2788fa9`); `make up && make smoke` (Task 1, OK); ran `tests/sql/bronze_daily_counts.sql` with spark-sql on Task 1 Bronze.
Observed: 18 days, totals 21,224 / 10,729 / 116, 0 NULL days; all 116 unmatched on 07-25 (same-day = any-day); empty days 07-07, 07-12–14, 07-20, 07-21. Saved to `tests/expected/bronze_daily.json` and `06_validation.md`.
Next: 2.0c `make down`, install minikube v1.39.0, `minikube start`.

### 2026-10-25 — 2.0a Decisions D1–D3
Did: D1 = chart 1.22.0 + Airflow 3.3.2 (chart gates only at >=3.0.0); D2 = A (24 daily runs, 18 load, 6 skip); D3 = expected counts with Spark SQL on Task 1 Bronze (`tests/sql/bronze_daily_counts.sql`), no extra tool. Task 2 tech = Minikube + Airflow + Spark; Trino = Task 3.
Next: 2.0b run the per-day query (Task 1 stack up), then `make down`, install minikube v1.39.0.

### 2026-10-25 — 2.0 Plan reviewed
Did: plan from Claude desktop reviewed in Claude Code; versions checked upstream; docs `01`–`07` created.
Observed: Spark Operator v2.2.1 = Spark 3.5.5 base, v2.3.0+ = Spark 4.0; operator webhook applies volumes/env/ConfigMaps (must stay on); Airflow chart 1.22.0 ships 3.2.2 (no 3.3.x chart yet); minikube v1.39.0 defaults to K8s v1.37.0; data has 18 days in a 24-day window.
Learned: see `01_knowledge.md` [review] notes.
Next: decisions D1–D3 (`02_config.md`), then 2.0 Prep.
