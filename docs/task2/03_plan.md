# Task 2 — Plan: architecture, steps, risks, repo paths

Knowledge: `01_knowledge.md`. Versions, RAM, disk, open decisions D1–D3: `02_config.md`. Corrections from the 2026-10-25 review are marked **[review]**.

Execution rule (Bee): Claude writes files and docs; **Bee types every command that changes state** (minikube, kubectl apply, helm, make k8s-*) in WSL, with an "Expect:" line per command, and pastes the output. Claude may run read-only checks.

---

## B. Target architecture

```
Namespace lake          : postgres (StatefulSet, DBs metastore + airflow) · minio (StatefulSet + PVC)
                          hive-metastore (Deployment, thin HMS image) · Jobs: bucket-init, hms-schema-init
Namespace spark-operator: Kubeflow Spark Operator 2.2.1 (Helm; controller + webhook), watches spark-jobs
Namespace spark-jobs    : SparkApplication bronze-<date> → driver pod → 1 executor pod
Namespace airflow       : Airflow 3.3.2 (official Helm chart 1.22.0, LocalExecutor, external Postgres)
                          DAG chatbot_bronze: check_landing → bronze (SparkKubernetesOperator) → validate → notify
```

Why our own manifests for MinIO/Postgres/HMS instead of community charts: Bitnami's free images moved in 2025, and the Airflow chart's bundled Postgres is a Bitnami subchart, so we disable it (`postgresql.enabled: false`) and point Airflow at our Postgres. Writing 3 small StatefulSet/Deployment manifests is also the best way to learn the objects in A1.

## C. Steps (one new component at a time)

| Step | Do | Done when |
|---|---|---|
| **2.0 Prep** | Stop Task 1 stack (`make down`); install **minikube v1.39.0**; `minikube start --driver=docker --kubernetes-version=v1.37.0 --memory=… --cpus=…` (`02_config.md` §2); metrics-server; namespaces; **[review]** `minikube image load` the 2 thin images (base for `spark-job`, and HMS); D3: per-day expected counts | `kubectl get nodes` Ready; `kubectl describe node minikube` Allocatable ≥ requests sum; `minikube image ls` shows both images; per-day expected table saved |
| **2.1 Storage + catalog** | Manifests: Postgres StatefulSet + PVC; MinIO StatefulSet + PVC; Job bucket-init (`lakehouse`, `landing`); Job hms-schema-init (check-then-create); HMS Deployment; ConfigMaps for `core-site.xml`, `spark-defaults.conf` (Service DNS names); **[review]** Secret with the rendered `hive-site.xml` (JDBC password) mounted as a file, like Task 1's Compose secret | busybox pod → `nslookup hive-metastore.lake.svc.cluster.local`; HMS log "schema exists"; MinIO console via `kubectl port-forward` |
| **2.2 Spark Operator + first SparkApplication** | Helm install operator **2.2.1** with `spark.jobNamespaces={spark-jobs}`, **webhook on**; landing simulator uploads daily files; job reads `s3://landing/…/dt=<date>`; `SparkApplication` YAML for one date, applied by hand | Application `COMPLETED`; counts for that day = expected; rerun → `overwrite` snapshot, total unchanged; **[review]** Task 1 `make acceptance` still passes after the `bronze_ingest.py` change (regression) |
| **2.3 Airflow** | Chart 1.22.0, `airflowVersion/defaultAirflowTag: 3.3.2` (D1), LocalExecutor, external Postgres secret, Redis/statsd/pgbouncer off, resources set; DAGs baked into a thin image `FROM apache/airflow:3.3.2` (same standard as Task 1) | UI reachable via port-forward; example DAG runs |
| **2.4 DAG** | `chatbot_bronze`: `check_landing` (file for the day exists, else skip: D2) → `bronze` (SparkKubernetesOperator, templated YAML with the interval date) → `validate` (retries=0) → `notify`; RBAC: Role in `spark-jobs` for `sparkapplications` + `pods` + `pods/log` bound to Airflow's ServiceAccount; **[review]** verify SparkApplication naming on retry (`01_knowledge.md` A4) | One scheduled/triggered run succeeds end to end; logs visible in the Airflow UI; a forced retry does not fail with `AlreadyExists` |
| **2.5 Backfill + drills** | Backfill 2024-07-02 → 07-25 (18 data days + 6 skipped); run it twice; drills: kill the driver pod mid-run (Airflow retry), delete an executor pod (Spark reschedules), restart the scheduler mid-run | Backfill ×2 → identical counts per day and per table; each drill recovers with no duplicates |
| **2.6 Alert** (Should) | `on_failure_callback` → Telegram with dag/task/date/try only | A forced failure produces one message with no data contents |
| **2.7 CI → GHCR** (Could) | GitHub Actions builds the images, pushes to GHCR; Minikube pulls with an `imagePullSecret` | Clean Minikube runs from registry images only |
| **2.8 Docs + tag** | `docs/task2/01…07`, errors **E25+**, README with RAM/disk, `git tag task2-done` | `make k8s-up && make k8s-demo` works from zero |

Silver is **not** in Task 2 by default: Task 2 is about running and orchestrating a job, and adding a new job at the same time breaks "one new component at a time". Add a `silver` task to the DAG after 2.5 if time allows (leader 10-23: Bronze only for now).

## D. Risks and first checks

| Symptom | Likely cause | First command |
|---|---|---|
| Pod `Pending` | Not enough memory for its request | `kubectl describe pod …` → Events |
| `OOMKilled` | Limit too low (driver: pandas in overhead) | `kubectl describe pod` → Last State |
| `ImagePullBackOff` | Image not in Minikube / wrong tag / `imagePullPolicy: Always` | `minikube image ls`; set `IfNotPresent` |
| `CrashLoopBackOff` | App error at start (config, DNS, secret) | `kubectl logs --previous` |
| Spark: `pods is forbidden` | Driver ServiceAccount lacks RBAC | `kubectl auth can-i create pods --as=system:serviceaccount:spark-jobs:<sa> -n spark-jobs` |
| Airflow: `sparkapplications … forbidden` | Airflow SA has no Role in `spark-jobs` | same `auth can-i` for the Airflow SA |
| `UnknownHost hive-metastore` | Short name used across namespaces | use `hive-metastore.lake.svc.cluster.local` |
| **[review]** Driver starts without `spark-defaults.conf` / S3 keys | Operator webhook off or not ready (volumes/env are added by the webhook) | `kubectl get mutatingwebhookconfigurations`; operator webhook pod logs |
| **[review]** `failed calling webhook` on SparkApplication pods | Webhook pod down, `failurePolicy: Fail` | `kubectl -n spark-operator get pods`; logs of the webhook |
| DAG not visible | Import error in the DAG file | `airflow dags list-import-errors` in the dag-processor pod |
| Runs process the wrong day | Zero-length intervals (A3 gotcha) | print `data_interval_start/end` in the first task |
| **[review]** Airflow chart job fails (migrations, create-user) | Chart 1.22.0 not released with 3.3.2 (D1) | `kubectl -n airflow logs job/…`; fall back to 3.2.2 |

## E. Questions for the leader

1. Is the Spark Operator (Kubeflow) what production uses, or native spark-submit / another platform?
2. Production Airflow version and executor (to match the lab)?
3. OK to simulate daily landing files split by event day, or should each run reload the full export?
4. ~~Is Silver expected inside Task 2?~~ Answered 10-23: Bronze only for now.

## F. Repo paths

Rule: **add, don't move.** Task 1 (`infra/compose/`, tags `task1-v*`) must still run unchanged, because each task is a standalone mini-project.

### F1. Tree (new = ➕, changed = ✏️)

```
~/vnpt-lakehouse/
├── CLAUDE.md                                ✏️ current task = 2, K8s rules, commands
├── Makefile                                 ✏️ k8s-* targets (F2)
├── .gitignore                               ✏️ + infra/k8s/secrets/ (secrets.env already covered by *.env)
├── docker/
│   ├── spark/  hive/                        Task 1, unchanged
│   ├── spark-job/    Dockerfile             ➕ FROM vnpt-lake-spark:…-r1 + COPY jobs/ (code baked in)
│   └── airflow/      Dockerfile, requirements.txt ➕ FROM apache/airflow:3.3.2 + pinned cncf.kubernetes provider + COPY dags/
├── jobs/
│   ├── bronze_ingest.py                     ✏️ + --source (local path | s3://landing/...); default = Task 1 behaviour
│   └── landing_simulator.py                 ➕ split each Excel by event day → s3://landing/<table>/dt=YYYY-MM-DD/
├── dags/                                    ➕
│   ├── chatbot_bronze.py
│   └── templates/ bronze_sparkapp.yaml, validate_sparkapp.yaml
├── tests/
│   ├── expected/bronze_daily.json           ➕ per-day expected counts (same format as bronze.json)
│   └── test_dag_integrity.py                ➕ run inside the Airflow image (Airflow is not installed in WSL)
├── infra/
│   ├── compose/ …                           Task 1, unchanged
│   ├── k8s/                                 ➕ plain manifests, `kubectl apply -k`
│   │   ├── kustomization.yaml, 00-namespaces.yaml, secrets.env.example
│   │   ├── lake/ postgres.yaml, minio.yaml, bucket-init-job.yaml, hms-schema-init-job.yaml, hive-metastore.yaml
│   │   └── spark-jobs/ spark-defaults.conf, rbac-spark.yaml, rbac-airflow.yaml, bronze-manual.yaml
│   └── helm/                                ➕ spark-operator/values-local.yaml, airflow/values-local.yaml
├── .github/workflows/images.yml             ➕ step 2.7 (Could)
└── docs/task2/ 01_knowledge … 07_drills     ➕
```

### F2. New Makefile targets

| Target | Does |
|---|---|
| `make k8s-start` | `minikube start --driver=docker --kubernetes-version=v1.37.0 …` + metrics-server |
| `make k8s-images` | `minikube image load` the 2 thin images (once), `minikube image build` spark-job and airflow |
| `make k8s-up` | `kubectl apply -k infra/k8s` + `helm upgrade --install` operator and Airflow with the values files, waits for Ready |
| `make k8s-status` | `kubectl get pods -A` + `kubectl top pods -A` |
| `make landing` | port-forward MinIO, run `landing_simulator.py`, list `s3://landing/` |
| `make sparkapp DATE=…` | apply `bronze-manual.yaml` for one date and follow the driver log |
| `make airflow-ui` | `kubectl port-forward` to the Airflow API server |
| `make backfill FROM=… TO=…` | `airflow backfill create` inside the scheduler pod |
| `make k8s-down` | `helm uninstall` + `kubectl delete -k` (PVCs kept) |
| `make k8s-clean` | also delete PVCs (needs `CONFIRM=yes`, like `acceptance-from-zero`) |
