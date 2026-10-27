# Slide 1 — Task 2: Spark on Kubernetes, orchestrated by Airflow
VNPT AI · Data Engineering onboarding lab · Task 2 of 5
The Task 1 Bronze job, run per day on Minikube by Airflow, shipped by CI
Bee (Pham Quang Minh) · 2026-10-27

Visual: dark title slide; small orange line "VNPT AI · DE onboarding lab · Task 2".

Speaker notes: Task 1 built the lake: MinIO, Iceberg, Hive Metastore and a reliable Bronze load. Task 2 takes the same job and makes it run like production: on Kubernetes, once per day of data, scheduled, retried, validated and alerted by Airflow, with images built by CI and deployed by digest. Same structure as Task 1: the knowledge first, then how this task uses it, then what I learned while running it, then the errors.

---
# Slide 2 — What Task 2 does with the data
In one sentence: every day of chatbot logs lands as a file in MinIO; Airflow starts the Spark job on Kubernetes for that day, checks the result and alerts on failure.

Lab target flow: Excel export → S3 landing zone → Spark ETL (on K8s) → Bronze Iceberg tables, orchestrated by Airflow

What Task 2 implements:
- Landing zone: the two Excel files split by the day written in CREATED_TIME → s3://landing/<table>/dt=YYYY-MM-DD/ (plays the source system dropping one export per day)
- DAG chatbot_bronze, one run per day: check_landing → bronze (Spark on K8s) → validate (23 checks) → notify
- 24 calendar days (2024-07-02 → 07-25): 18 load, 6 have no export and are skipped
- Same Bronze tables and rules as Task 1 (raw strings + load metadata), now partitioned by the data day

Agenda: Knowledge (Kubernetes, Minikube, operators, Spark on K8s, Airflow 3, Helm, CI/CD) · Stack of this task · Running it (intervals, backfill, drills, backup, maintenance, sizing) · Errors

Visual: left "s3://landing/…/dt=2024-07-24/x.xlsx" → Airflow box (check → bronze → validate → notify) → Kubernetes box (driver pod + executor pod) → "Bronze: Iceberg on MinIO"; a Telegram icon under Airflow.

Speaker notes: The landing zone promised in Task 1 now exists. A run is identified by its day, which is what makes retries and backfills safe: rerunning a day replaces exactly that day.

---
# Slide 3 — The problem
- Task 1 ran by hand: `make bronze DATE=…` on one machine, one container per service
- Needed for production:
  - Scheduled: one run per day of data, without a person
  - Recoverable: transient failures retried; a rerun or backfill never duplicates
  - Observable: someone is told when it fails or is late; logs survive restarts
  - Reproducible: the same images and config from Git, no "works on my laptop" builds
  - Least privilege: each component can touch only what it needs
- Constraint: one laptop (WSL2, 10 GiB for everything, disk already tight)

Visual: left "make bronze DATE=…" with a person; right a calendar of 24 days with green ticks and 6 grey "skipped" squares, a bell icon and a GitHub icon.

Speaker notes: Each requirement maps to a tool: Kubernetes for running and restarting, Airflow for scheduling and retries, GitHub Actions + GHCR for reproducible images, Telegram for alerts. The rest of the deck shows how each one works and how I proved it.

---
# Part 1 — Knowledge

---
# Slide 4 — Kubernetes: what it is and how it works
- What: a system that keeps containers running across machines from a declared desired state
- Control plane:
  - kube-apiserver: the only entry point; every client (kubectl, operators, kubelet) reads and writes objects through its REST API
  - etcd: the key-value store holding every object
  - kube-scheduler: picks a node for each new pod (based on requests, not real usage)
  - kube-controller-manager: controllers that reconcile (Deployment → ReplicaSet → Pods, Job, CronJob…)
- Node: kubelet (starts pods via the container runtime, reports status, enforces limits) · containerd (runtime) · kube-proxy (Service virtual IPs) · CNI plugin (pod network; ours kindnet)
- The core idea, the reconcile loop: observe actual state → compare with desired → act → repeat. Nothing is "run once"; everything is continuously corrected

Visual: control plane box (apiserver in the middle, etcd under it, scheduler and controller-manager on the sides) → arrows to a node box (kubelet → containerd → pods, kube-proxy); kubectl talking only to the apiserver.

Speaker notes: This is why a killed pod comes back: no one restarts it explicitly, a controller sees fewer pods than desired and creates one. Our drills rely on this. The scheduler only looks at requests, which is why requests must be honest (E25, E52).

---
# Slide 5 — Kubernetes objects used in Task 2

| Object | What it is | Used for |
|---|---|---|
| Namespace | A named scope for objects, names, RBAC | lake · spark-operator · spark-jobs · airflow |
| Pod | One or more containers sharing network and volumes; the scheduling unit | every running process |
| Deployment | N identical stateless pods, rolling updates | HMS, Airflow components, operator |
| StatefulSet | Stable pod names + one volume per pod | Postgres, MinIO |
| Service | Stable DNS name + virtual IP in front of pods | minio.lake.svc.cluster.local:9000 … |
| PersistentVolumeClaim | Storage that survives pod restarts | MinIO and Postgres data |
| ConfigMap / Secret | Config / credentials, mounted as files | core-site.xml, hive-site.xml, MinIO keys |
| Job / CronJob | Run to completion / on a schedule | bucket-init, minio-users, pg-backup (01:00 UTC) |
| ServiceAccount + Role + RoleBinding | Pod identity + allowed API verbs | Airflow may create SparkApplications in spark-jobs only |
| CustomResourceDefinition | A new object type in the API | SparkApplication |
| Mutating webhook | HTTPS service that edits objects before they are stored | Spark Operator adds volumes, env, Secrets to Spark pods |

Visual: four namespace boxes with their objects as small icons.

Speaker notes: Cross-namespace calls use the full DNS name <service>.<namespace>.svc.cluster.local. Secrets are only base64-encoded, not encrypted, so they are mounted as files and never printed.

---
# Slide 6 — Requests, limits, Allocatable, OOM
- Request: what the scheduler reserves for a pod on a node; limit: the hard cap enforced by the kernel (cgroup)
- Allocatable = node capacity − kube-reserved − system-reserved − eviction threshold; the scheduler admits pods while the sum of requests ≤ Allocatable
- Over the memory limit → the kernel's OOM killer kills a process in the container → exit code 137 (128 + SIGKILL 9), status OOMKilled, kubelet restarts the container
- QoS: requests = limits → Guaranteed (Spark pods); requests < limits → Burstable (most of ours); none → BestEffort (first to be evicted)
- Spark pods: request = limit = JVM memory + memoryOverhead (bronze driver 1g + 1g for pandas = 2 GiB; executor 768m + 384m)
- Our numbers: Minikube container capped at 8 GiB; kubelet first believed 10.7 GiB (E25) → system-reserved 3686Mi → Allocatable 7268 Mi; idle requests 4,036 Mi + a bronze run 3,200 Mi = 7,236 Mi

Visual: a bar of 8 GiB: system/kubelet slice, then Allocatable 7268 Mi split into lake / Airflow / operator / Spark driver / executor requests; a red line for one container limit with "137".

Speaker notes: Requests protect scheduling, limits protect the node. If the scheduler believes there is more memory than really exists, it admits pods that later all get killed together; that was E25. If a container's limit is too small for what really runs inside it, it is killed again and again; that was E52.

---
# Slide 7 — Minikube: a whole cluster in one container
- Docker driver: the "node" is one Docker container (image kicbase) inside Docker Desktop's WSL2 VM
- Inside the node: systemd, kubelet, containerd 2.3.4 and all control-plane pods; pods are containers of containerd, not of Docker
- Separate image store: an image in Docker Desktop is not visible to the cluster; the node pulls from the registry itself (or `minikube image load`)
- Our start: `minikube start --driver=docker --kubernetes-version=v1.37.0 --memory=8192 --cpus=6 --extra-config=kubelet.system-reserved=memory=3686Mi`
- Lifecycle: `minikube stop` keeps everything (PVC data, images); `minikube delete` destroys the node, its images and all data
- Laptop sleep/restart stops the node container → `minikube start` first (E40)

Visual: Windows → WSL2 VM → Docker Desktop → container "minikube" (kicbase) → inside: kubelet, containerd, pods; a second image store drawn inside the node.

Speaker notes: Minikube is a real single-node Kubernetes, so manifests and Helm charts are the same as on a production cluster; what changes is scale and high availability. Use crictl, not docker, to look at containers inside the node.

---
# Slide 8 — Operators and CRDs: the Kubeflow Spark Operator
- Operator pattern: a CRD (new object type) + a controller that reconciles it
- SparkApplication (CRD): desired Spark job in YAML: image, main file, arguments, driver/executor sizes, Spark conf, restartPolicy
- Controller: sees a new SparkApplication → runs spark-submit (cluster mode) → tracks state SUBMITTED → RUNNING → COMPLETED / FAILED → cleans up after timeToLiveSeconds
- Mutating webhook: when the driver and executor pods are created, it injects volumes, env, envFrom (our s3-credentials Secret), labels → must stay on (failurePolicy Fail: no half-configured Spark pods)
- Version 2.2.1: the last release built on Spark 3.5 (2.3+ ship Spark 4.0, and the operator's own spark-submit launches our 3.5.9 driver)
- restartPolicy Never: retries belong to Airflow only (one retry layer)

Visual: YAML "kind: SparkApplication" → controller → spark-submit → driver pod → executor pod; webhook box intercepting the pod creation.

Speaker notes: The operator turns a Spark job into a Kubernetes object with a status. Airflow does not need Spark installed: it only creates the object and watches its status and driver log.

---
# Slide 9 — Spark on Kubernetes: cluster mode
- spark-submit with master k8s://… and deploy mode cluster creates a driver pod
- The driver (with its ServiceAccount) asks the API server for executor pods, gives them tasks, and deletes them at the end
- Driver pod stays Completed with its log; executor pods disappear
- Memory: pod = spark.*.memory (JVM heap) + memoryOverhead (off-heap + Python): pandas parses the Excel in the driver's Python process, so the bronze driver gets 1g overhead
- Same job code as Task 1: only the master, the source (s3://landing instead of a folder) and the date change
- Fault tolerance: lost executor → driver requests a new pod and recomputes lost tasks (lineage); lost driver → the job fails, Airflow retries

Visual: driver pod in spark-jobs → API server → 1 executor pod; arrows from both to MinIO (S3) and from the driver to HMS (Thrift).

Speaker notes: In Task 1 local mode, driver and executor were one JVM. Here they are separate pods, which is what drills 1 and 2 test: an executor loss is handled by Spark, a driver loss by Airflow.

---
# Slide 10 — Airflow 3: architecture
| Component | Role |
|---|---|
| API server | UI + REST API + Task Execution API (in Airflow 3 tasks talk to this API, not to the database) |
| Scheduler | Creates DAG runs and task instances, hands them to the executor |
| DAG processor | Parses DAG files (separate process since Airflow 3) |
| Triggerer | Runs async waits and callbacks (our DeadlineAlert) without holding a worker |
| Metadata DB | Postgres: runs, task states, variables, backfills |
| Executor | LocalExecutor: tasks run as processes inside the scheduler pod (`parallelism` worker processes) |

- Why LocalExecutor: the heavy work runs in Spark pods; an Airflow task only creates a SparkApplication and waits → no Celery/Redis, no pod per task
- Consequence (E52): the scheduler pod is also the worker; it must be sized for its idle workers + a running task

Visual: five boxes around a Postgres cylinder; arrows "parse" (dag-processor), "schedule" (scheduler), "execute" (worker processes inside the scheduler box), "callbacks" (triggerer), "UI/API" (api-server).

Speaker notes: Airflow 3 moved tasks behind an API, separated DAG parsing, and replaced SLAs with deadline alerts. Several defaults changed too, which produced three of my errors (E38, E47, E49).

---
# Slide 11 — Airflow concepts this task relies on
- DAG: tasks + dependencies + schedule; task = an operator instance (PythonOperator via @task, SparkKubernetesOperator)
- Logical date (`ds`): the label of the data a run processes; data interval [start, end) = the period it covers
- Timetable: decides intervals and when a run starts; a daily data-interval run for day D starts after D+1 00:00
- Catchup off + backfill: history is filled on purpose (`airflow backfill create`), executed by the scheduler
- Retries / retry_delay / execution_timeout per task; dagrun_timeout per run; max_active_runs = 1 (runs write the same tables)
- Templates: fields rendered with Jinja (`{{ ds }}`, `{{ var.value.spark_job_image }}`), strict: an undefined variable is an error
- Connections and Variables: from env (`AIRFLOW_CONN_*`, `AIRFLOW_VAR_*`) mounted from Secrets, so no credentials in the database or Git
- Callbacks: on_failure_callback (after the last try), DeadlineAlert (2 h after the run was queued)

Visual: a timeline 07-02 | 07-03 | 07-04 with one bracket per interval and "run starts" arrows at each interval end.

Speaker notes: The logical date is the contract between Airflow and the job: the job never reads "now", it reads the day it is told to process. That is what makes a run for 2024-07-24 executed in 2026 correct.

---
# Slide 12 — Helm and kustomize: two ways to deploy manifests
| | Helm | kustomize (kubectl apply -k) |
|---|---|---|
| Model | Chart = templates + values → rendered → a release with revisions | Plain YAML + overlays/patches/generators |
| Used for | Third-party software: Airflow chart 1.22.0, Spark Operator chart 2.2.1 | Our own manifests: lake namespace, RBAC, Secrets, ConfigMaps |
| Change | `helm upgrade --install … -f values-local.yaml` (revision +1, rollback possible) | `kubectl apply -k infra/k8s` (idempotent) |
| Secrets | chart reads existing Secrets by name | secretGenerator builds Secrets from git-ignored files |

- Our rules: chart file from the Apache CDN checked by SHA-512 (E29); every Helm install records `--description "git <sha>"`; deploy only from clean main (E39)
- ConfigMap/Secret generators add a content hash to the name → a changed config rolls the pods that use it

Visual: left a chart folder → values-local.yaml → `helm upgrade` → release rev 1, 2, 3; right YAML files + secrets/ → kustomize → applied objects.

Speaker notes: Helm is good for software someone else maintains; kustomize keeps our own YAML readable without a template language. Both end as the same objects in the API server.

---
# Slide 13 — CI/CD: GitHub Actions → GHCR → deploy by digest
- Pull request → CI (4 checks): ruff lint + format, 52 unit tests · Dockerfile checks · kustomize render with dummy secrets + "every image has a digest" · DAG integrity inside the Airflow image (import, rules, strict template render with and without a logical date)
- Merge to main → images workflow: build changed images → Trivy scan (report) → push ghcr.io/beno1tp/<image>:<git sha> → print the digest
- Bump PR: copy the digest into infra/k8s/images.env (+ kustomize images:) → CI → merge → `make airflow-install` / `make k8s-up`
- Digest (sha256 of the manifest) vs tag: a tag can be moved, a digest cannot → what runs is exactly what was reviewed
- Private registry: the cluster pulls with a token that has only read:packages (Secret ghcr-pull)
- Rollback = deploy the previous digest; old versions pruned with `make ghcr-prune` (keeps deployed + 2 newest)

Visual: PR → CI checks (4 green ticks) → merge → build → scan → GHCR (sha256:…) → bump PR → Minikube pulls by digest.

Speaker notes: No image is built on the laptop: that saves CPU, disk and a second copy of every image, and it is the production flow. 35 merged PRs (#1–#37) went through this loop in Task 2.

---
# Part 2 — The stack of Task 2

---
# Slide 14 — Architecture

| Namespace | Runs |
|---|---|
| lake | Postgres 16 (DBs metastore + airflow) · MinIO · Hive Metastore 3.1.3 · Jobs bucket-init, minio-users · CronJob pg-backup |
| spark-operator | Spark Operator 2.2.1 controller + webhook (pinned by digest) |
| spark-jobs | SparkApplications bronze / validate / maintenance → driver + 1 executor (image vnpt-lake-spark-job) |
| airflow | api-server · scheduler (LocalExecutor, parallelism 2) · dag-processor · triggerer · image vnpt-lake-airflow (DAGs + alert plugin baked in) |

- DAGs: chatbot_bronze (daily 00:00 UTC) · lake_maintenance (Sunday 02:00 UTC) · alert_smoke (manual, proves alerts)
- External: GitHub + GHCR (images), Telegram (alerts)

Visual: Minikube box with the four namespaces; outside: GitHub/GHCR (pull arrow into the node), Telegram (arrow out of Airflow), the laptop folder with the Excel files (arrow into the landing pod).

Speaker notes: Same lake as Task 1, now as Kubernetes objects. Airflow uses the lake's Postgres for its own metadata database, so one database server holds both catalogs.

---
# Slide 15 — How the pieces talk

| From → To | How | Credentials |
|---|---|---|
| Airflow check_landing → MinIO | S3 LIST (S3Hook) | MinIO user airflow-landing (read landing) |
| Airflow bronze/validate → K8s API | create SparkApplication, read pod log | ServiceAccount + Role in spark-jobs |
| Spark Operator → K8s API | spark-submit creates the driver pod | operator ServiceAccount |
| Driver → K8s API | create/delete executor pods | spark-operator-spark ServiceAccount |
| Spark → HMS | Thrift :9083 | — |
| Spark → MinIO | S3 (Iceberg S3FileIO) | MinIO user spark-jobs (read landing, rw lakehouse) |
| HMS → Postgres / MinIO | JDBC :5432 / S3A | hive-site.xml Secret / user hive-metastore |
| Airflow → MinIO | task logs to s3://airflow-logs | user airflow-logs |
| pg-backup → MinIO | pg_dump upload | user backup-writer (no delete) |
| Airflow → Telegram | HTTPS Bot API | bot token Secret |
| Node → GHCR | image pull | ghcr-pull (read:packages) |

Speaker notes: Six MinIO users, one per consumer, each with a JSON policy; only bucket-init and minio-users get the root keys. `make s3-access-test` proves both what is allowed and what is denied.

---
# Slide 16 — Why these, and not others

| Role | Ours | Alternatives | Why ours here |
|---|---|---|---|
| Local cluster | Minikube (docker driver) | kind, k3d, Docker Desktop K8s | named by the lab; addons (metrics-server); full kubelet config |
| Spark on K8s | Spark Operator 2.2.1 | plain spark-submit, KubernetesPodOperator, Apache spark-kubernetes-operator (0.x) | declarative object with status; Airflow needs no Spark |
| Orchestrator | Airflow 3.3.2 | Dagster, Prefect, Argo Workflows | named by the lab; backfill, retries, deadlines, UI |
| Executor | LocalExecutor | Celery, KubernetesExecutor | fewest pods and RAM; heavy work is in Spark pods |
| Airflow → Spark | SparkKubernetesOperator | SparkSubmitOperator, BashOperator + kubectl | tracks state, streams logs, reattaches after a crash |
| Registry | GHCR (private) | Docker Hub, local registry | same place as the code; token per scope |
| Deploy | Helm (3rd party) + kustomize (ours) | raw YAML, Argo CD | charts for complex software, plain YAML for ours |

- Honest view: for 32k rows a cron job would do; the lab teaches the stack that scales and operates

Speaker notes: GitOps with Argo CD would be the next step for deploy-by-digest: the cluster pulls the desired state from Git instead of me running make.

---
# Slide 17 — The landing zone and "the day"
- Day = the date as written in CREATED_TIME (Oracle text, no time zone conversion); bot_predict rows split by their own CREATED_TIME
- Landing simulator (one-off pod): splits both Excel files by day → s3://landing/<table>/dt=YYYY-MM-DD/, checks each day against tests/expected/bronze_daily.json, prints counts only
- Expected per-day counts computed independently with Spark SQL on the validated Task 1 tables (D3)
- 18 days with rows; 07-24 + 07-25 hold 96.7 % of chat rows; 116 unmatched predict rows all on 07-25
- No file for a day → check_landing raises AirflowSkipException → run skipped (D2): a missing export is not an empty export
- Only one of the two tables present → fail (a half export is an incident)

Visual: the two Excel files → simulator → a folder tree landing/chatlog/dt=2024-07-02 … dt=2024-07-25 with 6 missing days greyed out.

Speaker notes: The simulator plays the source system. In production the export would arrive by itself; the DAG would not change.

---
# Slide 18 — The DAG: chatbot_bronze

| Task | What | Retries | Why |
|---|---|---|---|
| check_landing | S3 LIST for dt=ds in both tables | 0 | deterministic: skip or fail fast (no logical date → fail, E47) |
| bronze | SparkApplication bronze-ds: read landing, overwritePartitions(_ingest_date = ds) | 2, every 5 min | transient: pod eviction, HMS restart |
| validate | SparkApplication validate-ds: 23 checks | 1, after 1 min | a wrong number fails again; an interrupted process (E41) is retried |
| notify | prints the summary | — | — |

- Schedule CronDataIntervalTimetable("0 0 * * *", UTC); max_active_runs 1; execution_timeout 20 min; dagrun_timeout 3 h
- One retry layer: SparkApplication restartPolicy Never
- Alerts: on_failure_callback after the last try; DeadlineAlert 2 h after queued; Telegram message = DAG, task, day, run, attempt (no data)

Visual: four task boxes in a row with retry counters; a clock icon "2 h" over the run; a Telegram bubble "❌ chatbot_bronze.bronze FAILED for day 2024-07-24 (attempt 3/3, final)".

Speaker notes: Retrying in both Airflow and Spark would multiply attempts (3 × 3 = 9) and hide failures. The SparkApplication name gets a random suffix from the provider, so a retry never collides with the failed attempt's object.

---
# Slide 19 — Security and least privilege
- Secrets: from a git-ignored secrets.env → rendered files (mode 600) → kustomize secretGenerator → mounted as files; never in Git, images, Helm values or logs
- MinIO: 6 service users with JSON policies; Spark cannot create buckets or write landing; backup-writer cannot delete
- RBAC: Airflow's ServiceAccount may manage SparkApplications and read pods/logs in spark-jobs only, and cannot list Secrets
- Images: pinned by digest everywhere (CI rejects a tag-only image); Trivy scan per build
- Admin password never printed (E35: chart NOTES did); Telegram token pasted once → revoked (E43)
- Not in the lab: TLS, HMS authentication, secret rotation, NetworkPolicy (kindnet does not enforce it)

Visual: a lock per arrow of the "how the pieces talk" diagram, each labelled with its user.

Speaker notes: The rule is one identity per consumer with only what it needs, and proof by a negative test, not just a positive one.

---
# Part 3 — Advanced knowledge from running it

---
# Slide 20 — Data intervals in Airflow 3 (E38)

| schedule= | Timetable in Airflow 3.3.2 | Interval | Logical date |
|---|---|---|---|
| "0 0 * * *" (cron string) | CronTriggerTimetable | zero length | = run time |
| timedelta(days=1) | DeltaTriggerTimetable | zero length | = run time (16:29:09 = unpause time) |
| CronDataIntervalTimetable("0 0 * * *", UTC) | data-interval timetable | [D 00:00, D+1 00:00) | D |

- Airflow 3 changed the defaults (create_cron_data_intervals / create_delta_data_intervals = False) → my first DAG had zero-length intervals at random times
- Manual runs: may have no logical date at all (E47), and a manual run with a logical date gets today's interval, not that day's → the job takes the day from ds, never from the interval
- One run per (DAG, logical date): rerun a day = clear its run, not trigger again (E49)

Speaker notes: This is the most important Airflow 3 change for data pipelines. I found it because the first scheduled run printed its interval and it was empty. Lesson: print the interval in the first task and assert it.

---
# Slide 21 — Backfill and idempotency on Kubernetes
- `make backfill` = `airflow backfill create --from-date 2024-07-02 --to-date 2024-07-25 --max-active-runs 1`
- 24 runs: 18 loaded (exact counts), 6 skipped; run twice → identical counts on 18 days × 2 tables
- Why it is safe: each run overwrites only its own partition (`overwritePartitions` on _ingest_date = ds) in one Iceberg commit
- A rerun adds a snapshot, not rows: after a 07-24 rerun, snapshots 6 → 7, files 22 → 23, rows unchanged
- Grain rule from Task 1, now enforced by the scheduler: one day = one landing folder = one run = one partition = one validation

Visual: grid 24 days × 2 backfills, all green or grey, with "= identical" between the two rows.

Speaker notes: Backfill is just "run these intervals again". Because each interval maps to one partition, history can be rebuilt at any time without a cleanup step.

---
# Slide 22 — Data quality: 23 checks per day
- 13 day-generic rules (tests/sql/bronze_rules.sql), per table: partition not empty, ID not null, ID unique, CREATED_TIME parseable, event day = partition day; chatlog SOURCE ∈ accepted values; bot_predict ID_CHATLOG not null; both tables from one batch
- 10 exact reconciliation checks against bronze_daily.json (rows per table, unmatched predict rows…)
- The validate task fails the run on any FAIL; the alert says which day
- In the job itself: refuse success if rows written ≠ rows read
- Proof in the report: `[validate] 23/23 checks passed for _ingest_date=…` × 18 days

Speaker notes: Exact counts only work while we have a reference; the generic rules still work for tomorrow's day, which is what production needs.

---
# Slide 23 — Failure drills: orchestration vs compute

| Drill | What breaks | Who recovers | Result |
|---|---|---|---|
| 1 Kill the driver | Spark job fails | Airflow retry after 5 min → new SparkApplication | exact counts, no duplicates |
| 2 Delete the executor | lost tasks | Spark driver, same attempt | no Airflow retry |
| 3 Restart the scheduler | task process dies | Airflow retry; reattach to the running driver by labels | first failed (E41: 10 s grace), fixed: 120 s grace + validate retry 1 |
| 4 Backfill twice | — | idempotency | identical |
| 5 Forced failure / late run | — | alerts | one ❌ and one ⏰ in Telegram |

- Lesson: Spark state lives in Kubernetes, not in Airflow; labels (dag_id, task_id, run_id, try_number) make the work findable after the orchestrator crashes

Visual: three small timelines with the breaking point and the recovery arrow.

Speaker notes: Drill 3 failed first: the scheduler pod was replaced while validate ran, and validate had no retry. That is the value of drills: the configuration looked right until it was tested.

---
# Slide 24 — Backup and recovery
- Nightly CronJob: pg_dump of metastore + airflow → s3://backup/postgres/<timestamp>/
- Drill 6, lose the catalog DB: HMS starts empty → Spark "table not found" while every file is still in MinIO → restore → 23/23
- Drill 7, a commit newer than the backup: restore brings back the older pointer (RPO = time since the last backup) → `register_table` from the newest metadata.json → latest commit back
- Theory: the catalog stores only pointers; Iceberg tables are self-describing (metadata tree on S3)
- Gap found at the end: backups live in the same MinIO → `minikube delete` removed them with the data → production needs an off-cluster copy

Visual: Postgres → pg_dump → MinIO backup bucket; a broken HMS; arrows "restore" and "register_table(metadata.json)".

Speaker notes: Losing the catalog loses names, not data. That is a strong property of Iceberg, but only if someone knows how to re-register.

---
# Slide 25 — Table maintenance
- Weekly lake_maintenance (Sunday 02:00 UTC) and `make maintenance [DRY=yes]`
- Steps: metadata retention (10 previous metadata.json) → expire_snapshots (older than 7 days, keep ≥ 5) → rewrite_data_files → rewrite_manifests → remove_orphan_files (older than 72 h) → assert rows unchanged → time-travel check
- Measured on chatlog: snapshots 50 → 6, data files 50 → 22, metadata files 50 → 11, rows 21,224 unchanged; time travel to the oldest snapshot gone (expected)
- Iceberg guard: orphan removal refuses intervals < 24 h (a running write's files look like orphans) → make refuses too (E46)
- Retention = the time-travel window: a business decision

Visual: before/after bars for snapshots, files, metadata; rows bar unchanged.

Speaker notes: Maintenance changes history and files, never data, and the job asserts it. Six snapshots, not five: rewrite_manifests commits one more.

---
# Slide 26 — Observability: alerts and durable logs
- Telegram "Alert group": task failure after the last try + DeadlineAlert 2 h after queued; metadata only (DAG, task, day, run, attempt)
- alert_smoke DAG proves both paths on demand (paused otherwise)
- Task logs uploaded to s3://airflow-logs: survive scheduler restarts and `helm upgrade` (they died with the pod before, E41)
- UI reads logs by pod IP (E37: pod hostnames are not in cluster DNS)
- `make dag-report`: per day landing yes/no, rows loaded, validation, read from S3 (E51)
- Known: the operator may repeat the last second of driver log after a reconnect (the report counts each line once per file)
- Not in the lab: Prometheus/Grafana metrics, Spark History Server

Visual: run fails → triggerer/worker → Telegram bubble; task log → MinIO bucket → UI.

Speaker notes: An alert must tell you where to look without exposing data. A log that disappears with the pod is not a log.

---
# Slide 27 — Sizing the scheduler (E52)
- First from-zero demo: PASSED, but `exit code 137` and `container not found ("scheduler")` ~8 times; RESTARTS 9, OOMKilled at 1 GiB
- Inside the container (psutil): 4 × LocalExecutor worker ~263 MiB + scheduler 194 + serve-logs 152 → idle 893 MiB of 1 GiB
- + a running task's supervisor + my per-minute CLI poll (~300 MB) → over the limit → task killed → retry reattached to the running driver (data correct, 6 log lines missing)
- Fix: parallelism 4 → 2 (one task at a time anyway); scheduler 1 Gi request / 1.5 Gi limit; all make CLI calls in the api-server container; the demo fails if the scheduler restarted
- Second from-zero demo: 0 restarts, 16 min instead of 44 (the difference was the retries)

Visual: stacked bar inside a 1 GiB box (4 workers, scheduler, serve-logs, task, CLI) overflowing; next to it the 1.5 GiB box with 2 workers and room left.

Speaker notes: The run was green and still wrong. Restart counts are part of acceptance now. With LocalExecutor, the scheduler pod is the worker, so size it like one.

---
# Slide 28 — From zero: the acceptance test
- `minikube delete` → `make k8s-up` → `make k8s-demo`
- k8s-up (idempotent): cluster with the E25 flags → secrets → lake → waits (Postgres, MinIO, setup Jobs, HMS) → operator → Airflow → admin user; 12.5 min (images pulled from GHCR; Airflow image ~10 min on a slow day); rerun 20 s
- k8s-demo: landing → unpause → backfill 24 days → wait → report → fail on any failed run or scheduler restart → unpause maintenance; 16 min
- Result: 18 days × 2 tables exact, 23/23 ×18, 6 skipped, 0 failed, 0 restarts; the maintenance catch-up run succeeded right after
- Footprint: node 4.4 GiB used idle; disk ≈ 11 GB (node volume 8.3 GB, kicbase ~1.9 GB, ~/.minikube 0.86 GB)

Visual: three commands on a terminal with timings; a green "PASSED".

Speaker notes: Everything in this deck can be rebuilt from Git and the two Excel files. The first from-zero run found two bugs (E51, E52) that months of incremental work would have hidden.

---
# Part 4 — Errors

---
# Slide 29 — Errors and fixes (grouped, E25–E52)

| Theme | Errors | Lesson |
|---|---|---|
| Resources | E25 Allocatable 10.7 GiB on an 8 GiB node; E52 scheduler OOM | Measure what the kubelet and the container really see, then size |
| Airflow 3 changes | E34 DagBag API; E37 log hostname; E38 zero-length intervals; E47 no logical date; E49 one run per date | Read the version's defaults; print and assert what the run receives |
| Deploy discipline | E31/E32 digests not recorded; E33 secrets too early; E39 deployed from a branch; E36 chart warnings | Deploy only reviewed main by digest; record the git SHA in the release |
| Secrets | E27 secret file not readable (fsGroup); E35 password in chart NOTES; E42 my test overwrote secrets; E43 token in chat | Files with the right group; never print; test in a scratch copy |
| Recovery | E41 rollout killed validate; E46 Iceberg's 24 h orphan guard | Grace periods and one retry for interruptions; respect guard rails |
| Readers and reports | E44 alert texts; E51 report read local logs after logs moved to S3 | Move every reader with the source of truth; an empty report must fail |
| Tooling | E26 quoting; E29 slow chart archive; E30 kubectl -A; E40 cluster stopped after sleep; E45 placeholder in a command; E48 requires_python; E50 old gh | Pin by checksum; check tool versions; commands without placeholders |

Speaker notes: Full symptom → root cause → fix → theory for each is in 05_errors_and_fixes.md. Most errors were not in Spark or Iceberg anymore: they were in operating the platform around them.

---
# Slide 30 — Debugging story: E38, the empty interval

| Step | Evidence |
|---|---|
| Symptom | first scheduled run: logical_date 2026-10-25T16:29:09 = unpause time; data_interval [16:29:09, 16:29:09) |
| Hypothesis | schedule=timedelta(days=1) should give [D, D+1) (as in Airflow 2) |
| Source | Airflow 3.3.2 config: create_delta_data_intervals = False → timedelta becomes DeltaTriggerTimetable (zero length), same for cron strings |
| Fix | CronDataIntervalTimetable("0 0 * * *", timezone="UTC"); CI integrity test requires a data-interval timetable for lake DAGs |
| Proof | backfill: logical 2024-07-02, run_after 2024-07-03, interval [07-02, 07-03) |

Speaker notes: The job used ds, so data was never wrong, but the run times and intervals were meaningless. The plan written before the run had the Airflow 2 assumption; the printed interval caught it.

---
# Slide 31 — Debugging story: E47, the run without a date

| Step | Evidence |
|---|---|
| Symptom | manual lake_maintenance dry run failed twice (10 min apart), ❌ "FAILED for day none … attempt 2/2"; local log file 0 lines |
| Where | full log in s3://airflow-logs: `UndefinedError: 'ds' is undefined` rendering application_file, template line `name: maintenance-{{ ds }}` |
| Cause | Airflow 3 manual runs may have logical_date None → no ds; templates render with StrictUndefined |
| Fix | maintenance name without ds; chatbot_bronze fails fast without a date (no retry); CI renders every template with and without a logical date |
| Bonus | the first CI run of the new check failed on my own code: DagBag had already replaced the template path by its content |

Speaker notes: The retry waited 10 minutes for an error that could never succeed. Deterministic failures should fail fast; transient ones should retry. The fix includes a test, so the mistake cannot come back.

---
# Slide 32 — Production gaps: closed and open

| Gap | Closed in Task 2 | Open (production answer) |
|---|---|---|
| CI/CD | PR checks, image build/scan/push, digest bump PRs, deploy guard, pre-commit | environment promotion; Trivy gating |
| Orchestration | data-interval schedule, retries, backfill, deadline + failure alerts | — |
| Data quality | 13 generic rules + exact counts, fail the run | — |
| Backup | nightly dump, restore + register drills | off-cluster copy (same MinIO today) |
| Observability | Telegram, remote logs, report | metrics dashboards, History Server |
| Security | 6 service users, Secrets as files, RBAC, digests | TLS, rotation, NetworkPolicy |
| Maintenance | weekly DAG | — |
| Availability | K8s restarts failed pods | one replica each = no HA |
| Publish safety | — | Write-Audit-Publish with an Iceberg branch |

Speaker notes: The rule was: take what is cheap in RAM and teaches a production habit; document the rest with the production answer.

---
# Slide 33 — Open questions and next steps
- Silver placement (D6): Spark job at the start of Task 3, added to the DAG before Trino; dbt only for Gold in Task 4. Confirm?
- GHCR images private (token read:packages) or public?
- Off-cluster backup copy: worth it in the lab, or documented only?
- Next: Silver job (parse CREATED_TIME, dedupe, final_intent, flags) → chatbot_pipeline DAG → Trino on Bronze + Silver (Task 3)

Speaker notes: Silver is required by the lab's Medallion design; Task 2's goal was the step-1 job on Kubernetes under Airflow, so it stops at Bronze.

---
# Appendix A1 — Versions and the compatibility chain

| Component | Version | Reason |
|---|---|---|
| Minikube / Kubernetes | v1.39.0 / v1.37.0 | newest at start; kubectl 1.37.1 within skew |
| Spark Operator | 2.2.1 by digest | last release on Spark 3.5 |
| Spark / Iceberg / HMS | 3.5.9 / 1.11.0 / 3.1.3 | unchanged from Task 1 |
| Airflow / chart | 3.3.2 / 1.22.0 | chart gates only at >= 3.0.0 (D1) |
| Provider cncf.kubernetes | 10.22.0 | constraint-tested with 3.3.2 |
| Python (Airflow image / Spark image) | 3.12 / 3.10 | official bases |
| Dev tools | ruff 0.16.10, pytest 9.1.1, pre-commit 4.6.2 | 4.7.0 needs Python ≥ 3.11 (E48) |

Chain: Spark Operator 2.2.1 (spark-submit 3.5.5) → driver Spark 3.5.9 → Iceberg 1.11.0 → HMS 3.1.3 (Hive 2.3 client) → hadoop-aws 3.1.0.

---
# Appendix A2 — How to reproduce

- `cp infra/k8s/secrets.env.example infra/k8s/secrets.env` (passwords, Telegram, GHCR_PULL_TOKEN with read:packages only)
- `make k8s-up` → `make k8s-demo` → `[demo] PASSED`
- `make airflow-ui` → http://localhost:8080 (admin)
- Day 2: `make dag-runs`, `make dag-report`, `make maintenance DRY=yes`, `make backup-now`, drills `make drill-driver|drill-executor|drill-scheduler`
- Tag: task2-v1 · Docs: docs/task2/README.md, 04 (process), 05 (errors), 06 (validation), 07 (drills)

Speaker notes: Stop the Task 1 Compose stack first; both share the 10 GiB WSL VM. `minikube stop` keeps the data; `minikube delete` removes it.
