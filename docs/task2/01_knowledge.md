# Task 2 — Knowledge: Spark on Minikube, orchestrated by Airflow

Goal: run the Task 1 Bronze job **on Kubernetes** (Spark Operator), triggered and retried by **Airflow**, with one run per day of data, so that a backfill and a rerun give the same tables.

Inherited from Task 1: the thin Spark image (`vnpt-lake-spark:3.5.9-iceberg1.11.0-r1`), the thin HMS image, `bronze_ingest.py`, the validator (17 checks), and the rule "dependencies in images, config outside, credentials in files, data in volumes".

Source: plan written in Claude desktop (2026-10-25), reviewed in Claude Code the same day; corrections are marked **[review]**. Versions: `02_config.md`.

---

## A1. Kubernetes objects you will touch

| Object | What it is | Used for in Task 2 |
|---|---|---|
| Namespace | A named group of objects (names, quotas, RBAC are scoped to it) | `lake`, `spark-operator`, `spark-jobs`, `airflow` |
| Pod | One or more containers sharing network and volumes; the unit K8s schedules | Every running process |
| Deployment | Keeps N identical, stateless pods running; rolling updates | HMS, Spark Operator, Airflow components |
| StatefulSet | Like a Deployment, but stable pod names and one volume per pod | Postgres, MinIO |
| Service | Stable DNS name + virtual IP in front of pods (`<svc>.<ns>.svc.cluster.local`) | `minio.lake:9000`, `hive-metastore.lake:9083`, `postgres.lake:5432` |
| PersistentVolumeClaim | A request for storage; survives pod restarts | MinIO data, Postgres data |
| ConfigMap | Non-secret config as key/values or files | `spark-defaults.conf`, `core-site.xml` |
| Secret | Credentials (base64-encoded, **not encrypted** by default) | MinIO keys, Postgres password, `hive-site.xml` with the JDBC password, mounted as files |
| Job | Run a pod to completion | One-shots: create buckets, HMS schema init (= Compose one-shots / initContainers) |
| ServiceAccount + Role + RoleBinding | Pod identity + what it may do via the API (RBAC) | Spark driver creates executor pods; Airflow creates SparkApplications |
| CustomResourceDefinition (CRD) | Adds a new object type to the API | `SparkApplication` |
| requests / limits | Request = reserved for scheduling; limit = hard cap (memory over limit → OOMKilled) | Every pod; JVM `-Xmx` ≈ 70–75 % of the limit |
| Mutating admission webhook | An HTTPS service the API server calls to change an object before it is stored | Spark Operator's webhook adds volumes, env, ConfigMaps to driver/executor pods (A2) |

**Operator pattern:** a controller watches a custom object (the *desired state*) and keeps acting until the real state matches it (the *reconcile loop*). The Spark Operator watches `SparkApplication` objects and runs `spark-submit` for each one.

**Helm:** a package manager for K8s. A *chart* = templates + default `values.yaml`; `helm install -f values-local.yaml` renders and applies them as a *release*; `helm upgrade` / `helm rollback` manage versions of that release.

## A2. Spark on Kubernetes

| Concept | Meaning |
|---|---|
| Cluster mode | `spark-submit --master k8s://…` creates a **driver pod**; the driver asks the K8s API for **executor pods**; when the job ends, executors are deleted and the driver pod stays `Completed` (its logs remain) |
| Driver ServiceAccount | The driver needs RBAC to create/delete pods, services and configmaps in its namespace |
| Memory | Pod memory = `spark.driver.memory` + `memoryOverhead` (default max(384 MiB, 10 %)). **PySpark + pandas live in the overhead part**, so the driver needs extra overhead for the Excel parsing |
| Images | Driver and executors use the same image: our thin Spark image (Spark's own K8s entrypoint is in `apache/spark`) |
| Spark Operator | Turns a `SparkApplication` YAML into a spark-submit; tracks state `SUBMITTED → RUNNING → COMPLETED / FAILED`; optional `restartPolicy` |
| **[review] Operator webhook** | In Kubeflow Spark Operator 2.2.1 the **mutating webhook** is what applies `volumes`/`volumeMounts`, `env`/`envFrom`, `sparkConfigMap`, `hadoopConfigMap`, memory limits, tolerations, node selectors to the driver/executor pods (`internal/webhook/sparkpod_defaulter.go`: `addVolumes`, `addEnvFrom`, `addSparkConfigMap`, `addMemoryLimit`, …). We need ConfigMap + Secret mounts, so **keep the webhook on** (chart default `webhook.enable: true`) |

Version note (verified 2026-10-25 from each tag's `Dockerfile`): Kubeflow Spark Operator **v2.2.1 builds on `spark:3.5.5`**; **v2.3.0 and v2.4.0 on `spark:4.0.0`**; **v2.5.2 on `apache/spark:4.0.4`**. The operator runs its own `spark-submit` to launch our 3.5.9 driver, so use **2.2.1** (the last 2.2.x) to avoid 4.0 → 3.5 submission skew. Trade-off: 2.2.1 is from 2025-06-27 and gets no more patches. (Alternative to know: the ASF's own `apache/spark-kubernetes-operator`, 0.x, newer and less widespread.)

## A3. Airflow 3

| Component | Role |
|---|---|
| API server | UI + REST API + **Task Execution API** (in Airflow 3, tasks talk to this API, not directly to the metadata DB) |
| Scheduler | Creates DAG runs and task instances, hands them to the executor |
| DAG processor | Parses DAG files (separate process in Airflow 3) |
| Triggerer | Runs deferred (async) waits, so sensors don't hold a worker slot |
| Metadata DB | Postgres: runs, task states, connections, variables |
| Executor | **LocalExecutor**: tasks run as processes inside the scheduler pod. Enough here, because the heavy work runs in Spark pods; the Airflow task only creates a `SparkApplication` and waits |

| DAG concept | Rule for this task |
|---|---|
| Logical date / data interval | Each run processes **one day** `[data_interval_start, data_interval_end)`; pass it to the job, never `now()` |
| `catchup` | Default `False` in Airflow 3; use **backfill** (`airflow backfill create --dag-id … --from-date … --to-date …`) to fill history on purpose |
| Retries | `retries=2`, `retry_delay=5m` for the load (transient: pod eviction, HMS restart); **`retries=0` for validation** (a wrong number is deterministic) |
| `max_active_runs=1` | Runs write the same tables, so one at a time |
| `execution_timeout` | Always set, so a hung run frees its slot |
| Idempotency | Safe retries and backfills **only** because the job overwrites its own partition (Task 1) |

**Airflow 3 gotcha (verify in your version's docs):** with a cron string like `"@daily"`, Airflow 3 uses `CronTriggerTimetable` by default (`[scheduler] create_cron_data_intervals = False`), so `data_interval_start == data_interval_end`. For a real one-day interval, use `schedule=timedelta(days=1)` or `CronDataIntervalTimetable("@daily", timezone="UTC")`. Manual runs may also have no logical date: give the DAG a `date` param as a fallback.

**[review] Which "day":** `CREATED_TIME` is Oracle text without a time zone. The data day = the date **as written** in `CREATED_TIME` (no conversion). The DAG passes `ds` (YYYY-MM-DD of the interval start) as a label for that day, so the DAG time zone and the landing `dt=` must use the same convention. Data covers 2024-07-02 → 2024-07-25 with **18 distinct days** (`docs/task1/06_validation.md`): 6 days in that range have no file (A5).

## A4. How Airflow starts Spark: options

| Option | How | Verdict |
|---|---|---|
| **SparkKubernetesOperator** (provider `cncf.kubernetes`) | Creates a `SparkApplication` from a templated YAML, waits, streams driver logs | **Chosen**: declarative, the operator owns Spark's lifecycle |
| KubernetesPodOperator + `spark-submit` | Airflow starts a pod that runs spark-submit in cluster mode | Works without the Spark Operator; more YAML for you |
| SparkSubmitOperator | spark-submit from the Airflow worker | Needs Spark inside the Airflow image: heavy, couples versions |
| BashOperator + `kubectl apply` | Shell out | No state tracking: avoid |

**One retry layer only:** let Airflow retry, and keep the SparkApplication `restartPolicy: Never`. Retrying at both levels multiplies attempts (3 × 3 = 9).

**[review] Verify at step 2.4:** how the pinned provider names the SparkApplication on a retry (random suffix vs fixed name; does it delete the old object first?). A fixed name `bronze-<date>` that still exists makes the retry fail with `AlreadyExists`. Names must be lowercase DNS labels (≤ 63 chars).

## A5. Where the Excel input comes from on K8s

Task 1 bind-mounted a laptop folder. In K8s, the standard is a **landing zone in object storage**: `s3://landing/chatlog/dt=YYYY-MM-DD/…xlsx`. The job reads its day's file with `pyarrow.fs.S3FileSystem` (pyarrow is already in the image, so no new dependency) and hands the bytes to pandas.

To make daily runs and backfill meaningful, a one-off **landing simulator** script splits each Excel file by event day (`CREATED_TIME`) into daily files, playing the role of the source system that drops one export per day. Data stays inside MinIO on the laptop.

**[review] Semantics to fix before coding:**
- Bronze `_ingest_date` = the run's day = the landing `dt=` (in Task 1 it was one load date for the whole file).
- `bot_predict` rows are split by **their own** `CREATED_TIME`; the 116 predict rows without a chat row stay in their day. Per-day expected counts must be computed the same way.
- Days without data (6 of the 24 calendar days 07-02 → 07-25; decision D2 open): decide whether `check_landing` **skips** the run (`AirflowSkipException`, downstream skipped) or the simulator writes an empty file and the run loads 0 rows. Recommendation: skip — an empty export is not the same as a missing export.
