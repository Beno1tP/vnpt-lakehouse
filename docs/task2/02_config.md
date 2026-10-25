# Task 2 — Config: versions, budgets, values files

## 1. Versions (checked 2026-10-25)

| Component | Pin | Evidence | Note |
|---|---|---|---|
| Minikube | **v1.39.0** (2026-09-02) | GitHub releases API `kubernetes/minikube` latest | Not installed yet |
| Kubernetes (in Minikube) | **v1.37.0** (`--kubernetes-version=v1.37.0`) | minikube v1.39.0 `pkg/minikube/constants/constants.go`: Default = Newest = v1.37.0 | Matches local kubectl **v1.37.1** (skew ±1 OK) |
| kubectl | v1.37.1 | `kubectl version --client` | installed |
| Helm | v3.22.0 | `helm version` | installed |
| Kubeflow Spark Operator chart/app | **2.2.1** (2025-06-27) | tag `v2.2.1` `Dockerfile`: `ARG SPARK_IMAGE=spark:3.5.5`; v2.3.0/v2.4.0 → `spark:4.0.0`; v2.5.2 → `apache/spark:4.0.4` | Last 2.2.x, no further patches. Released when K8s 1.33 was current; 1.37 untested upstream → watch for API errors |
| Spark (jobs) | 3.5.9 (thin image `vnpt-lake-spark:3.5.9-iceberg1.11.0-r1`) | Task 1 | unchanged |
| Hive Metastore | 3.1.3 (thin image `vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1`) | Task 1 | unchanged |
| Airflow | **3.3.2** | PyPI `apache-airflow` latest = 3.3.2 | |
| Airflow Helm chart | **1.22.0** (2026-06-01, appVersion **3.2.2**) | `https://airflow.apache.org/index.yaml`, highest version | **No chart release ships 3.3.x yet.** Decision D1 below |
| Provider `apache-airflow-providers-cncf-kubernetes` | **10.25.0** (PyPI latest, requires `apache-airflow>=2.11.0`) | PyPI JSON | Pin exactly in `docker/airflow/requirements.txt`; check the provider's constraints for 3.3.2 |
| Postgres / MinIO | same digests as Task 1 `.env` | Task 1 | |

Re-check before installing: `helm search repo spark-operator/spark-operator --versions | head`, `helm search repo apache-airflow/airflow --versions | head`.

### Open decisions

| ID | Question | Options | Recommendation |
|---|---|---|---|
| D1 | Chart 1.22.0 defaults to Airflow 3.2.2; we want 3.3.2 | (a) chart 1.22.0 + `airflowVersion/defaultAirflowTag: 3.3.2`; (b) Airflow 3.2.2 | **Decided 10-25: (a).** Chart 1.22.0 templates branch on the Airflow version only at `>=3.0.0` (26×), `<3.0.0` (13×), `<2.11.0` (1×) (`grep semverCompare chart/templates`, tag `helm-chart/1.22.0`), so 3.3.2 renders exactly the same objects as 3.2.2. Remaining risk: config keys deprecated in 3.3 → read warnings at install; fallback (b) |
| D2 | Which days to run / backfill (data: 18 days with rows inside 2024-07-02 → 07-25) | see `03_plan.md` §C 2.5 | **Decided 10-25: option A** — daily runs over the calendar span 2024-07-02 → 07-25 (24 runs): 18 days load their landing file, 6 days without a file are **skipped** (`check_landing` → `AirflowSkipException`). 30 days is UC7 (Task 5, leader: 18 days OK), not a Task 2 requirement. Rejected: B (18 manual triggers), C (one full-file run, no meaningful backfill) |
| D3 | Where to compute per-day expected counts | **Decided 10-25: Spark only, no new tool.** Spark SQL on the Task 1 Bronze tables (Compose stack, already validated: 21,224 / 10,729), grouped by the date in `CREATED_TIME`. Independent of the landing simulator (pandas split). Task 2 stack = Minikube + Airflow + Spark; query engine (Trino) belongs to Task 3 | |

## 2. RAM budget

Minikube runs as a container in Docker Desktop. Docker Desktop and Ubuntu share **one WSL2 VM with 10 GiB** (`.wslconfig` memory=11GB). `progress.md` §2: start at the lab default `--memory=7168 --cpus=4`, raise to `8192 / 6` if stable.

| Pod | Request | Limit | Note |
|---|---|---|---|
| K8s control plane (apiserver, etcd, scheduler, controller-manager, coredns, kubelet) | — | — | **[review]** not in the original budget; roughly 0.7–1 GiB (estimate, measure with `kubectl top pods -n kube-system`) |
| postgres | 256 Mi | 512 Mi | two databases |
| minio | 256 Mi | 512 Mi | |
| hive-metastore | 768 Mi | 1 Gi | `-Xmx768m` |
| spark-operator controller + webhook | 128 Mi + 64 Mi | 512 Mi + 256 Mi | webhook stays on (`01_knowledge.md` A2) |
| Airflow api-server / scheduler / dag-processor / triggerer | 512 / 512 / 256 / 256 Mi | 1 Gi / 1 Gi / 512 Mi / 512 Mi | statsd, redis, pgbouncer, flower **off** |
| Spark driver | 2 Gi | 2 Gi | 1 Gi heap + 1 Gi overhead (pandas) |
| Spark executor ×1 | 1 Gi | 1 Gi | |
| **Sum (without control plane)** | **6,080 Mi ≈ 5.9 GiB requests** | **8,960 Mi ≈ 8.75 GiB limits** | Requests must fit **Allocatable** (`kubectl describe node minikube`) — only meaningful after the E25 fix (`kubelet.system-reserved`, Allocatable 7268 Mi); limits may overcommit, but real use near limits → OOM |

Rules: stop the Task 1 Compose stack while Task 2 runs; `minikube addons enable metrics-server`; watch `kubectl top pods -A` during the first Spark run. Start with `--memory=8192 --cpus=6` only if `docker info` shows ≥ 9.5 GiB total and nothing else heavy runs.

## 3. Disk budget

Minikube (docker driver) stores its node and its own image store **inside Docker Desktop's `docker_data.vhdx`** (now 9.45 GB after the 10-25 compaction; C: 87.2 GB free).

| Item | Estimate | Note |
|---|---|---|
| Minikube node image (kicbase) | ~1.2 GB | estimate |
| Thin Spark + HMS images loaded into Minikube | **4.16 GB** (2.44 + 1.72, measured in Docker Desktop) | **[review]** a second copy: `docker/spark-job` builds `FROM vnpt-lake-spark:…-r1`, so the base must exist in Minikube's store first (`minikube image load`) |
| Airflow 3.3.2 image + thin layer | ~1.5–2 GB | estimate |
| Spark Operator image | ~1 GB | estimate (Spark 3.5.5 inside) |
| Postgres + MinIO | ~0.9 GB | same digests |
| **Total (estimate)** | **≈ +9–10 GB** |
| **Measured after 2.0e** (only node + 2 thin images) | **+10.9 GB**: kicbase 1.89 + node volume 6.76 (containerd ≈ 1.35× image size) + `~/.minikube` 2.3 (1.5 GB image-load tar cache, deletable) | revised total ≈ **+13–14 GB** with Airflow, operator, Postgres, MinIO; C: had 87.2 GB free | measure: `docker system df` (node image + volume) and `minikube ssh -- sudo crictl imagefsinfo` (node runtime is **containerd**, not Docker) |

`minikube image build` puts *new* images straight into Minikube (no copy in Docker Desktop). `minikube delete` frees all of it **and deletes PVC data**. After Task 2: `minikube delete`, `docker builder prune -af`, compact the vhdx again (Task 1 `04_process.md` 1.15).

## 4. Values files (to write at steps 2.2 / 2.3)

- `infra/helm/spark-operator/values-local.yaml`: chart 2.2.1; `spark.jobNamespaces: [spark-jobs]`; `webhook.enable: true`; resources for controller and webhook.
- `infra/helm/airflow/values-local.yaml`: chart 1.22.0; `airflowVersion` / `defaultAirflowTag` (D1); custom image; `executor: LocalExecutor`; `postgresql.enabled: false` + external DB secret; `redis`, `statsd`, `pgbouncer`, `flower` off; resources on every component.
