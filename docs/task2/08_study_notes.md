# Task 1–2 study notes: Bee's 52 questions

Written 2026-11-06 after Bee's research on the whole stack of Tasks 1–2. Format: **global view first**, then one section per stack. Each answer = the concept, **how it shows up in our project** (file, E-number, measured number), and a reference where the claim depends on a version. `[interpreted]` = the question refers to a source Claude has not seen; the answer uses the most likely meaning, correct it if the source meant something else. Q-numbers follow Bee's list; the index at the end maps them to sections.

---

## 0. Global view

### 0.1 The stack in one picture

```
                    GitHub (PR → CI) ──images.yml──▶ GHCR (images by digest)
                                                          │ pull (Secret ghcr-pull)
Windows ▸ WSL2 VM ▸ Docker Engine ▸ [minikube node container] ▸ containerd ▸ pods
                                                          │
  airflow ns     api-server · scheduler(+LocalExecutor workers) · dag-processor · triggerer
                    │ creates SparkApplication (CR) via the K8s API (RBAC: airflow-spark-submitter)
  spark-operator ns controller (reconciles the CR → spark-submit) · webhook (injects env/volumes)
                    │
  spark-jobs ns  driver pod ──K8s API──▶ executor pods     (SA spark-operator-spark)
                    │ Iceberg catalog calls (thrift :9083)      │ data files (S3 API :9000)
  lake ns        hive-metastore ──JDBC──▶ postgres (metastore + airflow DBs)      minio (landing, lakehouse,
                                                                                  airflow-logs, backup)
```

Three layers of control, each fixing what it can and escalating the rest (drill 2): **Airflow** = which day, which step, retry, alert · **Spark Operator / Spark** = one job's driver and executors · **Kubernetes** = place pods, restart containers, keep desired state.

### 0.2 What Bee learned, and where it lives in the project

| Topic | Key idea | In our project |
|---|---|---|
| Iceberg maintenance (4 methods) | expire snapshots, compact data files, rewrite manifests, remove orphans | `jobs/maintenance.py`, DAG `lake_maintenance` (Sunday 02:00 UTC), drill 2.5c: snapshots 50 → 6, files 50 → 22 |
| Small files / shuffle files | many small files = slow planning + reads; shuffle files = Spark's intermediate data on local disk | daily `overwritePartitions` writes ~1 file per partition; compaction weekly; `spark.sql.shuffle.partitions=4` |
| Partitioning | prune files at query time; Iceberg hidden partitioning | Bronze partitioned by `_ingest_date` (`bronze_ingest.py:63`) |
| Schema evolution | add/rename/drop by column ID, no rewrite | Bronze = all strings, so drift shows as new/missing columns: DQ rule "expected column set" |
| CAS + pointer | a commit = atomic compare-and-swap of the table's current-metadata pointer in the catalog | HMS stores `metadata_location` in Postgres; drill 6/7 (lose catalog → restore / `register_table`) |
| Disaster handling | backup, replication, recovery | `pg-backup` CronJob (nightly `pg_dump`), restore drill; gap: backups on the same MinIO |
| Hive: version conflict, catalog config, permissions | HMS 4 broke Spark 3.5's Hive 2.3 client | E16 → HMS 3.1.3; hadoop-aws = HMS's hadoop-common (3.1.0) |
| Spark: Catalyst → codegen; driver/executor pods; batch vs stream; "no spark-submit" | the operator runs spark-submit for you | SparkApplication CR instead of a hand-typed spark-submit |
| Airflow: DAG/Task/Operator/Sensor/Scheduler, architecture, idempotency, backfill | runs are data intervals; reruns = clear | `chatbot_bronze`, E38/E47/E49 |
| Helm chart | packaged, versioned K8s manifests with values | Airflow chart 1.22.0 + Spark Operator chart 2.2.1; our lake = kustomize |
| DAG file rules | parse fast, no top-level work, deterministic | CI `tests/dags/test_dag_integrity.py` (Q14) |
| K8s architecture, CRs vs built-ins, controllers | everything is a desired state reconciled by a controller | SparkApplication CR + operator controller |
| Airflow scaling bottlenecks | metadata DB, DAG parsing, logs, executor choice | LocalExecutor `parallelism: 2` (E52), remote logs (E51) |
| Node allocation | Allocatable = Capacity − kube-reserved − system-reserved − eviction threshold; QoS classes | E25: 11217116Ki → 7268 Mi with `system-reserved=3686Mi` |
| Catalog access patterns ("embedded vs external") | REST catalog can also hand out storage credentials | we use HMS (thrift) + S3 keys in the engine (Q20) |
| SparkKubernetesOperator | creates the CR, follows the driver pod, streams logs, deletes on end | `dags/chatbot_bronze.py:62-70`, RBAC `rbac-airflow.yaml` (Q11, Q27) |
| Volume, PVC, PV, StatefulSet | stable identity + its own disk per replica | `minio-0`, `postgres-0`, PVCs `data-*-0` (Q31) |
| Timetable, data interval, triggerer, start_date | interval = what data a run covers; trigger = when it starts | `CronDataIntervalTimetable` (E38), `start_date` 2024-07-02, `catchup=False` (Q33, Q34) |

### 0.3 Three rules that explain most of Tasks 1–2

1. **Desired state, reconciled.** Kubernetes, the Spark Operator, Helm and Airflow all store *what should be* and loop until reality matches. Debugging = find which loop is stuck (E54: the StatefulSet loop waits for a pod that never becomes Ready).
2. **A run is identified by its data, not by its clock.** Day D = `ds` = logical date; jobs overwrite exactly that partition, so reruns and backfills are safe (E38, E49).
3. **Size from measurement, not from belief.** The kubelet, the JVM and Airflow each have their own idea of memory; requests, limits and heaps must agree (E25, E52, driver 1g + 1g overhead).

---

## 1. Kubernetes, containers and the local cluster

### Q1. How to access a node (`minikube ssh`), the cluster and a container? `--driver=docker` vs a VM in production? Pods in Docker Desktop?
- **Node:** `minikube ssh` opens a shell in the node, which with the docker driver is the `minikube` container (`docker exec -it minikube bash` reaches the same place). Inside, the runtime is **containerd**, so use `sudo crictl ps` / `crictl images`, not `docker` (CLAUDE.md, 04_process 2.0d).
- **Cluster:** you never "enter" it; you talk to the **API server** with `kubectl` using `~/.kube/config` (E53: no context → `localhost:8080`).
- **Container:** `kubectl -n <ns> exec -it <pod> -c <container> -- bash` (Chainguard MinIO has no shell: use `logs` or the console).
- **docker driver vs VM:** the docker driver runs the node as a container on Docker Desktop's VM (fast start, shared kernel, nested containers, separate image store +5.6 GB measured). A VM driver (Hyper-V, KVM) gives the node its own kernel. Production nodes are **VMs or bare metal** managed by the cloud or by kubeadm, one kubelet per machine, many machines.
- **Docker Desktop pods:** with Docker Desktop's built-in Kubernetes (kubeadm mode) pods run directly on Docker's engine, no node container. Our lab uses Minikube because the lab doc requires it and it lets us pin K8s and cap the node (see the advisor discussion, 04_process 10-28).

### Q2. How are a Deployment and a Service "activated"? What do `kubectl get deployments` / `get secrets` show?
- `kubectl apply` → **API server** (authentication → RBAC authorization → admission webhooks) → stored in **etcd**. Nothing runs yet.
- **Deployment controller** creates a ReplicaSet; **ReplicaSet controller** creates Pods; the **scheduler** binds each Pod to a node whose Allocatable fits its *requests*; the **kubelet** asks containerd → runc to start containers.
- **Service:** a stable virtual IP + DNS name (`minio.lake.svc.cluster.local`); the EndpointSlice controller lists ready pods matching the selector; kube-proxy programs the node's iptables so traffic to the IP reaches a pod. StatefulSets use a **headless** service (per-pod DNS).
- `kubectl get deployments` = desired vs ready replicas (`hive-metastore 1/1`). `kubectl get secrets` = names, types, key counts, never values (`minio-credentials-57ttb48fmf Opaque 2`). Values are **base64, not encrypted**; read them only with care (E43, E54).

### Q3 (with Q52). Cloud Kubernetes vs our local approach
| | Minikube (ours) | Managed cloud (EKS/GKE/AKS) |
|---|---|---|
| Control plane | in our node container, 1 copy | run by the provider, HA, upgraded for you |
| Nodes | 1 container, 8 GiB, no autoscaling | node pools of VMs, Cluster Autoscaler/Karpenter, spot nodes |
| Storage | `standard` StorageClass = hostPath in the node | CSI drivers (EBS, PD), snapshots, zones |
| Load balancing | `port-forward` | Service `LoadBalancer`, Ingress/Gateway with real IPs and TLS |
| Identity | ServiceAccount tokens, static S3 keys | workload identity (IRSA, GKE WI): pods get cloud IAM roles, no static keys |
| Cost | free, limited by RAM/disk | pay per node/hour; rightsizing matters |
The **manifests, Helm values and CI carry over**; the platform layer is replaced (README "Design notes").

### Q4. Why Kubernetes no longer uses dockershim
- Old path: kubelet → **dockershim** (adapter inside the kubelet) → Docker Engine → containerd → runc. Docker never implemented the **CRI** (Container Runtime Interface), so Kubernetes maintained this adapter itself.
- Kubernetes **removed dockershim in v1.24** (2022). The kubelet now speaks CRI directly to **containerd** or **CRI-O**: one hop fewer, less code to maintain, same OCI images. Images built with `docker build` still run (they are OCI images).
- Our node: containerd 2.3.4 (04_process 2.0d). Docker is still on the laptop, but only to run the node container.

### Q19. Docker vs Spark vs Iceberg vs Kubernetes "namespace"
| Word | What it isolates |
|---|---|
| **Linux namespaces** (used by Docker/containerd) | kernel views per container: PID, network, mount, user, UTS, IPC. This is what makes a container a container |
| **Kubernetes namespace** | a name scope for API objects + a unit for RBAC, ResourceQuota, LimitRange. **Not** network isolation by default (needs NetworkPolicy; kindnet doesn't enforce it). Ours: lake, airflow, spark-operator, spark-jobs |
| **Spark (catalog) namespace** | a database inside a catalog in Spark SQL: `lake.bronze.chatlog` = catalog `lake`, namespace `bronze`, table `chatlog` |
| **Iceberg namespace** | the same idea in Iceberg's catalog API (can be nested in REST catalogs); in HMS it maps to a Hive database |

### Q21. dockershim (Kubernetes) vs the shim in Docker/containerd
- **dockershim** = an adapter in the kubelet translating CRI calls to the Docker API. Removed (Q4).
- **containerd-shim** (`containerd-shim-runc-v2`) = a small per-container (or per-pod) process that **parents** the container after runc exits: it keeps stdio open, reports the exit code and lets containerd (or Docker) restart without killing running containers. Docker uses it too, because Docker runs on containerd. Same word, different layer.

### Q31. StatefulSet, PVC, PV, StorageClass; native vs host-based storage
```
StatefulSet minio (replicas 1) ──volumeClaimTemplates──▶ PVC data-minio-0 (5Gi, RWO)
     │ stable pod name minio-0                                  │ bound to
     ▼                                                          ▼
 pod minio-0 mounts /data  ◀──────────────────────────── PV pvc-fb07fa81-… (created by the
                                                          provisioner of StorageClass "standard")
```
- **StatefulSet:** stable name (`-0`), ordered start/stop, one PVC per replica that survives pod deletion (E54: pod deleted, PVC kept). **Deployment:** interchangeable pods, shared or no storage.
- **StorageClass** = how to create disks (provisioner + parameters); **PV** = an actual disk; **PVC** = a claim that binds to one PV.
- **Host-based** (minikube `standard` = hostPath under the node; `local` PVs): fast and simple, but the data lives on one node and dies with it (`minikube delete`). **Native/network** (CSI: EBS, Ceph, Longhorn): survives node loss, can snapshot, can move between nodes in a zone. Production stateful services: network storage or, better, a managed database / object store outside the cluster.
- Ours: `infra/k8s/lake/minio.yaml:54-58` (5Gi), `postgres.yaml:55-59` (2Gi).

### Q32. Node scaling
- **Cluster Autoscaler** adds a node when pods are `Pending` for lack of requests capacity, removes underused nodes. **Karpenter** (AWS, now multi-cloud) picks instance types per pending pod, faster bin-packing.
- Spark fits well: executors appear in bursts, then disappear. Use a separate node pool (taints/tolerations) for Spark, spot instances for executors, on-demand for drivers.
- Ours: one fixed node; a second Spark app waits `Pending` (README footprint: 7,236 of 7,268 Mi during bronze).

### Q43. Resource requests vs memory usage; scheduler and node capacity; requests vs limits
- **Request** = what the scheduler reserves; it schedules on *requests vs Allocatable*, **never on real usage**. **Limit** = the cgroup ceiling; above it the container is OOM-killed (memory) or throttled (CPU).
- Usage (`kubectl top`) is a sample every 15–60 s (too slow for 20 s Spark pods, 04_process 10-27). Plan with requests; check with usage over time.
- Our numbers: idle requests 4,036 Mi; bronze run +3,200 Mi (driver 2048 + executor 1152) = 7,236 of 7,268 Mi; limits 103 % = overcommitted (allowed).
- Rule of thumb: request ≈ steady usage (p50–p90), limit ≈ peak + margin; for JVMs request = limit for memory is common (predictable heap).

### Q44. Pod deleted vs container OOMKilled; limiting executors; kube/system-reserved; overcommit
- **OOMKilled:** the kernel kills a container that exceeds its memory limit (`Last State: OOMKilled, exit 137`, E52). The pod object stays; the kubelet restarts the container per `restartPolicy` (ours: Spark `Never` → the driver sees the executor lost and asks for a new one).
- **Pod deleted/evicted:** the whole pod object goes (kubectl delete, node pressure eviction, preemption). A controller creates a replacement (drill 2: Spark replaced the executor).
- **Limiting Spark executors:** `spark.executor.instances`, `spark.executor.memory` + `memoryOverhead` (→ pod requests/limits), dynamic allocation max; plus namespace **ResourceQuota** so Spark can't take the whole node.
- **kube-reserved / system-reserved:** memory kept away from pods for the kubelet/container runtime and the OS. We used `system-reserved=3686Mi` because the kubelet saw the WSL VM's 10.7 GiB, not the 8 GiB cap (E25).
- **Overcommit:** sum of *limits* > Allocatable is allowed; sum of *requests* can't exceed it. Overcommit is safe while not every pod peaks at once.
- "Is a K8s application more than a resource?" `[interpreted]`: a pod's cost = its containers + the kubelet/runtime overhead + sidecars (log groomers in Airflow pods: 32Mi requests each).

### Q45. Stateful vs stateless with HPA/VPA; node sizing; LimitRange/ResourceQuota; utilisation; headroom
- **HPA** scales replica count on metrics; fits stateless services (api-server). **VPA** adjusts requests; fits stateful/single-replica services (Postgres, HMS), usually in recommendation mode, because applying it restarts the pod.
- **Node sizing:** fewer big nodes = better bin-packing for big pods (Spark drivers); more small nodes = smaller failure blast radius. Size nodes so the biggest pod fits with headroom.
- **LimitRange** = default/min/max requests per container in a namespace; **ResourceQuota** = total cap per namespace (enforced by the API server at admission).
- **Utilisation:** compare requests vs usage per namespace (metrics-server now, Prometheus in production); the waste = requested but unused.
- **Headroom strategy:** keep spare Allocatable for the largest expected pod, or low-priority "placeholder" pods that get preempted when real work arrives.

### Q46. Kubernetes OOM vs JVM OOM; container RAM; tmpfs; observation
- **JVM `OutOfMemoryError`:** the Java heap (`-Xmx` = `spark.executor.memory`) is full; the JVM throws, the task fails, the container may live on.
- **Kubernetes OOMKilled:** the whole container's memory (heap + off-heap + Python workers + page cache that can't be reclaimed + tmpfs) exceeded the cgroup limit; the kernel kills it, exit 137.
- Our driver: heap 1g + overhead 1g; pandas lives **outside** the heap, in the overhead (`bronze_sparkapp.yaml:38-39`). Too little overhead → K8s OOM even with a healthy heap.
- **tmpfs** = a RAM-backed filesystem (`emptyDir: {medium: Memory}`, or Spark's `spark.kubernetes.local.dirs.tmpfs=true`): fast spill/shuffle, but every byte written **counts against the memory limit**.
- **Observe:** `kubectl describe pod` (Last State), `kubectl top`, container metrics (Prometheus `container_memory_working_set_bytes`), Spark UI Executors tab (GC time, peak memory).

### Q11. How executors are created; is the API real; driver ↔ API server; ServiceAccount and RoleBinding
- The Kubernetes API **is a real HTTPS REST API**. In cluster mode the driver runs Spark's Kubernetes scheduler backend (fabric8 client), which calls `POST /api/v1/namespaces/spark-jobs/pods` to create executor pods.
- It authenticates with the token of its **ServiceAccount** `spark-operator-spark`, mounted into the pod; the API server checks **RBAC**: a Role in `spark-jobs` allowing pods/services/configmaps, bound to that SA by a **RoleBinding** (created by the operator chart, `values-local.yaml: spark.rbac.create`).
- Executors connect **back to the driver** through a headless Service the driver creates, register, receive tasks.
- Airflow uses the same mechanism: SA `airflow-scheduler` + Role `airflow-spark-submitter` (create/watch SparkApplications, read pods and logs, nothing else: `infra/k8s/spark-jobs/rbac-airflow.yaml`).

---

## 2. Helm, CI/CD and GitOps

### Q5. How to write a standard Helm chart; using it for our own setup; releases and repositories
```
mychart/
  Chart.yaml          name, version (chart, SemVer), appVersion (the app), dependencies
  values.yaml         defaults; values.schema.json validates them
  templates/          *.yaml with Go templates; _helpers.tpl (names, labels); NOTES.txt
  charts/             vendored dependencies
```
- Standard habits: labels `app.kubernetes.io/*`, resources/probes/securityContext configurable, no secrets in values (reference existing Secrets: we do `metadataSecretName`, `jwtSecretName`), `helm lint`, `helm template | kubeconform`, chart-testing (`ct`) in CI.
- **Our own setup** `[interpreted: "model set up" = packaging our lake components]`: `infra/k8s/` could become a chart (`lake/` with values for sizes, images, buckets). We chose **kustomize** because nothing needs templating logic and the secretGenerator fits our file-based secrets (slide 12).
- **Release** = one installed instance (`airflow` in ns `airflow`); **chart version** tracks the package, **appVersion** the software inside. **Repository** = an `index.yaml` over HTTPS or an **OCI registry** (`helm push mychart-0.1.0.tgz oci://ghcr.io/<owner>/charts`). Pinning `--version` + checksum = reproducible installs (E29: chart 1.22.0 + SHA-512).

### Q7. CI/CD with Helm; revisions
- CI: `helm lint`, `helm template` + schema validation, render diff on PR; CD: `helm upgrade --install` from the pipeline, or a GitOps tool (Q29) that renders the chart itself.
- Every `helm upgrade` creates a **revision**, stored as a Secret `sh.helm.release.v1.<release>.v<N>` in the release namespace (values + rendered manifest). `helm history airflow -n airflow` lists them; `helm get values airflow --revision N` shows what was deployed.
- Ours: `make airflow-install` (deploy-guard → clean `main` only, E39), image by digest from `images.env`; CI renders kustomize and checks DAGs, Helm values are checked by deploying.

### Q8. Reusing/resetting values; rollback on a failed upgrade
- `--reuse-values` merges the last release's values with your new `--set`s (can carry stale values forward); `--reset-values` uses only the chart defaults + what you pass; `--reset-then-reuse-values` (Helm ≥ 3.14) = chart defaults, then last values, then yours. Safest: always pass the full values file (`-f values-local.yaml`) like `make airflow-install`.
- Rollback: `helm rollback <release> <revision>`; `--atomic` (Helm 3; called `--rollback-on-failure` in Helm 4) rolls back automatically if the upgrade fails/times out. A rollback does **not** undo database migrations (Airflow), so back up the metadata DB first (Q24).

### Q25. Spark DAG vs Airflow DAG; is Airflow run by Helm; Airflow chart vs Helm chart; Spark Operator chart
- **Spark DAG** = the plan of *one job*: stages split at shuffles, tasks per partition, built by Catalyst/DAGScheduler, lives seconds-minutes inside the driver. **Airflow DAG** = a *workflow* of tasks across systems, scheduled per data interval, lives in Python files and the metadata DB.
- "Helm chart" is the package format; the **Airflow chart** (Apache, 1.22.0) and the **Spark Operator chart** (Kubeflow, 2.2.1) are two charts. Airflow is *installed* by Helm; at runtime Airflow runs itself.
- Spark Operator chart = CRDs (SparkApplication, ScheduledSparkApplication), controller Deployment, webhook, RBAC for job namespaces (`infra/helm/spark-operator/values-local.yaml`).
- Making a chart available = publish to a repo or OCI registry (Q5).

### Q26. DAGs in a private repo; public key to GitHub
- Options: **bake** DAGs into the image (ours, D5: CI builds `vnpt-lake-airflow`, deploy by digest = reviewed code only); **git-sync** sidecar pulling a branch; a shared volume.
- git-sync from a private repo uses an **SSH deploy key**: generate a key pair, add the **public** key to the repo (Settings → Deploy keys, read-only), store the **private** key in a K8s Secret referenced by the chart (`dags.gitSync.sshKeySecret`). Ours doesn't need it: no runtime access to GitHub, only GHCR with a `read:packages` token.

### Q29. Argo CD between Kubernetes and Git
- Argo CD runs **in** the cluster, watches a Git repo (manifests, kustomize, Helm charts), and reconciles the cluster to match: drift is shown and optionally reverted (**self-heal**). Deploy = merge a PR; rollback = revert the commit.
- It would have **prevented E54**: the outside `apply -f` would show as "OutOfSync" and be reverted. Ours today: `make k8s-up` from clean `main` (manual GitOps).

---

## 3. Spark and the Spark Operator

### Q6. A CRD is third-party: how does Kubernetes accept it; SparkApplication, driver, executor; where is the job history
- A **CRD** (CustomResourceDefinition) registers a new API type (`sparkoperator.k8s.io/v1beta2 SparkApplication`) with the API server; from then on `kubectl get sparkapplications` works and objects are stored in etcd like built-ins. A CRD alone does nothing: a **controller** (the operator) watches the objects and acts.
- Flow: SparkApplication → operator controller runs **spark-submit** (its own Spark 3.5.5 client) in cluster mode → **driver pod** (our image, `mainApplicationFile`) → driver creates **executor pods** (Q11) → operator updates the CR status (`SUBMITTED → RUNNING → COMPLETED/FAILED`).
- **History:** Helm stores only installs (Q7). Job history lives in: the CR status until deleted (`timeToLiveSeconds: 86400`, or Airflow's `delete_on_termination`), **Airflow's metadata DB** (task instances, tries), **task logs in `s3://airflow-logs`** (driver log streamed into them, `dag-report`), and optionally Spark **event logs + History Server** (not enabled: gap #8).

### Q10. Reserved vs user vs Spark memory; Tungsten (binary, cache-aware, columnar)
```
executor pod limit = spark.executor.memory (heap, -Xmx) + memoryOverhead (off-heap: JVM, netty, Python workers)
heap = 300 MB reserved + usable
usable × spark.memory.fraction (0.6) = unified memory: execution (shuffles, joins, sorts) ⇄ storage (cache), soft split 0.5
usable × 0.4 = user memory (your objects, UDF data structures)
```
- Ours: executor 768m heap + 384m overhead = 1,152 Mi pod (`bronze_sparkapp.yaml:50-51`); driver 1g + 1g because pandas reads the Excel in overhead.
- **Tungsten:** rows stored in a compact **binary** format (off-heap capable, no Java object per field) → less GC; **cache-aware** sort (keep a key prefix next to the pointer so comparisons hit CPU cache); **whole-stage code generation** fuses operators into one Java function. **Columnar:** Parquet's vectorized reader and Arrow (we enable `spark.sql.execution.arrow.pyspark.enabled` for pandas ↔ Spark).

### Q22. Fault tolerance of Spark and MinIO
- **Spark:** lineage lets lost partitions be recomputed; failed tasks retry (`spark.task.maxFailures` 4); a lost executor is replaced (drill 2). A lost **driver** = job lost → our layer above retries (Airflow, drill 1). Iceberg commits are atomic, so a half-finished write never becomes visible; leftovers = orphan files (removed weekly).
- **MinIO:** in distributed mode, **erasure coding** across drives/nodes survives losing up to half the drives (per erasure set), with bitrot checks; versioning, replication to another site. Ours: **single node, single drive** = no redundancy; the PVC is the only copy (and the backups are on it too: gap #5).

### Q23. Kubeflow history; using the Spark UI
- The operator started at Google (`GoogleCloudPlatform/spark-on-k8s-operator`, ~2017–18), was donated to the **Kubeflow** project (`kubeflow/spark-operator`) and rewritten as v2 on controller-runtime. 2.2.x is the last line on Spark 3.5; 2.3+ moved to Spark 4.0 (CLAUDE.md pins).
- **Spark UI** runs in the driver on port 4040 while the job runs: `kubectl -n spark-jobs port-forward pod/<driver-pod> 4040:4040` → http://localhost:4040 (Jobs, Stages, SQL plan, Executors). After the job ends, the UI is gone unless event logs are written to S3 and read by a **History Server** (on-demand ~0.5 GiB, 03_plan §H #8).

### Q27. Polling and log aggregation: how Airflow knows what happens in Kubernetes
- `SparkKubernetesOperator` (provider 10.22.0) creates the CR, then **polls** its status and the driver pod phase through the API (needs `get/watch` on sparkapplications and pods), and **streams the driver log** (`pods/log`) into the Airflow task log (`get_logs: True`). If the scheduler restarts, a retry **reattaches** to the running driver (E52: duplicated/missing log lines).
- Task logs are shipped to `s3://airflow-logs` (remote logging, E51), so they survive pod restarts; `make dag-report` reads them. Production: central log aggregation (Loki/ELK) for all pods, not only Airflow tasks.

### Q28. Does the Spark Operator depend on a Docker image or on Airflow?
- It needs **an image for its own controller** (pinned `controller:2.2.1@sha256:…`) and **the job image** you name in the SparkApplication (ours: `vnpt-lake-spark-job`, built by CI, contains Spark 3.5.9 + Iceberg jars + `/opt/jobs`). Spark versions must match closely (operator's spark-submit 3.5.5 vs our 3.5.9).
- It does **not** depend on Airflow: any client that creates a SparkApplication works (`make sparkapp`, kubectl, Argo Workflows). Airflow integration = the cncf.kubernetes provider's operator.

### Q30. Batch and streaming: Spark → Iceberg → Airflow
- **Batch** (ours): Airflow starts a bounded job per interval; job ends; Airflow validates and moves on.
- **Streaming:** Spark Structured Streaming runs **continuously** (e.g. Kafka → Iceberg every N seconds/minutes, checkpoint in S3 for exactly-once with Iceberg commits). It is a long-running SparkApplication (restartPolicy `Always`), **not** an Airflow task. Airflow then orchestrates around it: maintenance (compaction is critical with many small streaming commits), downstream batch marts, quality checks per interval.

### Q47. Spill to disk; cleanup; static sizing; cost; executor OOM vs compaction; pros/cons of the 4 maintenance methods
- **Spill:** when execution memory is full, sorts/aggregations write sorted runs to local disk (`spark.local.dir` = emptyDir) and merge them later. Slower, but it is how Spark *avoids* OOM. Watch "Spill (memory/disk)" in the Spark UI.
- **Cleanup of previous runs:** the operator deletes the CR + driver after `timeToLiveSeconds`; Airflow deletes on termination; emptyDirs go with the pod; failed writes leave orphan files → `remove_orphan_files` (72 h guard, E46).
- **Static executor sizing** fails when data grows (OOM/spill) or wastes nodes when it shrinks. On K8s, dynamic allocation needs `spark.dynamicAllocation.shuffleTracking.enabled=true` (no external shuffle service).
- **"Cut K8s costs by 80 %"** `[interpreted]`: such claims usually combine right-sizing requests, autoscaling to zero, spot nodes for executors, and bin-packing. The number depends on how wasteful the start was; measure requests vs usage first (Q45).
- **Executor OOM vs compaction:** `rewrite_data_files` reads and rewrites many files in one job; bound it with `max-file-group-size-bytes`, `partial-progress.enabled`, per-partition filters, and run it when nothing else runs (Sunday 02:00 after the 00:00 load).

| Method | Pros | Cons |
|---|---|---|
| Expire snapshots | metadata and storage shrink; GDPR deletes become physical | time travel/rollback beyond the window is lost (proved in drill 2.5c) |
| Compaction (`rewrite_data_files`) | fewer, bigger files → faster scans and planning | CPU/IO cost, conflicts with concurrent writers, temporary double storage until expiry |
| Rewrite manifests | faster query planning (fewer, better-clustered manifests) | small benefit on small tables; another commit |
| Remove orphans | reclaims storage from failed writes | dangerous if the age threshold is shorter than running writes (Iceberg refuses < 24 h by default, E46) |

### Q49. Broadcast join vs sort-merge join
- **Broadcast hash join:** the small side is collected to the driver and sent to every executor; each executor joins locally, **no shuffle** of the big side. Chosen when one side < `spark.sql.autoBroadcastJoinThreshold` (10 MB default) or with a hint. Risk: broadcasting something too big → driver/executor OOM.
- **Sort-merge join:** both sides are **shuffled** by the join key and sorted, then merged. Scales to any size, costs shuffle + sort (spill possible).
- AQE (on by default in Spark 3.x) can switch a sort-merge join to broadcast at runtime when a side turns out small. Our Silver join chat ↔ predict (~21k × 11k rows) will broadcast.

---

## 4. Airflow

### Q12. Jinja templating and the DAG definition file
- The DAG file is Python that **declares** tasks; it is parsed every few seconds by the dag-processor, so it must not do work at import time.
- **Templated fields** are rendered with Jinja **at run time**, per task instance, from the context (`{{ ds }}`, `{{ data_interval_start }}`, `{{ var.value.x }}`, `{{ params.x }}`). Ours: the SparkApplication YAML is a template (`application_file`, `.yaml` in `template_ext`), so `{{ ds }}` becomes `--ingest-date 2024-07-24`.
- Airflow 3 renders with **StrictUndefined**: a missing variable is an error, not an empty string (E47). CI renders every template with and without a date.

### Q14. "15 rules of DAG production" `[interpreted: Bee's list not seen; this is the common set, with our status]`
| # | Rule | Ours |
|---|---|---|
| 1 | No heavy work / network calls at top level (parse speed) | ✅ DAG integrity test imports in CI |
| 2 | Idempotent tasks: same interval → same result | ✅ `overwritePartitions` per day |
| 3 | Use the data interval, never `now()` | ✅ `ds` / `CronDataIntervalTimetable` (E38) |
| 4 | Static, timezone-aware `start_date` | ✅ 2024-07-02 UTC, checked in CI |
| 5 | `catchup` decided on purpose | ✅ False + explicit backfill |
| 6 | Retries with delay, sized per failure type | ✅ bronze 2×5 min, validate ≤ 1 (E41) |
| 7 | `execution_timeout` on every task, `dagrun_timeout` | ✅ 20 min / 3 h |
| 8 | Alert on failure + SLA replacement (Deadline) | ✅ Telegram + 2 h DeadlineAlert |
| 9 | Heavy compute outside Airflow workers | ✅ Spark pods |
| 10 | No secrets in code; Connections/Secrets backend | ✅ env from K8s Secrets |
| 11 | Small XComs only (metadata, not data) | ✅ none used for data |
| 12 | `max_active_runs` / pools to protect shared targets | ✅ 1 run at a time |
| 13 | Tags, owners, docs | ✅ tags; `doc_md` partly |
| 14 | Tests: import, structure, template render | ✅ CI DAG integrity |
| 15 | Data checks inside the DAG (fail the run) | ✅ validate, 23 checks |

### Q24. Clean up and back up an existing Airflow instance; DAG authors; deployment managers
- Before an **upgrade**: back up the metadata DB (migrations are one-way; Helm rollback won't undo them), and **`airflow db clean`** old rows (task instances, logs, XComs) so migrations run fast and the DB stays small (a scaling bottleneck).
- Airflow's security model names roles: **Deployment Managers** (install/upgrade/configure Airflow, own the infrastructure: here, Bee via Helm), **DAG Authors** (write DAG code, which runs with the workers' permissions: so their code must be reviewed), **UI users** (operate runs). "Instance upgraded by a deployment manager" = the deployment manager runs the chart upgrade, which triggers the DB **migration job** (`run-airflow-migrations`, seen on 10-28).

### Q33. DataIntervalTimetable; why `catchup=False`; batch ETL and history; backfill and clear
- A **data-interval** timetable gives each run `[start, end)`; the run starts after `end`. `CronDataIntervalTimetable("0 0 * * *")` = daily `[D, D+1)`, logical date D.
- **catchup=True** would create a run for every missed interval since `start_date` (2024-07-02 → today = 800+ runs, mostly empty). **False** = only the latest; history is loaded on purpose with **backfill** (`make backfill FROM TO REPROCESS=none|completed|failed`, Airflow 3 `airflow backfill create`).
- **Clear** = rerun an existing run (same run ID, try +1): `airflow tasks clear chatbot_bronze --start-date D --end-date D --yes` (E49). Backfill creates runs; clear reruns them.

### Q34. `current_date`; why trigger vs interval matters
- Using `current_date()` / `now()` in a job makes it **non-idempotent**: a rerun next week processes a different day. Always use the interval passed by Airflow (`--ingest-date {{ ds }}`).
- **Trigger** timetables (`CronTriggerTimetable`, `DeltaTriggerTimetable`) give a point in time with **zero-length** interval: fine for "send a report at 8:00", wrong for "load yesterday's data". E38 was exactly this: `ds` = the run day, not the data day.

### Q35. dag-processor and triggerer in Airflow 3; what to separate; SubDAGs, TaskGroups, Assets
- **dag-processor** parses DAG files and stores serialized DAGs in the DB (a separate, mandatory component in Airflow 3: DAG code no longer runs in the scheduler). **Scheduler** reads serialized DAGs, creates runs, decides which tasks are ready, sends them to the executor.
- **Triggerer** runs **deferred** waits (async triggers) for many tasks in one process, so a sensor doesn't hold a worker slot.
- Separate: parsing (untrusted DAG code) from scheduling (critical loop) from execution (workers) from waiting (triggerer) from UI/API. Each scales and fails independently.
- **SubDAGs** were removed (they ran a whole DAG inside a task: deadlocks, slots wasted). **TaskGroups** = visual grouping only, same DAG. **Assets** (formerly Datasets) = data-aware scheduling: DAG B runs when DAG A updates `asset("s3://lakehouse/bronze/chatlog")`. That's the clean way to trigger Silver/Gold on new Bronze (option A, Task 3).

### Q36. "JWT problem for memory parallelism" `[interpreted]`
- In Airflow 3, workers no longer write to the metadata DB directly; each task talks to the **Task Execution API** on the api-server with a short-lived **JWT**. All components must share the same signing secret (our Secret `airflow-jwt-secret`); a mismatch → tasks fail with 401/403 after an upgrade or secret change.
- Link to memory/parallelism: each LocalExecutor worker process holds an API client + token (~260 MiB RSS per worker, E52), so `parallelism` multiplies memory; long tasks rely on token refresh. If your source meant something else, send it and I'll update.

### Q37. Scheduler heartbeat and the deferrable path; is it true; why split roles
- **True.** The scheduler detects dead tasks by missed **heartbeats** (in Airflow 3 the task supervisor heartbeats through the API) and fails/retries them. A **deferrable** operator runs on a worker, hits a wait, **defers** (frees the slot), the **triggerer** waits asynchronously, then the scheduler re-queues the task on a worker to finish.
- Advantage: thousands of waits cost one asyncio process instead of thousands of worker slots; scheduler stays light; failures are isolated. Ours: `SparkKubernetesOperator` runs non-deferred (it holds a LocalExecutor slot while Spark runs; fine at parallelism 2). Deadline callbacks (`AsyncCallback`) are async work of the same kind.

### Q40. LocalExecutor parallelism: where is it in a pod?
- LocalExecutor runs tasks as **child processes of the scheduler**, inside the **scheduler container**. `parallelism` = number of worker processes kept alive. Measured: 4 × ~263 MiB + scheduler 194 + serve-logs 152 → OOM at 1 Gi (E52) → `parallelism: 2`, limit 1,536 Mi.
- Consequence: a scheduler restart kills running tasks (E41 → 120 s grace + validate retry). KubernetesExecutor would put each task in its own pod.

### Q41. Upgrading Airflow 2 → 3: old code, chart, security, migration, automatic checks
- **Code:** run **ruff's `AIR3xx` rules** (`ruff check --select AIR301,AIR302,AIR311,AIR312 dags/`): flags removed/moved imports and parameters (`schedule_interval` → `schedule`, `airflow.operators.*` → providers/`airflow.sdk`, SubDAGs, `execution_date` → `logical_date`, direct DB access from tasks).
- **Config:** `airflow config lint` / `airflow config update` for renamed/removed options.
- **Chart/infra:** new components (dag-processor mandatory, api-server replaces webserver), JWT secret, Execution API.
- **DB:** back up, `airflow db clean`, then `airflow db migrate` (the chart's migration job).
- **Behaviour changes** we hit: timedelta/cron schedules (E38), runs without a logical date (E47), strict templates, one run per logical date (E49). Automatic checks can't find these: a dry run of every trigger path can.

### Q42. Scaling and executors; Airflow scaling bottlenecks
| Executor | Where tasks run | Fits |
|---|---|---|
| Local | processes in the scheduler pod | small, one scheduler (ours) |
| Celery | long-lived worker pods fed by a broker (Redis/RabbitMQ) | many short tasks, low start latency |
| Kubernetes | one pod per task | isolation, per-task resources, bursty loads; pod start latency |
| Multiple (Airflow ≥ 2.10) | per task choice | mix of the above |
Bottlenecks in order of pain: **metadata DB** (connections → PgBouncer, row counts → `db clean`), **DAG parsing** (top-level code, many files → more dag-processors), **scheduler loop** (HA schedulers), **logs** (remote storage), **executor capacity**.

### Q9. Drills on a data pipeline
- A **drill** = break something on purpose and verify detection, recovery and data correctness. Ours (07_drills): kill the driver, delete the executor, restart the scheduler, backfill twice, forced failure + deadline alert, lose the catalog DB, re-register from `metadata.json`, maintenance (README "Failure drills").
- Good drills check **three things**: the alert arrives (once), the system recovers without manual data fixes, the numbers afterwards are exact (23/23).

---

## 5. Idempotency, auditability, data operations

### Q38. Process-level vs write-level idempotency; overwrite, MERGE, windowed; deleted output but a successful run
- `[interpreted]` **Process-level** = running the *task* twice is harmless (no duplicate side effects: alerts, API calls). **Write-level** = the *data written* is the same after one or many runs.
- **Partition overwrite** (ours): replace partition D entirely → write-level idempotent for append-style daily data. **MERGE/upsert**: match on a key, update or insert → idempotent if the key is stable and the source is deterministic. **Windowed**: recompute a sliding window (e.g. last 3 days) to catch late data, each run overwriting the window.
- If someone **deletes the output partition** after a successful run, **Airflow does not notice**: it records *task state*, not data. Detection needs data checks (rerun `validate`, a reconciliation DAG, or Asset checks). Repair = clear the run (idempotent job rewrites it).

### Q39. Data audit and the facets of idempotency
- **Audit:** who/what/when for every row: our Bronze has `_ingest_date`, `_ingested_at`, `_source_file`, `_batch_id`; Iceberg snapshots record each commit (operation, summary, timestamp); Airflow keeps runs/tries; logs in S3.
- `[interpreted]` facets: **re-run** (same input → same output), **scope** (a run touches only its interval's partition), **isolation + throttle** (`max_active_runs=1`, pools: concurrent runs don't collide), **seam + reconcile** (check counts at the boundary between systems: rows read = rows written, Bronze = landing), **resumability** (a failed run restarts from its first unfinished task: clear the failed task only).

---

## 6. Iceberg and catalogs

### Q13. Which files change on an update; COW vs MOR; why metadata is easier than data; schema evolution
```
catalog pointer ──▶ metadata.json (schemas, partition specs, snapshots, current-snapshot-id)
                        └─▶ manifest list (one per snapshot) ──▶ manifests ──▶ data files (+ delete files)
```
- A commit writes **new** data files, new manifests, a new manifest list, a new `metadata.json`, then **CAS-swaps the pointer** in the catalog. Old files are never edited (immutable), which is why time travel works.
- **COW (copy-on-write):** an update rewrites whole data files containing changed rows → slow writes, fast reads. **MOR (merge-on-read):** write small **delete files** (position/equality) next to the data → fast writes, reads merge on the fly until compaction. Choose COW for rare batch updates (ours: overwrite partitions), MOR for frequent upserts/CDC.
- **Metadata is easier to change than data:** a schema change is a new `metadata.json` (KB), not a rewrite of TB of Parquet.
- **Schema evolution:** columns have **IDs**, so add/drop/rename/reorder and safe type widening (int → long, float → double, decimal precision up) don't rewrite data; files written before a column existed read it as null. Bronze is all-strings, so Silver will own the typing.

### Q15. Snapshots for time travel; challenges
- `SELECT … FROM lake.bronze.chatlog VERSION AS OF <snapshot_id>` or `TIMESTAMP AS OF '…'`; `lake.bronze.chatlog.snapshots` / `.history` list them; `CALL lake.system.rollback_to_snapshot(…)`.
- Uses: audit "what did the table look like when the report ran", debugging a bad load, rollback.
- Challenges: only within **retention** (expired snapshots are gone: drill 2.5c proved the old time travel fails); storage cost of keeping old files; time travel reads the old **schema** at that snapshot; GDPR deletes are not physical until expiry.

### Q16. Why the catalog gives transactional guarantees, and its scale/partitioning/operational problems
- Iceberg's ACID rests on **one atomic operation**: swap the table's metadata pointer only if it still points to the version you read (**compare-and-swap**). The catalog provides it (HMS: a lock + update in its DB; REST: a server-side commit; JDBC: a conditional UPDATE). Concurrent writers: the loser retries on the new version.
- **Scale/performance:** HMS was built for Hive's partition-per-row model; Iceberg keeps partitions in its own metadata, so HMS stores just a pointer, but every commit still hits one Thrift service + one DB → a bottleneck at high commit rates, plus HMS locks.
- **Operational complexity:** one more stateful service + DB to run, back up and secure (E16, E27, drill 6). That is why many teams move to REST catalogs or managed ones.

### Q17. Hadoop vs Hive catalog, and REST, JDBC, Nessie, Polaris, Lakekeeper, Gravitino, Snowflake
| Catalog | How it commits | Pros | Cons / when |
|---|---|---|---|
| **Hadoop** (file-system) | atomic rename of `metadata.json` | no service | unsafe on S3 (no atomic rename); single engine, tests only |
| **Hive (HMS)** — ours | lock + DB update via Thrift | everywhere supported (Spark, Trino, StarRocks) | old, Thrift, version coupling (E16), no fine-grained auth |
| **JDBC** | conditional UPDATE in a SQL DB | simple, no extra service | each engine needs DB credentials; few features |
| **REST** (spec) | HTTP API; server owns commits | engine-neutral, credential vending, auth | needs a server implementation (below) |
| **Nessie** | Git-like commits across tables | branches/tags for the **whole catalog**, multi-table transactions | another service; REST support added later |
| **Apache Polaris** | REST implementation | open governance, RBAC, credential vending; Snowflake Open Catalog is managed Polaris | young project |
| **Lakekeeper** | REST implementation (Rust) | lightweight, OIDC/authz, single binary | smaller ecosystem |
| **Apache Gravitino** | metadata federation over many sources (Iceberg REST, Hive, JDBC DBs, files) | one catalog of catalogs | broader scope = more to run |
| **Snowflake** (Horizon / Open Catalog) | Snowflake-managed | managed, integrated with Snowflake | vendor coupling; external engines get read or REST access depending on the product |
Criteria: engines that must read/write it, auth and multi-tenant needs, transactions across tables, operations you can afford, vendor lock-in. For our lab, HMS was chosen for engine compatibility (Trino 483, StarRocks 3.5 support it); REST is the forward-looking option (Q52).

### Q20. "Embedded vs external" approach `[interpreted]`
- What we run (Bee's "external/old"): the engine gets **two** things separately: table metadata from **HMS** (thrift) and **its own static S3 keys** to read/write files (`s3-credentials`). Every engine must hold storage keys.
- Newer pattern (Bee's "embedded/new"): the engine asks a **REST catalog** for the table; the catalog returns the metadata **and temporary, scoped storage credentials** (credential vending). One service governs both catalog and storage access; engines never hold long-lived keys.
- Trade-off: much better security/governance; needs a REST catalog (Polaris, Lakekeeper, Gravitino) and engine support.

### Q48. Automating Iceberg maintenance: manual, CronJob, event-driven, AI/autonomous
| Way | Pros | Cons |
|---|---|---|
| Manual (`make maintenance`) | full control, good for learning | forgotten; doesn't scale |
| Scheduled (ours: Airflow weekly DAG; or a K8s CronJob) | predictable, alerts and logs in Airflow | runs even when not needed; fixed window |
| Event-driven (after N commits / file-count threshold / Asset update) | runs when needed, good for streaming tables | more logic; risk of running during peak writes |
| Managed/autonomous (e.g. AWS S3 Tables, catalog-side optimizers, ML-driven) | no ops, adapts | vendor-specific, less control, cost |
Compaction techniques: **bin-pack** (merge small files, cheap), **sort** (cluster by a column for better pruning), **z-order** (multi-column clustering; costly). Ours: bin-pack where a partition has ≥ 2 files (`maintenance.py:81`).

### Q50. Tags and branches on snapshots; recovery window
- **Tag** = a named snapshot with its own retention (`ALTER TABLE … CREATE TAG eom_2024_07 RETAIN 365 DAYS`): keeps a month-end state even after normal expiry. **Branch** = an independent line of snapshots (`CREATE BRANCH audit`): write there, validate, then fast-forward `main` → **Write-Audit-Publish** (gap #7).
- **Recovery window** = how far back you can time-travel/rollback = snapshot retention (ours: 7 days, keep at least 5) + tags. Longer window = more storage. Disaster recovery beyond it = backups/replication of files + catalog (gap #5).

---

## 7. Query engines and integration

### Q18. Spark vs Trino for querying
| | Spark SQL | Trino |
|---|---|---|
| Model | batch engine; job = driver + executors started per job | always-on cluster (coordinator + workers), MPP |
| Strength | heavy ETL, writes, Python/ML, long jobs with fault tolerance (stage retry) | interactive SQL, low latency, many concurrent users, federation (one query across Iceberg + Postgres + …) |
| Fault tolerance | recomputes lost partitions | a failed worker fails the query (unless fault-tolerant execution with exchange spooling is on) |
| In our lab | writes Bronze/Silver, maintenance | Task 3: reads Bronze/Silver for analysts and dbt |

### Q51. Why `Airflow → Trino → {Postgres, Iceberg} → MinIO`, `Trino → HMS → MySQL/Postgres`, `Airflow → dbt`?
```
Airflow ──▶ dbt ──(SQL)──▶ Trino ──▶ Iceberg connector ──▶ HMS ──▶ metastore DB (Postgres here; MySQL is common)
   │                         │            └──────────────▶ MinIO (data files, S3 API)
   └──(orchestrates)         └──▶ PostgreSQL connector ──▶ an operational DB
```
- **Trino as the SQL layer:** one SQL endpoint over many sources (**federation**): join an Iceberg table with a Postgres reference table without copying it first → data integration and exploration.
- **HMS** is only the catalog (pointers); its own DB can be Postgres (ours) or MySQL.
- **dbt** compiles SQL models and runs them *through* Trino (dbt-trino) or StarRocks; **Airflow** schedules dbt after Silver (`ingest_to_iceberg >> dbt_run_gold >> dbt_test_gold` in the lab doc).
- Use: integrate sources, build Gold with SQL, serve BI. Caveat: federation pushes load onto operational DBs; for heavy use, ingest them into Iceberg instead.

### Q52. Should we change the strategy in the future?
**Yes, for production:** move the platform layer: managed K8s with autoscaling, KubernetesExecutor, HA Postgres/S3 outside the cluster, a REST catalog with credential vending, GitOps (Argo CD), off-cluster backups, Prometheus/Grafana, Parquet/JDBC/CDC ingestion instead of Excel on the driver.
**No, not for the lab now:** the lab doc fixes Minikube + HMS-compatible engines; Task 2 is verified on it; changing the platform mid-lab costs re-verification and teaches nothing new for Tasks 3–5. The code, DAGs, CI and idempotency rules already carry over unchanged.
**Decide per item with evidence:** the first change worth making in the lab is event-driven runs with Assets (Task 3, option A); the first in production is the ingestion bottleneck.

---

## Index: question → section

| Q | Section | Q | Section | Q | Section |
|---|---|---|---|---|---|
| 1 | 1 | 19 | 1 | 37 | 4 |
| 2 | 1 | 20 | 6 | 38 | 5 |
| 3 | 1 | 21 | 1 | 39 | 5 |
| 4 | 1 | 22 | 3 | 40 | 4 |
| 5 | 2 | 23 | 3 | 41 | 4 |
| 6 | 3 | 24 | 4 | 42 | 4 |
| 7 | 2 | 25 | 2 | 43 | 1 |
| 8 | 2 | 26 | 2 | 44 | 1 |
| 9 | 4 | 27 | 3 | 45 | 1 |
| 10 | 3 | 28 | 3 | 46 | 1 |
| 11 | 1 | 29 | 2 | 47 | 3 |
| 12 | 4 | 30 | 3 | 48 | 6 |
| 13 | 6 | 31 | 1 | 49 | 3 |
| 14 | 4 | 32 | 1 | 50 | 6 |
| 15 | 6 | 33 | 4 | 51 | 7 |
| 16 | 6 | 34 | 4 | 52 | 7 |
| 17 | 6 | 35 | 4 | | |
| 18 | 7 | 36 | 4 | | |

`[interpreted]` items to confirm with Bee's sources: Q5 ("model set up"), Q14 (15 rules), Q20 (embedded/external), Q36 (JWT), Q38–Q39 (facet names), Q44 (last part), Q47 (80 % cost claim), and "Repartition Controllers" in the learned list (read as reconciliation controllers, §0.3 rule 1).
