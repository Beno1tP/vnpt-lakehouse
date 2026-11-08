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

Agenda: Knowledge (Kubernetes, Minikube, operators, Spark on K8s, Airflow 3, Helm, CI/CD) · Kubernetes in depth (K1–K18, with a chatbot-platform use case) · Stack of this task · Running it (intervals, backfill, drills, backup, maintenance, sizing) · Errors · Deep dive: 52 questions from the stack review, one global view + per-stack answers

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

Speaker notes: No image is built on the laptop: that saves CPU, disk and a second copy of every image, and it is the production flow. Every change in Task 2 went through this loop (PRs #1–#43).

---
# Part 1b — Kubernetes in depth

Numbered K1–K18 so slides 14–69 keep their numbers. Structure follows the Viblo "Kubernetes Series" (19 articles, after *Kubernetes in Action*), each topic tied to what Task 2 runs. K1–K12 = concepts, K13–K18 = one real-life use case (6 slides).

---
# Slide K1 — The map: Kubernetes topics → our cluster
| Topic (series article) | Object / mechanism | Where it is in Task 2 |
|---|---|---|
| Pod (2), Pod internals (12) | pause container, init containers, probes | HMS `schema-init` initContainer; MinIO readiness probe |
| Controllers (4), Deployment (5) | ReplicaSet, Deployment, Job, CronJob, DaemonSet | Airflow ×4, HMS, operator; bucket-init; pg-backup; kindnet |
| Services (4) | ClusterIP, headless, DNS | `minio.lake.svc.cluster.local:9000`, headless postgres |
| Volume (6), PVC (7) | emptyDir, PVC, StorageClass | Spark local dirs, `data-minio-0`, `standard` hostPath |
| ConfigMap/Secret (8) | env vs files, hash names | secretGenerator (E54), Secrets as files (E27) |
| StatefulSet (9) | ordinal identity, volumeClaimTemplates | `minio-0`, `postgres-0` |
| Downward API (10) | pod metadata into env/files | Spark driver binds to `status.podIP` |
| Internals (11) | API server, etcd, watch, controllers | every drill (slide 23) |
| RBAC (13) | ServiceAccount, Role, RoleBinding | Airflow → SparkApplications only |
| Pod/Network security (14) | Pod Security Admission, NetworkPolicy | fsGroup, non-root; NetworkPolicy not enforced (kindnet) |
| Resources (15), autoscaling (16) | requests/limits, QoS, HPA/VPA/CA | E25, E52; slides 6, 42–45 |
| Scheduling (17–18) | taints, affinity | not needed on 1 node; use case K14 |
| Custom resources (19) | CRD + controller | SparkApplication (slide 8) |

Speaker notes: Ingress is the one big topic the series skips; the use case (K13) covers it.

---
# Slide K2 — Inside a Pod
- A Pod = one network namespace + shared volumes for 1..n containers. The hidden **pause** container holds the namespace; app containers join it → they talk over `localhost`, share the pod IP
- **Init containers** run first, in order, each to completion → our HMS `schema-init` runs `schematool` before the metastore starts
- **Sidecars** (native since 1.29: init container with `restartPolicy: Always`) run beside the app → Airflow's log-groomer sidecars
- **Probes:** startup (slow boot), readiness (in or out of the Service), liveness (restart if stuck). Readiness ≠ liveness: a not-ready pod gets no traffic but is not killed
- **Phases:** Pending → Running → Succeeded/Failed (Unknown = node lost, E40). `restartPolicy` Always / OnFailure / Never decides what the kubelet does on exit
- **Termination:** SIGTERM → `terminationGracePeriodSeconds` → SIGKILL; preStop hooks run first. Too short a grace killed `validate` in E41 (10 s → 120 s)

Visual: one pod box: pause container in the middle, app + sidecar around it, shared volume at the bottom, pod IP on the edge.

Speaker notes: Everything the kubelet does to a pod is driven by these few fields; most "mysterious" restarts are a probe or a grace period.

---
# Slide K3 — Controllers: who keeps what alive
| Controller | Keeps | Pod identity | Typical use | Ours |
|---|---|---|---|---|
| ReplicationController | N pods (legacy, equality selectors) | interchangeable | replaced by ReplicaSet | — |
| ReplicaSet | N pods matching a label selector | interchangeable | only via a Deployment | under every Deployment |
| Deployment | ReplicaSets across versions | interchangeable | stateless apps, rolling updates | Airflow, HMS, operator |
| StatefulSet | N pods with ordinal names + own PVCs | stable (`-0`, `-1`) | databases, brokers | MinIO, Postgres |
| DaemonSet | one pod per (matching) node | per node | node agents: CNI, logs, metrics | kindnet, kube-proxy |
| Job | pods until N completions | run to completion | batch, migrations | bucket-init, minio-users, Airflow migrations |
| CronJob | a Job per schedule | per run | periodic batch | pg-backup 01:00 UTC |

- All use the same loop: **watch** desired objects + their pods → compare counts/specs → create/delete pods. Pods point back with `ownerReferences` → deleting the owner garbage-collects them
- Labels are the glue: a ReplicaSet "owns" whatever matches its selector; a hand-made pod with matching labels would be adopted (and maybe deleted)

Speaker notes: Picking the controller is picking the failure behaviour. Our E54 is a StatefulSet property: it will not replace a pod that never became Ready.

---
# Slide K4 — Example: stateless app + replicas = ReplicaSet
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: chat-api, namespace: bot }
spec:
  replicas: 3
  selector: { matchLabels: { app: chat-api } }
  strategy: { rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } }
  template:
    metadata: { labels: { app: chat-api } }
    spec:
      containers:
        - name: api
          image: ghcr.io/org/chat-api@sha256:…
          resources: { requests: { cpu: 250m, memory: 256Mi }, limits: { memory: 512Mi } }
          readinessProbe: { httpGet: { path: /ready, port: 8080 } }
```
- Deployment → creates ReplicaSet `chat-api-7d9f…` (hash of the pod template) → 3 pods `chat-api-7d9f…-xxxxx`
- Kill a pod → ReplicaSet sees 2 ≠ 3 → new pod (new name, new IP); the Service keeps routing to ready ones
- `kubectl scale --replicas=5` → 2 more pods; scheduler spreads them by requests
- New image → a **second** ReplicaSet grows 0 → 3 while the old one shrinks 3 → 0 (maxSurge 1, maxUnavailable 0 = never below 3 ready); the old ReplicaSet is kept at 0 for `rollout undo`
- Works because the app is **stateless**: any replica can answer any request; state lives in a database or cache outside the pod

Visual: Deployment → ReplicaSet v1 (3 pods) and ReplicaSet v2 (0 → 3) side by side with arrows during a rollout.

Speaker notes: Our Airflow api-server is the same shape with replicas 1. If two pods needed different data or a stable name, this pattern would be wrong; that is the StatefulSet case.

---
# Slide K5 — Rollouts: Deployment vs StatefulSet
| | Deployment | StatefulSet |
|---|---|---|
| Strategy | RollingUpdate (maxSurge, maxUnavailable) or Recreate | RollingUpdate (one pod at a time, highest ordinal first, optional `partition`) or OnDelete |
| New pod before old dies? | yes with maxSurge > 0 | no: same name, so old must go first |
| History / undo | old ReplicaSets → `kubectl rollout undo` | ControllerRevisions; `rollout undo` too, but a pod that never became Ready must be deleted by hand (E54) |
| Readiness gate | waits for new pods Ready before continuing | waits for each pod Ready before the next |
| Disruption guard | PodDisruptionBudget limits voluntary evictions (drain, upgrades) | same; critical for quorum systems |

- Our cases: scheduler rollout killed a running task (E41) → longer grace; MinIO stuck on a bad template (E54) → re-apply with `-k`, delete the pod
- `kubectl rollout status` = the command every make target waits on

Speaker notes: A rollout is only as safe as the readiness probe: without one, Kubernetes declares success as soon as the container starts.

---
# Slide K6 — Services and networking
| Type | Reachable from | Use |
|---|---|---|
| ClusterIP (default) | inside the cluster, virtual IP | MinIO, HMS, Postgres, Airflow api-server |
| Headless (`clusterIP: None`) | DNS returns pod IPs | StatefulSet pod names, Spark driver ↔ executors |
| NodePort | every node's IP:30000–32767 | quick tests |
| LoadBalancer | cloud load balancer IP | public entry in the cloud |
| ExternalName | DNS alias to an outside name | managed DB behind a stable in-cluster name |

- Path of a request: client → `minio.lake.svc.cluster.local` (CoreDNS) → ClusterIP → kube-proxy rules (iptables/IPVS, or eBPF with Cilium) → one **ready** pod from the EndpointSlice
- Readiness controls membership: a pod failing readiness leaves the EndpointSlice; no traffic, no restart
- L7 entry: **Ingress** (or the newer Gateway API) = host/path routing + TLS in front of ClusterIP Services; we use `kubectl port-forward` instead (one user, laptop)
- Pod network: every pod gets a routable IP (CNI: kindnet here, Calico/Cilium in production); NetworkPolicy needs a CNI that enforces it

Speaker notes: E37 was a networking lesson: pod hostnames are not in DNS, pod IPs are, so the Airflow UI fetches logs by IP.

---
# Slide K7 — Storage: Volume, PV, PVC, StorageClass
| Volume | Lifetime | Ours |
|---|---|---|
| emptyDir (disk or `medium: Memory`) | the pod | Spark local dirs, landing simulator `/work`, HMS `/home/hive` (E28) |
| configMap / secret / projected | the pod (content from the API) | core-site.xml, hive-site.xml, MinIO keys |
| hostPath | the node | minikube's `standard` provisioner under the hood |
| persistentVolumeClaim | independent of the pod | `data-minio-0` 5 Gi, `data-postgres-0` 2 Gi |

- Decoupling (article 7): the developer asks for "5 Gi, RWO" (PVC); the cluster's StorageClass provisions a PV; the pod only names the claim → same manifest on Minikube and in the cloud
- Access modes: RWO (one node), ROX, RWX (many nodes, needs NFS/CephFS/EFS), RWOP (one pod)
- Reclaim policy: Delete (PV goes with the PVC) vs Retain (keep the disk for manual recovery) → production databases: Retain
- Binding modes: `WaitForFirstConsumer` creates the disk in the zone where the pod is scheduled

Speaker notes: The storage abstraction is why `minikube delete` is the only command that loses our data: pods and StatefulSets come and go, the claims stay.

---
# Slide K8 — Configuration: ConfigMap, Secret, Downward API
- **ConfigMap** = non-secret config; **Secret** = base64 (not encrypted) values, encrypted at rest only if the API server is configured to; both injected as **env** or **files**
- Env is read once at start; mounted files are updated in place (~1 min) unless `subPath` is used → a config change via env needs a pod restart
- **Generated names with a content hash** (kustomize): a changed config = a new name = the referencing pods roll automatically. Trap: applying one file without kustomize points the pod at a name that doesn't exist (E54)
- `immutable: true` protects big ConfigMaps/Secrets from edits and saves API watch load
- **Downward API:** a pod's own metadata (name, namespace, labels, IP, resource limits) as env or files without talking to the API → Spark's driver gets its bind address from `status.podIP`
- Security: Secrets as files with `defaultMode` 0440 + `fsGroup` (E27), never in images, Helm values (E35) or logs

Speaker notes: In production, Secrets usually come from an external store (Vault, cloud secret manager) through the External Secrets operator; the pod side stays the same.

---
# Slide K9 — Internals: one request, every component
`kubectl apply -f deployment.yaml` (3 replicas):
1. **kubectl** → HTTPS → **kube-apiserver**: authenticate (cert/token) → authorize (RBAC) → admission (mutating webhooks, defaults, validating) → write Deployment to **etcd**
2. **Deployment controller** (in kube-controller-manager) is *watching* Deployments → creates a ReplicaSet (another write through the API server)
3. **ReplicaSet controller** watches ReplicaSets → creates 3 Pods with `nodeName` empty
4. **kube-scheduler** watches unscheduled Pods → filters nodes (requests vs Allocatable, taints, affinity) → scores → writes a **Binding**
5. **kubelet** on that node watches Pods bound to it → pulls the image (CRI → containerd) → starts the pause + app containers → reports status
6. **EndpointSlice controller** adds ready pod IPs → **kube-proxy** on every node programs the Service rules → **CoreDNS** answers the name

- Only the API server talks to etcd; every component is a client that watches and reacts. No component calls another directly
- `kubectl get events -w` shows these steps live (Scheduled, Pulling, Pulled, Created, Started)

Speaker notes: This chain explains debugging: Pending = step 4, ImagePullBackOff = step 5, Running but no traffic = step 6.

---
# Slide K10 — Authority matrix: who may do what, and why
| Component | Identity | May change (authority) | Scope | Why limited |
|---|---|---|---|---|
| kube-apiserver | serving cert | the only writer to etcd; enforces authn/authz/admission for everyone | cluster | single gate = single audit point |
| etcd | peer/client certs | stores everything; accepts only the API server | cluster data | whoever reads etcd reads every Secret |
| kube-scheduler | `system:kube-scheduler` | pod **bindings** (+ events); cannot create pods | cluster | decides placement, nothing else |
| kube-controller-manager | one SA per controller | the objects its controllers own (ReplicaSets, Pods, EndpointSlices, Jobs…) | cluster | each loop limited to its resources |
| kubelet | `system:node:<name>` (Node authorizer + NodeRestriction) | its own Node object and pods bound to it; reads only Secrets/ConfigMaps those pods use | **one node** | a compromised node cannot read other nodes' Secrets |
| kube-proxy | `system:kube-proxy` | nothing in the API (read Services/EndpointSlices); writes local iptables | node | dataplane only |
| CoreDNS | its SA | read Services/EndpointSlices | cluster read | answers names only |
| Spark Operator controller | `spark-operator` SA | SparkApplications, driver pods in `spark-jobs` (`jobNamespaces`) | namespace(s) | runs spark-submit |
| Spark Operator webhook | webhook SA + TLS | mutates driver/executor pods at admission | `spark-jobs` pods | `failurePolicy: Fail` |
| Airflow scheduler | `airflow-scheduler` SA | create/watch/delete SparkApplications; read pods + logs | `spark-jobs` only | cannot read Secrets or touch `lake` |
| Spark driver | `spark-operator-spark` SA | create/delete executor pods, services, configmaps | `spark-jobs` | executors only |
| MinIO, Postgres, HMS | default SA | **no API rights** needed | — | improvement: `automountServiceAccountToken: false` |

Check any line: `kubectl auth can-i create pods -n spark-jobs --as=system:serviceaccount:airflow:airflow-scheduler` → `no`

Speaker notes: The pattern is least privilege by scope: cluster components get exactly their loop's resources, apps get one namespace, and data services get nothing from the API.

---
# Slide K11 — RBAC and ServiceAccounts in practice
- Subjects: **users** (certs/OIDC, outside the cluster), **groups**, **ServiceAccounts** (in-cluster identities, one per app)
- A pod's SA token is projected into `/var/run/secrets/kubernetes.io/serviceaccount/` — short-lived, audience-bound, rotated by the kubelet (no long-lived token Secrets since 1.24)
- **Role** (namespaced) / **ClusterRole** (cluster-wide or reusable) = list of (apiGroup, resource, verb); **RoleBinding** / **ClusterRoleBinding** = who gets it, where
- Subresources are separate: `pods/log`, `pods/exec`, `sparkapplications/status` → our Airflow Role allows `pods/log get` but not `pods/exec`
- Verbs: get, list, watch (read) · create, update, patch, delete (write) · escalate, bind, impersonate (dangerous)
- Aggregated ClusterRoles: `view`, `edit`, `admin` grow automatically when CRDs add rules
- Ours: `infra/k8s/spark-jobs/rbac-airflow.yaml` (Role + RoleBinding across namespaces: subject in `airflow`, rights in `spark-jobs`)

Speaker notes: RBAC is additive only: there is no "deny" rule. Safety comes from granting little, per namespace, to one identity per app.

---
# Slide K12 — Pod and network security
- **Pod Security Admission** (replaced PodSecurityPolicy, removed in 1.25): namespace labels `pod-security.kubernetes.io/enforce=baseline|restricted`. `restricted` requires non-root, no privilege escalation, dropped capabilities, seccomp
- **securityContext** per pod/container: `runAsNonRoot`, `runAsUser`, `fsGroup` (volume group ownership, E27), `readOnlyRootFilesystem`, `capabilities.drop: [ALL]`
- **NetworkPolicy**: default allow-all; a policy selecting a pod turns it into default-deny for that direction; allow lists by pod/namespace labels and ports. Example: only `spark-jobs` and `lake` may reach MinIO :9000; only HMS may reach Postgres :5432
- Enforced by the CNI only: kindnet ignores NetworkPolicy → in our lab it is documentation; Calico/Cilium enforce it
- Images: by digest, non-root base (Chainguard MinIO), scanned in CI (Trivy)

Speaker notes: RBAC protects the API; Pod Security protects the node from the pod; NetworkPolicy protects pods from each other. Our lab has the first fully, the second partly, the third only on paper.

---
# Slide K13 — Use case (1/6): a chatbot platform on Kubernetes
Scenario (illustrative, sized for discussion, not measured): the chatbot whose logs we load, run on Kubernetes for several provinces.

| Need | Number (assumed) | Kubernetes consequence |
|---|---|---|
| Chat API traffic | ~200 req/s peak at 9–11 h, 10× lower at night | stateless Deployment + HPA |
| NLU model inference | 50–150 ms per call, CPU-bound | separate Deployment, own node pool, HPA on CPU |
| Sessions / conversation state | must survive pod restarts | Redis (StatefulSet or managed) |
| Chat log storage | durable, backed up | Postgres/Oracle outside the app pods |
| Daily analytics | the lakehouse of Tasks 1–5 | Spark Operator + Airflow, batch node pool |
| Availability | 99.9 % for the API | ≥ 2 replicas, spread across zones, PDBs |
| Teams | bot team, data team, platform team | namespaces + RBAC + quotas |

Speaker notes: The point is not the numbers but how each requirement maps to an object type; the next five slides do the mapping.

---
# Slide K14 — Use case (2/6): namespaces, workloads, scheduling
| Namespace | Workload | Object | Scheduling |
|---|---|---|---|
| `bot` | chat-api | Deployment 3–20 replicas + HPA + PDB `minAvailable: 2` | general pool; topologySpreadConstraints across zones |
| `bot` | nlu-serving | Deployment 2–10 + HPA | **node affinity** to `pool=inference`; pod anti-affinity (not 2 on one node) |
| `bot` | redis | StatefulSet 3 (primary + replicas) or managed | anti-affinity, PVC per pod, Retain |
| `data` | Airflow | Deployments (api-server ×2, scheduler ×2, dag-processor, triggerer), KubernetesExecutor | general pool |
| `data` | Spark jobs | SparkApplications | **taint** `pool=batch:NoSchedule` on spot nodes + **toleration** in executors; drivers on on-demand |
| `platform` | ingress controller, cert-manager, monitoring | Deployments + DaemonSets (log/metrics agents) | every node (DaemonSet) |

- Taints keep everyone else **off** expensive or unstable nodes; affinity pulls a workload **onto** the right nodes (articles 17–18)
- PriorityClass: chat-api > nlu > Airflow > Spark executors → under pressure, batch is preempted first

Speaker notes: Our lab is the `data` namespace of this picture on one node; the scheduling features only matter once there is more than one kind of node.

---
# Slide K15 — Use case (3/6): the stateless tier end to end
- Entry: DNS → cloud LoadBalancer → **Ingress** (TLS by cert-manager, host `chat.example.vn`, path `/api`) → Service `chat-api` (ClusterIP) → ready pods
- chat-api: Deployment (K4 shape), readiness `/ready` checks Redis + NLU reachability, liveness `/healthz` checks only the process
- Scaling: **HPA** on CPU 60 % (and requests-per-second from Prometheus Adapter) → more pods → Pending → **Cluster Autoscaler/Karpenter** adds nodes (~1–2 min) → headroom pods hide that delay
- Release: new digest via GitOps (Argo CD) → RollingUpdate maxSurge 25 %, maxUnavailable 0 → readiness gates each step → PDB protects during node drains; canary = a second Deployment with 5 % of traffic at the Ingress
- Config: ConfigMap for thresholds (NLU_THRESHOLD lives here, not in code), Secrets from External Secrets; config hash in a pod annotation → change = rollout
- Same rules as our lab: requests from measurements, limits on memory, images by digest, one SA per app with no API rights

Visual: request path left to right, with HPA and autoscaler arrows below the pods.

Speaker notes: Everything here is the K4 example plus four additions: Ingress, HPA, PDB, GitOps.

---
# Slide K16 — Use case (4/6): permissions, scope and authority per team
| Who | Identity | Granted | Scope | Cannot |
|---|---|---|---|---|
| Bot developers | OIDC group `bot-dev` | `edit` ClusterRole via RoleBinding | `bot` | read Secrets in `data`; touch nodes; change RBAC |
| Data engineers (us) | `data-eng` | `edit` | `data` | change `bot`; exec into chat-api pods |
| Platform team | `platform-admin` | `cluster-admin` (break-glass, audited) | cluster | — |
| CI/CD (Argo CD) | SA `argocd-application-controller` | apply manifests | the namespaces it manages | read app data |
| chat-api pods | SA `chat-api`, token not mounted | none in the API; DB/Redis credentials only | its own Secrets | call the Kubernetes API |
| Airflow scheduler | SA | SparkApplications + pod logs | `data` | Secrets, other namespaces |
| Spark driver | SA | executor pods | `data` | anything else |

- Quotas per namespace (ResourceQuota + LimitRange): `data` capped so a backfill cannot starve the chat API
- NetworkPolicy default-deny per namespace, then allow: Ingress → chat-api; chat-api → nlu, redis; Spark → object store, catalog
- Pod Security `restricted` on `bot`, `baseline` on `data` (Spark images need some extra rights)

Speaker notes: Authority follows ownership: teams own namespaces, apps own one SA each, and only the platform team holds cluster-wide rights, through an audited path.

---
# Slide K17 — Use case (5/6): stateful tier and the data platform
- Databases: production choice = **managed** (RDS/Cloud SQL) or an operator (CloudNativePG, Zalando) running a StatefulSet with replicas, failover and backups; never a single `postgres-0` like our lab
- Redis: StatefulSet with headless Service + Sentinel/Cluster mode, or managed; PVCs with `Retain`
- StatefulSet + node loss: pods on a dead node stay `Terminating` until the node is confirmed gone (the controller cannot risk two pods with the same identity) → out-of-service taint or node deletion lets them move
- Data platform (our Tasks 1–5): Iceberg on object storage, a catalog service, Spark Operator for batch, Airflow for scheduling, Trino/StarRocks for queries, Superset for dashboards → mostly stateless compute on top of two stateful services (object store, catalog DB)
- That ratio is the design goal: keep state in few, well-protected places; make everything else replaceable

Speaker notes: Our lab already has this shape. The production difference is who runs the two stateful pieces and how many copies they have.

---
# Slide K18 — Use case (6/6): failure walkthrough at peak
| Time | Event | Who reacts | Result |
|---|---|---|---|
| t0 | node with 2 chat-api + 1 nlu pod loses power | — | traffic to those pods fails |
| +0–10 s | readiness/endpoint updates as kubelet heartbeats stop | — | — |
| +~40–50 s | node-lifecycle controller marks the node `NotReady`, adds taint `node.kubernetes.io/unreachable` | controller-manager | EndpointSlices drop its pods → traffic only to healthy pods (other replicas carry it, HPA may add pods) |
| +300 s | pods' default toleration `unreachable … for 300s` expires (seen in our `describe pod`) | taint eviction | ReplicaSets create replacements on other nodes |
| +1–2 min after | new node from the autoscaler if capacity is short | Cluster Autoscaler | capacity restored |
| StatefulSet pod (redis-1) on that node | not replaced automatically | operator/human | out-of-service taint → replacement with the same PVC |

- Bad release instead of a dead node: new pods fail readiness → rollout stops (maxUnavailable 0) → old ReplicaSet still serves → `kubectl rollout undo` / Git revert
- What makes this work: ≥ 2 replicas spread across nodes, readiness probes, PDBs, requests honest enough for the scheduler to place replacements

Speaker notes: Shorten the 300 s with `tolerationSeconds` on stateless pods if faster failover is worth more eviction churn. Our drills 1–3 are this table on one node.

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
# Slide 29 — Errors and fixes (grouped, E25–E55)

| Theme | Errors | Lesson |
|---|---|---|
| Resources | E25 Allocatable 10.7 GiB on an 8 GiB node; E52 scheduler OOM | Measure what the kubelet and the container really see, then size |
| Airflow 3 changes | E34 DagBag API; E37 log hostname; E38 zero-length intervals; E47 no logical date; E49 one run per date | Read the version's defaults; print and assert what the run receives |
| Deploy discipline | E31/E32 digests not recorded; E33 secrets too early; E39 deployed from a branch; E36 chart warnings | Deploy only reviewed main by digest; record the git SHA in the release |
| Secrets | E27 secret file not readable (fsGroup); E35 password in chart NOTES; E42 my test overwrote secrets; E43 token in chat | Files with the right group; never print; test in a scratch copy |
| Recovery | E41 rollout killed validate; E46 Iceberg's 24 h orphan guard | Grace periods and one retry for interruptions; respect guard rails |
| Readers and reports | E44 alert texts; E51 report read local logs after logs moved to S3 | Move every reader with the source of truth; an empty report must fail |
| Operating after the tag | E53 kubectl without a context; E54 Secret name drift from an apply without kustomize; E55 merged before CI registered | Read the address in the error; apply `infra/k8s/` only with `-k`; wait until the checks exist |
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
# Part 5 — Deep dive: questions from the stack review

Full written answers with file references: `docs/task2/08_study_notes.md` (Q1–Q52).

---
# Slide 34 — Global view: one stack, six layers
| Layer | Our pieces | Question topics | Slides |
|---|---|---|---|
| Host and runtime | Windows → WSL2 → Docker Desktop → Minikube node (containerd) | node access, drivers, dockershim and shims, namespaces, local vs cloud | 36–40 |
| Kubernetes core | API server, controllers, StatefulSets, PVCs | objects, storage, authority, use case | 37, 41; K1–K18 |
| Resources | requests, limits, Allocatable, QoS | OOM types, overcommit, VPA/HPA, node scaling | 42–45 |
| Delivery | Helm, kustomize, GitHub Actions, GHCR | chart writing, revisions, values, rollback, Argo CD | 46–47 |
| Compute | Spark 3.5.9 via Spark Operator 2.2.1 | memory model, driver ↔ API, operator, fault tolerance, joins, Trino, streaming | 48–53 |
| Table and catalog | Iceberg 1.11.0 + HMS 3.1.3 on MinIO | commits, COW/MOR, catalogs, embedded vs external, snapshots, maintenance | 54–58 |
| Orchestration | Airflow 3.3.2 (LocalExecutor) | components, executors, timetables, Jinja, 15 rules, idempotency, DAG delivery, upgrade | 59–66 |
| Cross-cutting | drills, strategy | drills on a pipeline, should we change the strategy | 67–69 |

- 52 questions; this deck gives the decision-level answer per question and ties it to Task 2 evidence (Q1–Q52 numbered in the order asked)
- Q51 (Airflow → Trino/dbt → Iceberg/HMS layout): answered in the notes (§7); its slide comes with Task 3, when Trino exists

Speaker notes: Each layer only talks to the one below it through an API: kubectl to the API server, Spark to the catalog and S3, Airflow to Kubernetes. Most of my errors happened where a layer made an assumption about the layer below (E25 memory, E38 intervals, E52 process count).

---
# Slide 35 — Global view: one day of data through every layer
1. 00:00 UTC D+1: scheduler (data-interval timetable) creates run D; LocalExecutor worker process inside the scheduler pod starts check_landing
2. check_landing → S3 LIST on MinIO (user airflow-landing)
3. bronze → SparkKubernetesOperator POSTs a SparkApplication to the API server → etcd
4. Spark Operator controller sees it → spark-submit → driver pod; mutating webhook injects Secrets/volumes
5. kube-scheduler places the driver by its requests → kubelet → containerd → containerd-shim → runc
6. Driver (ServiceAccount token) asks the API server for an executor pod
7. Executors read landing, write Parquet to MinIO; driver commits: new manifests → manifest list → metadata.json → HMS swaps the pointer (CAS)
8. Operator streams status; Airflow streams the driver log, marks success → validate → notify; logs to s3://airflow-logs

Visual: one vertical swimlane per layer, numbered arrows 1–8.

Speaker notes: When something breaks, I locate the step number first. Drill 1 broke step 6–7, drill 3 broke step 1, drill 6 broke step 7's catalog.

---
# Slide 36 — Getting inside the node; drivers (Q1)
| Level | What it is | How to enter |
|---|---|---|
| WSL2 VM | Linux VM running Docker Desktop's engine | `wsl` |
| Node | Docker container `minikube` (kicbase) | `minikube ssh` (= `docker exec -it minikube bash`) |
| Containers on the node | managed by containerd, not Docker | inside the node: `sudo crictl pods`, `sudo crictl ps`, `crictl stats` |
| Pod / container | pause container + app containers sharing net namespace | `kubectl exec -it <pod> -c <container> -- sh` |
| Node from the API | debug pod with host namespaces | `kubectl debug node/minikube -it --image=busybox` |

- `--driver=docker`: node = container, shares the WSL2 kernel; light and fast, but the kubelet read the VM's memory (10.7 GiB), not the 8 GiB container cap → E25
- VM drivers (Hyper-V, VirtualBox, KVM, QEMU): node = VM with its own kernel and RAM → capacity is real; heavier, needs hypervisor access from the host OS
- Production: several Linux VMs or bare-metal nodes (kubeadm, or a managed service), HA control plane with 3 etcd members
- Docker Desktop's own Kubernetes: kubeadm mode runs pods on Docker Desktop's engine (one shared image store, no node container); kind mode runs nodes as containers, like Minikube

Speaker notes: "Minikube ssh" is just entering the node container. The useful habit is knowing which tool sees which level: docker sees the node, crictl sees pods, kubectl sees API objects.

---
# Slide 37 — How objects come alive (Q2 + controllers)
- `kubectl apply` → API server: authN → authZ (RBAC) → mutating webhooks → schema validation → validating webhooks → stored in etcd. Nothing runs yet
- Controllers react through watches (the reconcile loop):
  - Deployment controller → ReplicaSet → ReplicaSet controller → Pods (no node yet)
  - kube-scheduler binds each Pod to a node (requests vs Allocatable)
  - kubelet on that node starts the containers
  - Service: EndpointSlice controller lists ready pod IPs matching the selector; kube-proxy programs iptables; CoreDNS answers `minio.lake.svc.cluster.local`
  - Headless Service (`clusterIP: None`, our postgres): DNS returns pod IPs directly, needed for StatefulSet pod names
- Custom resources follow the same path; only the controller is third-party (Spark Operator instead of kube-controller-manager)
- `kubectl get deployments`: desired vs ready vs up-to-date replicas → is the rollout done
- `kubectl get secrets`: names, type, number of keys; values only base64 in `-o yaml` (not encryption). Our names end with a content hash from kustomize (`postgres-credentials-m864d4fd7f`); applying one file with `-f` loses that hash (E54)

Speaker notes: The API server never starts anything itself. Every visible effect is a controller noticing a difference between desired and actual state.

---
# Slide 38 — Runtimes: why no dockershim, and what a shim is (Q4, Q21)
- Kubelet talks to runtimes through CRI (gRPC). Docker Engine never implemented CRI, so the kubelet carried an adapter, dockershim: kubelet → dockershim → dockerd → containerd → runc
- Deprecated in 1.20, removed in 1.24 (2022): one fewer hop, no Docker-only code in the kubelet, Docker features the kubelet never used (build, swarm, its own network) left out
- Now: kubelet → CRI → containerd (ours, 2.3.4) or CRI-O → runc. Docker-built images still run: they are OCI images. Teams that need dockerd use cri-dockerd (Mirantis, outside Kubernetes)

| | dockershim | containerd-shim (e.g. containerd-shim-runc-v2) |
|---|---|---|
| Where | inside the kubelet (old) | one small process per pod/container on the node |
| Job | translate CRI calls into Docker API calls | parent of the container process: keeps stdio, reports the exit code, lets containerd restart without killing containers |
| Today | removed | in every containerd and Docker installation |

Speaker notes: Same word, two layers: dockershim was an API translator in Kubernetes; a containerd shim is a process supervisor under the runtime. Docker itself uses containerd and its shim.

---
# Slide 39 — Four kinds of "namespace" (Q19)
| Namespace | Layer | What it isolates | Ours |
|---|---|---|---|
| Linux namespaces (what Docker uses) | kernel | process view: pid, net, mount, uts, ipc, user | each container has its own; pods share net |
| Kubernetes namespace | API | names, RBAC, quotas; NOT a network or security boundary by itself | lake, spark-operator, spark-jobs, airflow |
| Spark | config + SQL | `spark.kubernetes.namespace` = where pods go; in SQL `catalog.namespace.table` = Iceberg namespace | pods in spark-jobs; table `lake.bronze.chatlog` |
| Iceberg namespace | catalog | a group of tables (= Hive database) | `bronze` (→ `bronze.db/` in the warehouse path) |

Speaker notes: In one command I use three of them: the driver pod runs in K8s namespace spark-jobs, inside Linux namespaces, and writes Iceberg namespace bronze.

---
# Slide 40 — Local vs cloud Kubernetes (Q3)
| Concern | Minikube (ours) | Managed cloud (EKS, GKE, AKS) |
|---|---|---|
| Control plane | one pod each, on the same node | run by the provider, HA, upgrades handled |
| Nodes | 1 container, 8 GiB | node pools, autoscaled, mixed on-demand / spot |
| Storage | hostPath provisioner, lost on `minikube delete` | CSI cloud disks with snapshots; object store as S3 |
| Network | kindnet (no NetworkPolicy), port-forward | VPC CNI, LoadBalancer Services, Ingress |
| Identity to storage | static MinIO keys in Secrets | workload identity (IRSA, GKE WI): pod gets a cloud role, no keys |
| Registry pull | ghcr-pull Secret | same, or the cloud registry with node IAM |
| Cost model | laptop | pay per node-hour → requests drive cost |
| Same in both | manifests, Helm charts, RBAC, CRDs, operators | |

Speaker notes: Everything written for Task 2 is portable; what changes in the cloud is who runs the control plane, where data persists, and that over-sized requests cost money.

---
# Slide 41 — StatefulSet, PVC, PV, StorageClass (Q31)
- StatefulSet postgres: stable name `postgres-0`, `volumeClaimTemplates` → creates PVC `data-postgres-0` (2 Gi, RWO)
- PVC = request for storage; StorageClass `standard` (provisioner `k8s.io/minikube-hostpath`) creates a PV dynamically and binds it
- PV = the actual storage; here a directory inside the node container
- Lifecycle: delete the pod → same PVC re-attached (E54: `minio-0` deleted, data kept); delete the StatefulSet → PVC stays (we deleted `data-postgres-0` by hand to reset passwords, E27); reclaim policy Delete removes the PV when the PVC goes
- Deployment + PVC would share one volume between replicas; StatefulSet gives each replica its own

| | Network/native (CSI: cloud disks, Ceph, Longhorn) | Host-based (hostPath, local PV) |
|---|---|---|
| Survives node loss | yes, re-attaches on another node (same zone) | no, data is on that node |
| Speed / cost | network latency, paid | local disk speed, free |
| Use | databases in production, catalog DB | single-node labs, caches, data already replicated by the app (MinIO erasure sets, Kafka) |

Speaker notes: Our backup lived on a hostPath PV of the same node, which is why `minikube delete` erased it (slide 24).

---
# Slide 42 — Requests, usage, limits (Q43)
- kube-scheduler only adds requests: a pod fits if Σ requests on the node + its request ≤ Allocatable. It never looks at real usage
- Usage (metrics-server, `kubectl top`) is what the kernel sees; limits are what the kernel enforces
- Our node, idle (measured 10-27/10-28): requests **4,036 Mi (55 %)**, usage 3,312–4,393 Mi (45–60 %), limits **7,516 Mi (103 %)** of 7,268 Mi Allocatable; a bronze run adds +3,200 Mi of requests → 7,236 Mi
- Set requests from usage over time, not one sample: p95 across a full backfill including the 07-24 peak (12,970 rows), per component
- Request = protects scheduling (my pod gets room); limit = protects the neighbours (my pod cannot take theirs)
- Memory is incompressible (over → kill); CPU is compressible (over → throttled)

Visual: three bars for the node: requests, usage, limits against the Allocatable line.

Speaker notes: Usage above requests is legal and common (Burstable). It becomes a problem only when many pods burst together and the node itself runs out, which triggers eviction.

---
# Slide 43 — Pod deleted vs container OOMKilled; reserved memory; overcommit (Q44)
| | Pod deleted / evicted | Container OOMKilled |
|---|---|---|
| Who | API (kubectl delete, drain, preemption) or kubelet eviction under node pressure | kernel, container's cgroup over its limit |
| Effect | pod object gone; a controller creates a new pod (new name/UID) | same pod; container restarted; RESTARTS +1; Last State OOMKilled, 137 |
| Ours | drill 1 (driver), drill 2 (executor), E54 (forced rollback) | E52 scheduler, 9 restarts |

- Eviction order under node pressure: BestEffort first, then pods using most above their request; Guaranteed last
- kube-reserved = kubelet + container runtime; system-reserved = OS daemons; plus eviction threshold → Allocatable. Control-plane pods (apiserver 379 Mi, etcd) on Minikube are ordinary pods and take Allocatable too
- Limiting Spark: `spark.executor.instances` or dynamic allocation `maxExecutors`; memory + overhead → request = limit; and a ResourceQuota on spark-jobs
- Overcommit: Σ limits > Allocatable is allowed (ours 103 %); fine for CPU, risky for memory because bursts coincide (backfill + maintenance + CLI)

Speaker notes: Two very different failures look similar in a status column. Restarts in place mean "your limit is too small"; new pods mean "something removed you".

---
# Slide 44 — Two kinds of OOM, tmpfs, how to observe (Q46)
| | JVM OutOfMemoryError | Kubernetes OOMKilled |
|---|---|---|
| Trigger | heap above `-Xmx` (= spark.executor.memory) | whole container RSS above the cgroup limit |
| Counts | heap objects only | heap + metaspace + direct buffers + thread stacks + Python workers + memory-backed tmpfs |
| Signal | Java exception, stack trace, Spark task failure | SIGKILL, exit 137, log stops mid-line |
| Fix | more heap, less data per task, spill | more overhead / limit, fewer processes |

- cgroup v2 (Kubernetes ≥ 1.28): `memory.oom.group` = 1 → one OOM kills every process in the container → E52's `container not found ("scheduler")`
- tmpfs: RAM-backed filesystem; an `emptyDir` with `medium: Memory` (and /dev/shm) counts against the container's memory limit. `spark.kubernetes.local.dirs.tmpfs=true` puts Spark's spill directory in RAM → spill can itself cause OOMKilled
- Observe: `kubectl describe pod` (Last State, Reason), events, `kubectl top` (working set), `crictl stats`, cgroup `memory.events` (oom_kill counter), process view (our psutil), Spark UI executor peak memory metrics

Speaker notes: E52 was the second kind: no Java error anywhere, only exit codes, which is why I measured processes inside the container instead of reading logs.

---
# Slide 45 — Scaling: VPA/HPA, quotas, nodes, headroom (Q45, Q32)
- HPA: more replicas from CPU/memory/custom metrics → stateless (api-server, Trino workers)
- VPA: rewrites requests from observed usage; recommend-only mode is safe for stateful pods. In-place pod resize is GA since K8s 1.35, VPA `InPlaceOrRecreate` is beta; JVM and Python do not resize their heap without a restart. Do not run HPA and VPA on the same metric
- LimitRange: per-container default/min/max in a namespace (a pod without requests gets defaults). ResourceQuota: total requests/limits/pods per namespace (e.g. cap spark-jobs so a backfill cannot starve airflow/lake)
- Utilization = requests/Allocatable (what you pay for) and usage/Allocatable (what you use); the gap is waste
- Headroom: keep 10–20 % free, or low-priority placeholder pods that get preempted when real pods need space
- Node scaling: Cluster Autoscaler (node groups; adds a node when pods are Pending; removes empty ones) vs Karpenter (picks an instance type per pending pods, consolidates). Both react to requests, not usage
- Node sizing: few large nodes = better bin-packing, less per-node overhead, larger blast radius; many small = the opposite
- Minikube: no autoscaling; `minikube start --nodes 2` is the way to test multi-node behaviour

Speaker notes: Spark is the classic consumer of node autoscaling: executor pods go Pending, nodes are added, and removed after the job. On one laptop the equivalent control is a namespace quota.

---
# Slide 46 — Writing and publishing a Helm chart (Q5, Q25)
- Layout: `Chart.yaml` (name, version, appVersion) · `values.yaml` + `values.schema.json` · `templates/` (Deployment, Service, ConfigMap, HPA, `_helpers.tpl`, `NOTES.txt`) · `crds/` · `charts/` (dependencies)
- Standard practice: values are the only interface; image by digest; resources and probes in values; schema validation; `helm lint` + `helm template` in CI; chart version bumped on every change
- Example for a model-serving setup: one chart, values = model image digest, model URI (s3://…), replicas, resources, HPA thresholds; one values file per environment
- Release = one installed instance; each upgrade = revision; stored as Secrets `sh.helm.release.v1.<name>.v<n>` in the release namespace
- Publishing: `helm package` → static repo (`index.yaml`, e.g. GitHub Pages: that is `kubeflow.github.io/spark-operator`) or OCI registry (`helm push … oci://ghcr.io/<user>/charts`)
- "Airflow chart" = the Apache Airflow project's Helm chart (1.22.0); Spark Operator chart = Kubeflow's chart that installs the controller, webhook, CRDs and RBAC. Both are just Helm charts
- Airflow is not "run by Helm": Helm renders and applies the objects once; Kubernetes runs them

Speaker notes: Helm tracks versions of configuration. It does not track the software's runtime; that is the controllers' job.

---
# Slide 47 — Helm in CI/CD, values, rollback, Argo CD (Q7, Q8, Q29)
- CI: `helm lint`, `helm template | kubeconform`, `helm diff upgrade` in the PR; deploy job runs `helm upgrade --install --wait` with the reviewed values file
- Revisions: `helm history airflow` (ours shows `git <sha>` per revision); `helm rollback airflow <rev>` creates a new revision with old values; `--history-max` caps stored revisions
- Values on upgrade: `--reuse-values` merges with the last release's values (hidden drift); `--reset-values` starts from chart defaults; `--reset-then-reuse-values` (Helm ≥ 3.14) = new chart defaults + previous overrides. Ours: always `-f values-local.yaml` + `--set` → fully reproducible from Git
- Rollback on failure: `--atomic` (Helm 3) is renamed `--rollback-on-failure` in Helm 4 (Nov 2025; old name still works with a deprecation warning, a later minor turns it into an error; `--force` → `--force-replace`). Helm 3 security patches end 2027-02-10. CRDs in `crds/` are never upgraded or deleted by Helm
- Argo CD (GitOps): a controller in the cluster watches a Git path or chart + values, diffs live vs Git, syncs, prunes and reverts manual drift; rollback = git revert. It renders charts with `helm template`, so there is no Helm release history
- For us: the digest bump PR already is the GitOps change; Argo CD would replace `make airflow-install` and would have reverted the drift behind E54

Speaker notes: The rule I follow already matches GitOps: only merged main is deployed and the release records the SHA. Argo CD would make the cluster pull instead of me pushing.

---
# Slide 48 — Spark memory model and Tungsten (Q10)
- Executor heap (`spark.executor.memory`):
  - Reserved: 300 MB fixed
  - Unified memory = (heap − 300 MB) × `spark.memory.fraction` 0.6: execution (shuffle, join, sort, agg buffers) + storage (cache, broadcast); soft boundary 50 % (`storageFraction`), execution can evict cached blocks
  - User memory: the remaining 40 %: UDF objects, user data structures, Spark internal metadata
  - Ours, 768 m executor: unified (768 − 300) × 0.6 ≈ 281 MB, user ≈ 187 MB
- Outside the heap: `memoryOverhead` = max(384 MiB, factor × heap); factor 0.1 for JVM jobs, 0.4 for PySpark on K8s; plus optional off-heap (`spark.memory.offHeap`)
- Tungsten: rows as compact binary (UnsafeRow) managed by Spark, not Java objects → less GC, exact size accounting
  - Cache-aware sort: sort arrays of (key prefix, pointer) that stay in CPU cache instead of chasing objects
  - Whole-stage code generation (last phase of Catalyst): fuses a stage's operators into one Java loop
- Columnar: Parquet stores each column contiguously → read only needed columns, compress well, sequential memory access the CPU can prefetch; vectorized reader works on column batches

Speaker notes: Spark's memory settings only govern the heap; Kubernetes governs the whole container. Sizing needs both numbers.

---
# Slide 49 — How the driver creates executor pods (Q11, Q6)
- The API is real HTTPS REST: driver uses the fabric8 Kubernetes client against `https://kubernetes.default.svc`
- Identity: ServiceAccount `spark-operator-spark`; token + CA mounted at `/var/run/secrets/kubernetes.io/serviceaccount/`
- Authorization: Role in spark-jobs (pods create/get/list/watch/delete, services, configmaps, PVCs) + RoleBinding to that ServiceAccount; nothing outside the namespace
- Flow: driver POSTs executor pods → watches them → executors connect back through a headless driver Service → driver assigns tasks → at the end deletes executors; executors carry an ownerReference to the driver pod so garbage collection removes them if the driver dies
- CRD registration: the chart applies the CustomResourceDefinition → API server serves `/apis/sparkoperator.k8s.io/v1beta2/.../sparkapplications`, validates with its OpenAPI schema, stores in etcd
- Any client with RBAC can use it: Airflow (our Role), kubectl, Argo Workflows; ScheduledSparkApplication (same CRD group) adds cron inside the operator
- Three different SAs, three Roles: Airflow creates SparkApplications; the operator creates drivers; the driver creates executors

Visual: Airflow SA → SparkApplication → operator SA → driver pod → driver SA → executor pods; each arrow labelled with its RoleBinding.

Speaker notes: `kubectl get --raw /api/v1/namespaces/spark-jobs/pods` makes the same call the driver makes; that is a good way to show the API is just HTTP.

---
# Slide 50 — Spark Operator, history and observability (Q6, Q23, Q27, Q28)
- History: Kubeflow started at Google (2017) to run ML jobs on Kubernetes; the Spark Operator began as GoogleCloudPlatform/spark-on-k8s-operator and moved to the Kubeflow organization in 2024. Apache also started its own spark-kubernetes-operator (0.x)
- Dependencies: the operator runs its own spark-submit (3.5.5 in 2.2.1); the application image only needs a compatible Spark + K8s entrypoint (ours vnpt-lake-spark-job). Airflow is optional: any client creating the CR works
- Where history lives (Helm does not keep runs):

| What | Where | Lifetime |
|---|---|---|
| Desired job + state | SparkApplication `.status` | until deleted / timeToLiveSeconds |
| Driver output | Completed driver pod log | until the pod is cleaned |
| Kubernetes events | Event objects | ~1 h default |
| Task attempts | Airflow metadata DB + s3://airflow-logs | permanent |
| Spark stages/tasks | event log → History Server | not set up (gap) |

- How Airflow knows what happens: SparkKubernetesOperator creates the CR, polls its status, follows the driver pod log into the task log, and on restart re-finds the pod by labels (reattach). In deferrable mode the waiting moves to the triggerer (check support in provider 10.22.0 before relying on it)
- Spark UI: live on driver port 4040 → `kubectl -n spark-jobs port-forward pod/<driver> 4040:4040`; after the job: History Server reading `spark.eventLog.dir=s3a://…`

Speaker notes: Airflow keeps the "what and when", Spark's event log keeps the "how". We only have the first today.

---
# Slide 51 — Fault tolerance, spill, sizing and cost (Q22, Q47)
- Spark: task retry (`spark.task.maxFailures` 4) → stage retry on lost shuffle → recompute from lineage; executor decommissioning moves blocks before a planned loss; the driver is a single point of failure (Airflow retries it)
- MinIO: erasure coding spreads data + parity over drives/nodes and survives losing up to the parity count; bitrot check and healing; bucket/site replication. Ours: 1 node, 1 drive → no redundancy (lab only)
- Spill: when execution memory is full, sort/agg/shuffle buffers are written to local dirs and merged → protects the JVM heap, costs disk I/O. Does not protect the container if local dirs are tmpfs or if memory is outside the heap
- Cleanup: shuffle files go with the application; local dirs are emptyDir, deleted with the pod; operator TTL removes old SparkApplications; failed Iceberg writes leave orphan files → `remove_orphan_files`
- Static sizing breaks on skew: 07-03 has 2 rows, 07-24 has 12,970; one size either wastes or OOMs. Dynamic allocation on K8s needs `shuffleTracking` (no external shuffle service)
- Cutting Spark-on-K8s cost (the "up to 80 %" figure is a best case from combining these; the saving depends on how wasteful the start was; Notion reports 60–90 % on Spark compute):
  1. Right-size requests from measured usage (requests are what you pay for)
  2. Spot/preemptible nodes for executors, on-demand for drivers; graceful decommissioning
  3. Scale to zero: dynamic allocation + node autoscaling (Karpenter consolidation / bin-packing)
  4. Separate node pools per workload, ARM instances where images allow
  5. Cheaper data: compaction + partition pruning + columnar formats read less; schedule heavy jobs off-peak

Speaker notes: Cost and reliability pull in opposite directions on Spark: spot nodes are cheap, but losing many executors at once loses shuffle data. Notion's answer was a per-job spot/on-demand ratio.

---
# Slide 52 — Joins, engines and two meanings of "DAG" (Q49, Q18, Q25)
| | Broadcast hash join | Sort-merge join |
|---|---|---|
| When | one side small (< `autoBroadcastJoinThreshold`, 10 MB default) | both sides large |
| How | small side sent to every executor, hash table in memory | shuffle both sides by key, sort, merge |
| Cost | no shuffle of the big side; memory on driver + every executor | network + disk; spills safely |
| Risk | OOM if the "small" side is not small | skew on hot keys (AQE splits skewed partitions) |

| | Spark | Trino |
|---|---|---|
| Model | stages split at shuffles, shuffle written to disk | MPP, pipelined in memory between stages |
| Strength | ETL, long jobs, Python, streaming | interactive SQL, many users, low latency |
| Failure | retries tasks/stages | query fails unless fault-tolerant execution (spooling) is on |
| Lab role | writes Bronze/Silver | queries Iceberg (Task 3) |

- Spark DAG: the plan of one application (stages, tasks), lives for seconds/minutes, recovered by lineage
- Airflow DAG: the workflow across systems and days, state in the metadata DB, retried per task. Our Airflow task `bronze` contains one Spark application, which has its own DAG

Speaker notes: AQE can switch a sort-merge join to broadcast at runtime when the real size turns out small.

---
# Slide 53 — Batch and stream into Iceberg (Q30)
| | Batch (ours) | Structured Streaming |
|---|---|---|
| Unit | one day = one run = one partition overwrite | one micro-batch = one Iceberg commit |
| Runs as | SparkApplication per run, started by Airflow | long-running SparkApplication (restart policy OnFailure) |
| Exactly-once | idempotent overwrite + retry | checkpoint + atomic Iceberg commit |
| Table impact | ~1 snapshot/day | trigger 1 min → ~1,440 snapshots/day, many small files |
| Airflow's role | the scheduler | not the runner: maintenance, health checks, downstream batch via Assets |

- Streaming makes maintenance mandatory and frequent (compaction, expiry)
- Choose by latency need; our consumers need data by early morning → batch is enough

Speaker notes: A stream into Iceberg works, but every commit is a snapshot, so the maintenance DAG becomes part of the streaming design, not an extra.

---
# Slide 54 — Iceberg commit anatomy; COW vs MOR; schema evolution (Q13)
- A commit never edits a file. It writes: new data files → new manifest(s) only for changed files (old manifests reused) → new manifest list (the snapshot) → new metadata.json → catalog swaps one pointer (compare-and-swap with the expected old location)
- Why the root is easy and data is not: data = many large immutable files; root = one pointer, one atomic operation. Readers see old or new table, never a mix
- Copy-on-write: update/delete rewrites affected data files → fast reads, slower writes; fits batch overwrite (ours replaces whole partitions anyway)
- Merge-on-read: writes delete files (position deletes; deletion vectors in format v3) → fast writes, reads merge, needs compaction; fits frequent row-level updates/CDC. Set per operation: `write.delete.mode`, `write.update.mode`, `write.merge.mode`
- Schema evolution by column ID, not by name or position: add, drop, rename, reorder, widen (int→long, float→double, decimal precision) are metadata-only; old files read missing columns as null
- Partition evolution: new spec applies to new data; old files keep the old spec, queries handle both
- Challenges: incompatible type changes need a rewrite; readers on old engine/HMS versions; downstream jobs that select by position. Bronze keeps raw strings → least exposed

Speaker notes: This is why drill 7 worked: the newest metadata.json fully describes the table, so re-pointing the catalog restores everything.

---
# Slide 55 — What the catalog really does; embedded vs external (Q16, Q20)
- Old Hive-format tables: directory = table, folder = partition, the metastore lists partitions → no atomic multi-partition change, slow S3 listing at scale, users must filter on physical partition columns, `MSCK REPAIR` and small-file chores
- Iceberg moves table state into the metadata tree: file lists + column statistics (no listing), hidden partitioning (`days(ts)`), snapshots. The catalog keeps only: name → current metadata.json, changed by CAS
- So the catalog provides the transactional guarantee by being the single atomic pointer; performance and partitioning come from Iceberg metadata; operations become table maintenance
- External vs embedded = at which level an engine reaches the data:

| | External (ours) | Embedded |
|---|---|---|
| Shape | Spark → HMS (Thrift) + Postgres for metadata; Spark → MinIO (S3) separately for files | the object store itself serves the Iceberg REST catalog: one endpoint for both storage (S3) and catalog |
| Examples | HMS, Polaris/Lakekeeper over any S3 | MinIO AIStor Tables (GA Feb 2026), AWS S3 Tables |
| Pros | vendor-neutral, any storage, mature | one system, one auth model, no catalog DB to run or back up |
| Cons | two systems to operate and back up (drills 6–7) | tied to that storage product; AIStor is commercial |

Speaker notes: Our drills 6 and 7 are exactly the cost of the external model: the catalog database is a second thing to lose.

---
# Slide 56 — Catalog choices and trade-off criteria (Q17)
| Catalog | Commit mechanism | Strengths | Weaknesses | Fits |
|---|---|---|---|---|
| Hadoop | atomic rename of metadata file | no service | unsafe on S3 (no atomic rename) | local tests only |
| Hive (ours) | lock + CAS on `metadata_location` | universal engine support | HMS + DB to run; Thrift; version pins (HMS 3.1.3 / Hive 2.3 client) | existing Hadoop shops |
| JDBC | UPDATE … WHERE old pointer | just a database | per-engine config, no REST features | small teams |
| REST (spec) | server-side commit | any engine, credential vending, server-side features | needs a server implementation | the standard target |
| Nessie | Git-like refs | branches/tags across tables, multi-table commits | extra concepts | data-as-code workflows |
| Apache Polaris | REST | top-level Apache project (Feb 2026), RBAC, vended credentials, multi-engine | Java service + DB | vendor-neutral REST catalog |
| Lakekeeper | REST | Rust, light, fine-grained authz, change events | smaller community | lightweight self-hosted REST |
| Apache Gravitino | REST + federation | one view over many catalogs (Hive, Iceberg, JDBC, Kafka, files) | broader and heavier | many metadata sources |
| Snowflake Horizon | managed REST (Apache Polaris integrated) | external engines (Spark, Trino) read and write Snowflake-managed Iceberg v2/v3 tables; vended credentials | no tags/branches or table registration from external engines; S3-compatible non-AWS storage (e.g. MinIO) not supported; API calls billed | Snowflake-centric teams |

- Criteria: commit safety on object storage · multi-engine (REST) · multi-table transactions · governance and credential vending · ops footprint (RAM, DB, backups) · lock-in

Speaker notes: For a Spark + Trino lake the decision is really "HMS, which both already speak, vs a REST catalog, which is where the ecosystem is going".

---
# Slide 57 — Snapshots: time travel, tags, branches, recovery window (Q15, Q50)
- Time travel: `SELECT … FROM t VERSION AS OF <snapshot_id>` or `TIMESTAMP AS OF '…'`; inspect with `t.snapshots`, `t.history`, `t.files`; rollback with `CALL lake.system.rollback_to_snapshot(...)`
- Uses: audit "what did the table look like at 08:00", reproduce a model's training data, undo a bad load
- Challenges: only as far as retention (our oldest snapshot gone after maintenance, by design); old snapshots keep their files → storage cost; schema at that snapshot is used; reads of very old snapshots can be slow before compaction
- Tag: named, immutable pointer to a snapshot with its own retention (`max-ref-age`); expire_snapshots keeps it → e.g. month-end release
- Branch: movable named line of snapshots → Write-Audit-Publish: write to `audit` branch, validate, fast-forward `main` (our open gap, slide 32)
- Recovery window = snapshot retention (7 days, keep ≥ 5) for logical mistakes + catalog backup (daily, RPO ≤ 24 h) + `register_table` from the newest metadata.json for catalog loss; files deleted by maintenance are not recoverable from Iceberg itself

Speaker notes: Retention is a business number: how far back must we be able to undo? Today it is 7 days because nobody asked; it should be agreed.

---
# Slide 58 — Maintenance: the 4 operations, automation, compaction (Q47, Q48)
| Operation | Pros | Cons |
|---|---|---|
| rewrite_data_files (compaction) | fewer, larger files → faster scans | heavy Spark job; conflicts with concurrent writes |
| expire_snapshots | removes old metadata and unreferenced files | ends time travel for those snapshots |
| remove_orphan_files | frees files no snapshot references (failed writes) | dangerous if too recent (24 h guard, E46); lists storage |
| rewrite_manifests | faster planning (fewer, clustered manifests) | one more commit/snapshot |

- Compaction strategies: binpack (just merge files, cheapest) · sort (cluster rows by columns → better pruning, needs shuffle) · z-order (multi-column clustering, most expensive)
- Keeping compaction from OOMing executors: limit `max-file-group-size-bytes` and `max-concurrent-file-group-rewrites`, enable `partial-progress`, compact by partition filter (recent days only), run it as its own SparkApplication with its own sizing, not concurrently with loads

| Automation | Pros | Cons |
|---|---|---|
| Manual (`make maintenance`) | full control | forgotten |
| Scheduled (ours, Sunday 02:00) | simple, predictable | runs even if nothing changed; late if tables churn fast |
| Event-driven (thresholds on file count, snapshot count, small-file ratio from metadata tables) | runs when needed | more logic to own |
| Managed / autonomous (S3 Tables, vendor catalogs) | no ops | opaque decisions, cost, lock-in |

Speaker notes: With one load per day, weekly scheduling is right. A stream or many small commits would push us to event-driven triggers.

---
# Slide 59 — Airflow 3 roles: dag-processor, triggerer, deferral, JWT (Q35, Q36, Q37)
- dag-processor (mandatory separate process in Airflow 3): runs DAG files, stores serialized DAGs. The scheduler never executes user code; it reads serialized DAGs and decides what runs
- Scheduler, triggerer, dag-processor write heartbeats to the `job` table; the scheduler's liveness probe is `airflow jobs check --job-type SchedulerJob --local` (seen in our pod)
- Deferral (true in Airflow 3): worker runs the task → task raises defer(trigger) → state `deferred`, worker slot freed → triggerer runs the async trigger with many others in one event loop → event fires → scheduler queues the task again → a worker resumes it
- Advantage: waiting costs a coroutine instead of a worker process (~263 MiB each, measured in E52). Our DeadlineAlert callback already runs in the triggerer
- JWT: every task talks to the Task Execution API with a JWT issued for that task instance, signed with one shared secret (our AIRFLOW_JWT_SECRET). Mismatched secrets across pods → tasks cannot report state. It is not a memory cost: memory grows with worker processes (`parallelism`), not tokens
- SubDAGs removed in Airflow 3: a DAG inside an operator held a worker slot, deadlocked pools, scheduled separately
  - TaskGroups: grouping inside one DAG (UI and structure only)
  - Assets (formerly Datasets): a DAG runs when another DAG updates an asset → data-aware cross-DAG scheduling
- Separate in production: api-server (UI + task API), scheduler (≥ 2 for HA), dag-processor (scale with file count), triggerer (HA), workers (by executor)

Speaker notes: The Airflow 3 split moves user code away from the scheduler and the database. That improves security and stability, and it is why our tasks needed the JWT secret.

---
# Slide 60 — LocalExecutor and scaling executors (Q40, Q42)
- LocalExecutor runs inside the scheduler process: it forks `parallelism` worker processes (`airflow worker -- LocalExecutor`) in the scheduler container; each runs a task supervisor + the task
- Measured: 4 workers × ~263 MiB idle → parallelism 2 (E52). parallelism also caps concurrent tasks globally

| Executor | Where tasks run | Good for | Cost |
|---|---|---|---|
| Local | processes in the scheduler pod | small installs, tasks that only orchestrate | one machine; scheduler pod = worker |
| Celery | long-lived worker pods via a broker (Redis/RabbitMQ) | many short tasks, low latency | broker + always-on workers |
| Kubernetes | one pod per task | isolation, per-task resources, scale to zero | pod start latency, API server load |
| Multiple executors | per task choice | mixed workloads | more config |

- Bottlenecks at scale: metadata DB connections (PgBouncer) · DAG parsing (dag-processor replicas, file count, light top-level code) · scheduler loop (several schedulers with row locks) · logs (remote logging, done) · K8s API pressure with KubernetesExecutor · broker for Celery

Speaker notes: LocalExecutor stays right for us because the heavy work is in Spark pods. With deferrable waits it could also run more Spark jobs at once without more worker processes.

---
# Slide 61 — Timetables, catchup, current_date, backfill and clear (Q33, Q34)
- CronDataIntervalTimetable("0 0 * * *"): run D covers [D, D+1), starts after D+1 00:00. `ds` = D; the run_id is named after `run_after` = D+1 (seen 10-27: `backfill__2024-07-25` loaded 07-24)
- `catchup=False` (Airflow 3 default): unpausing does not create every run since `start_date`; history is filled on purpose with `airflow backfill create` (throttled with `--max-active-runs`, `--reprocess-behavior none|failed|completed`)
- `start_date` fixed: it anchors the first interval; a moving start date makes runs appear and disappear
- Lakehouse history: idempotent partition overwrite makes backfill a normal operation, not a migration
- Clear vs trigger: a (DAG, logical date) pair exists once (E49) → rerun a day with `airflow tasks clear chatbot_bronze -s D -e D` or Clear in the UI; same run id, new try number
- `current_date` / `now()` inside a job: wrong for backfill and retries (2024-07-24 processed in 2026); the job takes the day from `ds` only
- Trigger vs interval matters: trigger timetables give zero-length intervals (E38); a manual run may have no logical date (E47); a job reading "yesterday" relative to the run time would be off by one day

Speaker notes: The interval is the contract; the trigger time is only when the work happens. Mixing them up gives correct-looking runs that processed the wrong day.

---
# Slide 62 — Jinja and the DAG definition file (Q12)
- DAG file = Python executed by the dag-processor on every parse (default every ~30 s): top-level code runs each time → keep it cheap, no network/DB calls
- Templates are rendered at task run time on the worker, only in the operator's `template_fields`; files with `template_ext` (our `.yaml` application_file) are read and rendered whole
- Context: `{{ ds }}`, `{{ data_interval_start }}`, `{{ params.x }}`, `{{ var.value.spark_job_image }}`, `{{ conn.x }}`, macros
- Strict undefined: a missing variable fails the task (E47: no `ds` in a run without a logical date)
- Check before deploy: `airflow tasks render <dag> <task> <date>`; our CI renders every template with and without a logical date
- Pitfall found by CI: after DagBag loads, `application_file` already holds the file content, not the path

Speaker notes: Two different times: parse time (every 30 s, no context) and run time (once per task, with context). Jinja belongs only to run time.

---
# Slide 63 — DataVidhya's 15 production DAG rules vs chatbot_bronze (Q14)
| # | Rule | Ours | Note |
|---|---|---|---|
| 1 | Idempotent tasks | ✓ | partition overwrite |
| 2 | Atomic tasks | ✓ | check / load / validate / notify; one Iceberg commit |
| 3 | Simple DAGs | ✓ | 4 tasks; maintenance is its own DAG |
| 4 | No heavy processing in operators | ✓ | Spark pods do the work |
| 5 | Connections, no hardcoded credentials | ✓ | `AIRFLOW_CONN_*` from Secrets |
| 6 | Always retries + delay | partly, on purpose | 0 for deterministic checks (E47 lesson); no exponential backoff |
| 7 | execution_timeout | ✓ | 20 min |
| 8 | Tags | ✓ | `["task2", "lake", "bronze"]`; CI requires tags |
| 9 | catchup=False | ✓ | default in Airflow 3; backfill explicit |
| 10 | Static start_date | ✓ | timezone-aware, checked in CI |
| 11 | TaskFlow for Python tasks | ✓ | check_landing, notify |
| 12 | Fast parsing | ✓ | checked in CI DAG integrity |
| 13 | Test before deploy | ✓ | import + rules + template render in CI |
| 14 | Meaningful names | ✓ | |
| 15 | doc_md | ✗ | module docstring only; `doc_md` not set (small fix, Task 3) |

- What the list misses (learned in Task 2): use a data-interval timetable (E38); separate deterministic from transient failures; `max_active_runs` for runs sharing a table; failure + deadline alerts; data validation as a task; acceptance from zero
- One dated detail: its idempotency example uses `execution_date`, removed in Airflow 3 → `logical_date` / `ds`

Speaker notes: The rules are a good checklist; the gaps are exactly the Airflow 3 and operations lessons from this task.

---
# Slide 64 — Process-level and write-level deduplication; data audit (Q38, Q39)
- Process-level deduplication: running the task again leaves the same state (no double side effects: emails, external calls). Write-level deduplication: the write itself cannot create duplicates
  - Partition overwrite (ours): replace exactly D's partition; needs partition grain = run grain
  - MERGE/upsert on a key: idempotent if the source is deduplicated and deterministic; handles late updates
  - Windowed: recompute the last N days every run and overwrite them; absorbs late data, costs N× compute
- Partition deleted by hand after a successful run: Airflow does not notice. Run state records that the task ran, not that data exists. Detection needs a check that runs later (audit job, Asset checks, scheduled reconcile); repair = clear the run → idempotent rerun restores it

| Principle | In our pipeline |
|---|---|
| Reframe: "produce the state of interval D", not "append new rows" | load reads only dt=D |
| Idempotency | overwritePartitions |
| Scope: what one run owns | one partition of two tables |
| Isolation and throttle | max_active_runs 1, backfill `--max-active-runs 1` |
| Seams and reconcile: check counts where data crosses systems | landing count = rows read = rows written = expected |
| Resumability: restart from the failed step | clear failed task only; Iceberg commit is all-or-nothing |
| Audit trail | `_ingest_date`, `_batch_id`, Iceberg snapshot summary, Airflow run/try logs in S3 |

Speaker notes: Airflow is the record of execution; Iceberg snapshots are the record of data changes. An audit needs both.

---
# Slide 65 — Getting DAGs into Airflow; private repos (Q26)
| | Baked into the image (ours) | git-sync sidecar | DAG bundles (Airflow 3) |
|---|---|---|---|
| Change | build + digest bump PR | push to branch | versioned bundle (e.g. Git) |
| Reproducible | exactly the reviewed digest | whatever the branch says now | version recorded per run |
| Speed | minutes | seconds | seconds |

- Private repo with git-sync: generate an SSH key pair; public key → repo Settings → Deploy keys (read-only); private key → Kubernetes Secret referenced by the chart's `dags.gitSync.sshKeySecret`; pin `known_hosts`
- Ours needs no Git credentials in the cluster: CI builds DAGs into the image and only a read-only registry token is stored

Speaker notes: Baked images trade deploy speed for certainty. For a daily batch pipeline that is the right trade.

---
# Slide 66 — Airflow 2 → 3 upgrade (Q24, Q41)
1. Prepare: be on 2.7+ (ideally latest 2.x), supported Python
2. Clean and back up: `airflow db clean` trims old XCom/logs so schema migration is fast; back up the metadata DB with Airflow stopped (consistent backup); a failed migration without backup leaves a half-migrated DB
3. DAG authors (people writing DAG code): `ruff check dags/ --select AIR301,AIR302` (breaking: AIR301 = removed in 3.0, AIR302 = moved to provider packages) and AIR311/AIR312 (deprecated, still work through a compatibility layer), `--fix`; move imports to `airflow.sdk`; no direct DB access in tasks
4. Deployment manager (platform owner): new image + chart → `airflow db migrate` (the chart's migration Job, seen in our install) → start api-server (replaces webserver) and dag-processor separately; update config (`airflow config update`)
- Behaviour changes that bit us: timetable defaults (E38), no logical date for manual runs (E47), DagBag API (E34), SLA → DeadlineAlert
- Automate: CI runs ruff AIR rules + DAG import + template render inside the new image (our DAG integrity job), then a staging install from a DB copy before production

Speaker notes: Our CI already does step 3 continuously, so a future 3.x → 4 upgrade would start from a tested baseline.

---
# Slide 67 — Drills on a data pipeline (Q9)
- Method: hypothesis → inject one fault → observe (recovery time, data correctness, alert) → fix → keep it as a `make drill-*` target

| Layer | Done in Task 2 | Worth adding |
|---|---|---|
| Compute | 1 kill driver, 2 delete executor | node pressure / eviction mid-job |
| Orchestrator | 3 scheduler restart (E41 found) | metadata DB restart |
| Data | 4 backfill twice | malformed file, new column, late file (rerun day), half export |
| Alerts | 5 failure + deadline | alert channel down |
| Catalog | 6 lose catalog, 7 newer commit than backup | HMS down during commit |
| Platform | from-zero rebuild (found E51, E52); config drift by a non-kustomize apply (E54, unplanned) | expired GHCR token, disk full, MinIO down mid-write |

Speaker notes: Every drill that failed taught more than every one that passed. The next useful ones are data faults, because so far all drills broke infrastructure.

---
# Slide 68 — Should we change the strategy? (1) the cluster (Q52)
| | docker driver (ours) | VM driver | Production cluster |
|---|---|---|---|
| Node | container sharing the WSL2 kernel | own VM, own kernel | VMs/bare metal, several nodes |
| Memory accounting | wrong until fixed (E25) | real | real |
| Start / RAM cost | fast, light | slower, RAM split from WSL | n/a |
| Failure realism | node container stops on sleep (E40) | closer to a real node | HA control plane, node loss handled |
| Storage | hostPath in the container | hostPath in the VM | network volumes |

- Recommendation: keep the docker driver for the lab (E25 is fixed by config; switching costs time and RAM on a 16 GB laptop). Run the VM driver once as a comparison experiment, and try `--nodes 2` to see scheduling and storage behaviour across nodes. Not now

Speaker notes: The main lesson of the comparison is not "VMs are better" but "know what the kubelet believes about the node", which E25 already taught.

---
# Slide 69 — Should we change the strategy? (2) resource management (Q52)
- Problem: a limit is also a failure source. E52 was not a leak; the limit was guessed and the process count was not measured
- Proposed strategy:
  1. Measure before setting: usage over a full backfill (`kubectl top`) + processes inside the container (psutil) → requests = p95, per component
  2. Derive limits from structure, not guesses: scheduler = workers × ~263 MiB + scheduler 194 + serve-logs 152 + one running task (~300) → 1.5 Gi
  3. Per workload type: Spark (heap fixed by `-Xmx`) → request = limit, overhead sized for Python; multi-process Python (Airflow) → limit above request with measured margin; no CPU limits, CPU requests only
  4. Protect the node, not each guess: namespace ResourceQuota on spark-jobs, Allocatable fixed (E25)
  5. Detect: restarts and OOMKilled fail the acceptance test (done) and should alert like a task failure
  6. Revisit when parallelism, data volume or images change; VPA in recommend-only mode on a real cluster
- Why yes: cheap, uses tools we have, prevents the next E52. Why not more: removing limits on one node risks the control plane; autoscaling needs a real cluster

Speaker notes: My answer to "should we change the strategy" is: keep the platform, change how numbers are chosen, from estimates to measurements with a test that fails when they are wrong.

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

---
# Appendix A3 — Sources for Part 5
- DataVidhya, "Best Practices: 15 Rules Every Production DAG Should Follow": https://datavidhya.com/learn/production-airflow/production/best-practices/
- Apache Airflow, "Upgrading to Airflow 3": https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- Kubernetes blog, "In-Place Pod Resize Graduates to Stable" (v1.35): https://kubernetes.io/blog/2025/12/19/kubernetes-v1-35-in-place-pod-resize-ga/
- Kubernetes blog, "The Shift to cgroup v2 in Kubernetes" (memory.oom.group): https://kubernetes.io/blog/2026/10/06/kubernetes-cgroups-v2-shift/
- Notion, "Balancing cost and reliability for Spark on Kubernetes": https://www.notion.com/blog/balancing-cost-and-reliability-for-spark-on-kubernetes
- Apache Polaris graduation (Feb 2026): https://polaris.apache.org/blog/2026/02/19/apache-polaris-graduates-to-top-level-project/
- MinIO AIStor Tables GA (Feb 2026): https://www.businesswire.com/news/home/20260203064684/en
- Snowflake, "Access Apache Iceberg tables with an external engine through Snowflake Horizon Catalog": https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon
- Helm 4 migration guide (flag renames, Helm 3 EOL): https://dev.to/amareswer/helm-4-migration-guide-what-breaks-and-how-to-fix-it-before-eol-4p99
- Ruff AIR rule numbering (AIR301/302/311/312): https://gist.github.com/Dev-iL/3ee77257fff578b884a0f7492d082093
- Project evidence: `docs/task2/08_study_notes.md` (Q1–Q52), `05_errors_and_fixes.md` (E25–E55), `04_process.md`, `06_validation.md`
