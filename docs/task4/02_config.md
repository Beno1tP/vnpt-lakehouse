# Task 4 — Config: versions, RAM, disk

## 1. Versions (re-check before installing; pin by digest / checksum)

| Component | Pin | Check command (Bee) |
|---|---|---|
| StarRocks FE / BE | **3.5.21** (`progress.md` §2: fixes an intermittent Iceberg-via-HMS query error); `starrocks/fe-ubuntu:3.5.21`, `starrocks/be-ubuntu:3.5.21` by digest | `docker buildx imagetools inspect starrocks/fe-ubuntu:3.5.21` |
| StarRocks Operator + chart `kube-starrocks` | input plan says v1.11.x — **not verified here**; pick the newest release whose notes support 3.5 | `helm repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator && helm search repo starrocks --versions \| head` |
| dbt-core + dbt-starrocks | newest pair where `dbt-starrocks`'s `Requires-Dist` allows that `dbt-core`; pinned in `docker/dbt/requirements.txt` (with hashes if CI allows) | `pip index versions dbt-starrocks`; `pip download dbt-starrocks==X --no-deps` → read `METADATA` |
| dbt image base | `python:3.11-slim` by digest | |
| Airflow | unchanged; `KubernetesPodOperator` is in cncf.kubernetes 10.22.0 (already in the image) | |

## 2. RAM budget (Allocatable 7,268 Mi; idle requests 4,036 Mi, Task 2)

Input plan sizes (FE 2 Gi + BE 3 Gi) give 4,036 + 5,120 = **9,156 Mi** idle: does not fit even without Spark. Smaller, Burstable sizing (real usage is small with ~32 k rows; measure and adjust):

| Pod | Request | Limit | Setting |
|---|---|---|---|
| StarRocks FE | 1 Gi | 1.5 Gi | `JAVA_OPTS -Xmx1024m` (≈ 70 % of limit) |
| StarRocks BE | 1.5 Gi | 2 Gi | `mem_limit = 1600M` (80 % of limit) |
| StarRocks operator | 128 Mi | 256 Mi | |
| dbt pod | 256 Mi | 512 Mi | runs, then exits; work runs inside StarRocks |

| Mode | Running | Requests | Fits? |
|---|---|---|---|
| pipeline | lake, operator, Airflow, Spark app; Trino 0, StarRocks 0 | ≤ 7,236 | ✅ (Task 2) |
| **gold** | lake, Airflow, StarRocks, dbt pod; no Spark | 4,036 + 2,688 + 256 = **6,980** | ✅ 288 Mi spare |
| query | lake, Airflow, Trino | 6,084 | ✅ |

FE/BE scaled to 0 keep their PVCs (data survives). Check after each install: `kubectl describe node minikube | grep -A8 "Allocated resources"`, `kubectl top pods -A`. If FE is OOMKilled at 1.5 Gi: raise FE to 2 Gi and scale the Airflow triggerer to 0 only if no deferrable task/deadline needs it (check first).

Fallback (input plan): `starrocks/allin1-ubuntu:3.5.21` as one pod. It saves the operator and one JVM's overhead, not much RAM, and is documented by StarRocks as not for production. Use only if the operator fails; record as a deviation.

## 3. Disk budget

| Item | Estimate | Measure |
|---|---|---|
| FE + BE images in the node (× ~1.35 containerd) | ~3–4 GB | `minikube ssh -- sudo crictl images` |
| Operator image | < 100 MB | |
| dbt image (slim + dbt) | ~300–400 MB | CI log |
| FE meta + BE storage PVCs | < 1 GB | `kubectl get pvc -A` |

Check `minikube ssh -- df -h /var` and C: free first. If Task 3 is done and Trino is not needed for the demo, `minikube ssh -- sudo crictl rmi` the Trino image (re-pulled on demand).
