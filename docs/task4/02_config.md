# Task 4 — Config: versions, RAM, disk

## 1. Versions (re-check before installing; pin by digest / checksum)

| Component | Pin | Check command (Bee) |
|---|---|---|
| StarRocks FE / BE | **3.5.21**: `starrocks/fe-ubuntu:3.5.21@sha256:cbc2460aa325c69c143c6497c3ae27822328aec3c74fb17525bb387a719d9fc6`, `starrocks/be-ubuntu:3.5.21@sha256:6ed9e75bfb7fd6b48947b8b75f5811a49abe3b793595e4cd4239a6dba7fcbe9f` (Bee 2026-11-10, `imagetools inspect`) | `make starrocks-render` (no `4.1-latest`, both digests rendered) |
| Operator + chart `kube-starrocks` | **1.11.8** (released 2026-10-22; operator image `starrocks/operator:v1.11.8`); chart from GitHub Releases, SHA-256 `c05804d0…2286` = asset digest; chart defaults: image tag `4.1-latest`, FE `-Xmx8192m`, FE/BE 4–8 Gi, BE storage 1 Ti → all overridden in `infra/helm/starrocks/values-local.yaml`. R10 closed by rendering, not by release notes (`CHANGELOG.md` stops at 1.9.8) | `make starrocks-chart`, `make starrocks-render` |
| dbt-core + dbt-starrocks | **dbt-starrocks 1.12.2** (2026-09-15; `dbt-core>=1.9.0`, `mysql-connector-python>=8.1`, Python <3.15) + **dbt-core 1.12.6**: the adapter sets only a floor, so CI proves the pair (`dbt parse` + `dbt debug`) when the dbt image is built (4.2) | `pip index versions …`; PyPI `requires_dist` |
| dbt image base | `python:3.11-slim` by digest | |
| Airflow | unchanged; `KubernetesPodOperator` is in cncf.kubernetes 10.22.0 (already in the image) | |

## 2. RAM budget (Allocatable 7,268 Mi; idle requests 4,036 Mi, Task 2)

Input plan sizes (FE 2 Gi + BE 3 Gi) give 4,036 + 5,120 = **9,156 Mi** idle: does not fit even without Spark. Smaller, Burstable sizing (real usage is small with ~32 k rows; measure and adjust):

| Pod | Request | Limit | Setting |
|---|---|---|---|
| StarRocks FE | 1 Gi | 1.5 Gi | `JAVA_OPTS -Xmx1024m` (≈ 70 % of limit) |
| StarRocks BE | 1 Gi (was 1.5: measured 302 Mi idle, option A 11-10) | 2 Gi | `mem_limit = 1600M` (80 % of limit) |
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

## 4. Measured 2026-11-10 (after `make starrocks-install`, 16 min, pipeline idle, Trino 0)

| Measure | Value | Note |
|---|---|---|
| Pods | operator, FE, BE: 0 restarts; init job Completed after 4 retries (FE still starting) | 4.1 "done when" met: FE LEADER alive, BE alive, 3.5.21-85fccf8 |
| Usage / request | FE 485 / 1,024 Mi · BE 302 / 1,536 Mi (13 % of `mem_limit`) · operator 19 / 128 Mi | idle, no tables yet (87 system tablets) |
| Node requests | 6,724 Mi (92 %) = 4,036 + 1,024 + 1,536 + 128; limits 11,356 Mi (156 %) | as predicted |
| Real margin | 7,268 − Σ max(request, usage) = 7,268 − (6,724 + 264 triggerer + 155 dag-processor) ≈ **125 Mi** | rule "< 200 Mi" → option A: BE request 1 Gi → ≈ 637 Mi (re-measure during `dbt build`, 4.3) |
| WSL VM | used 6,938 MB, available 4,016 MB, swap 559 MB / 4 GB | |
| Disk | node `/var` 24 G (+5 G): be-ubuntu 1.33 GB, fe-ubuntu 1.02 GB, operator 25 MB | |
| PVC size | BE reports `TotalCapacity` 1,006.854 GB, not 5 Gi | Minikube's hostpath provisioner does not enforce PVC sizes (a folder on the node disk); watch `df` instead |

Mode budget (requests, after A): pipeline 4,036 (StarRocks 0) · gold 6,212 · query 6,084 (StarRocks 0) · gold + Trino 8,260 ❌ · gold + Bronze run ≥ 9,400 ❌.
