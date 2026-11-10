# Task 3 — Config: versions, RAM, disk

## 1. Versions (re-check before installing; one change at a time)

| Component | Pin | Check command (Bee) |
|---|---|---|
| Trino | **483**; image `trinodb/trino:483@sha256:db58cc93e593…0534` (multi-arch index, read from Docker Hub 2026-11-10) | `docker buildx imagetools inspect trinodb/trino:483` |
| Trino Helm chart | **1.44.0** (appVersion 483; 1.43.0 also 483, 1.45.0 = 484), GitHub Releases asset, SHA-256 `ce3c2f65…6caa` = asset digest = index digest; cached by `make trino-chart` (E58) | chart index read 2026-11-10 |
| Silver job | same image `vnpt-lake-spark-job` (Spark 3.5.9, Iceberg 1.11.0), new job file → new git-SHA tag by CI | `infra/k8s/images.env` |
| Airflow, provider | unchanged (3.3.2, cncf.kubernetes 10.22.0). Trino provider is **not** in the image → only if 3.6 is done | `pip show apache-airflow-providers-trino` in the image |

## 2. RAM budget (measured base, Task 2 README "Resource footprint")

| State | Node memory **requests** | Of Allocatable 7,268 Mi |
|---|---|---|
| Idle (lake + operator + Airflow + control plane) | 4,036 Mi | 56 % |
| Bronze running (driver 2 Gi + executor 1.1 Gi) | 7,236 Mi | 32 Mi spare |
| Silver running (no pandas → driver 1 Gi: 512m heap + 384m overhead, executor 1.1 Gi) | ≈ 6,200 Mi (estimate, measure) | |
| Trino up (request = limit 2 Gi) + idle | 6,084 Mi | fits |
| Trino up + any Spark app | ≥ 8,200 Mi | ❌ Spark pod `Pending` |

**Rule: two modes, never both** (`make mode-pipeline` / `make mode-query`, step 3.3):
- *pipeline*: Trino at 0 replicas, Airflow DAG runs Spark.
- *query*: Trino at 1, no DAG run in progress (pause `chatbot_bronze` or wait for the day's run to end).

Trino values: limit = request = 2 Gi (Guaranteed QoS), `-Xmx1536m` (75 %), `query.max-memory-per-node=600MB`, `query.max-memory=600MB`, headroom default (30 % = 460 MB) → 1,060 MB < 1,536 MB.

## 3. Disk budget

Minikube (docker driver) keeps its node and images inside Docker Desktop's `docker_data.vhdx` (Task 2 `02_config.md` §3; measured node volume 8.3 GB). With the docker driver `/var` in the node is that ext4 disk, **not** a 20 GB VM disk, so do not recreate Minikube for `--disk-size` before seeing `df`.

| Item | Estimate | Measure |
|---|---|---|
| `trinodb/trino:483` in the node (containerd ≈ 1.35 × image size, Task 2) | ~2–3 GB (image ships all connectors) | `minikube ssh -- sudo crictl images \| grep trino` |
| New spark-job image tag (shared layers with the old tag) | < 50 MB | |
| Silver data | < 5 MB (≈ 32 k rows) | `$files` |

First checks: `minikube ssh -- df -h /var`, `df -h /mnt/c` (C: free). Prune after the task: old spark-job tags in the node (`minikube ssh -- sudo crictl rmi <id>`), `make ghcr-prune`.

## 4. Measured 2026-11-10 (pipeline mode, after Trino was installed)

| Measure | Value | Note |
|---|---|---|
| Node requests / usage | 4,036 Mi (55 %) / 4,324 Mi (59 %) | Trino and History Server at 0 cost nothing |
| Pods above their request | triggerer 525 Mi (request 256), dag-processor 410 Mi (256) | Burstable: first to be evicted under pressure. Raising their requests (+~400 Mi) would make a Bronze run (7,236 Mi) exceed Allocatable 7,268 Mi → kept as a known trade-off of the 8 GiB node |
| WSL VM | used 5.1 Gi, available 5.3 Gi, swap 273 Mi / 4 Gi | |
| Node `/var` | 19 G (18 G on 11-09: +1.04 GB Trino image) | |
| Docker volumes / images / build cache | 10.42 GB / 7.58 GB / 0.88 GB | volumes +1.55 GB since 11-09 |
| Old images in the node | airflow ×1, spark-job ×2 not deployed | remove at the 3.7 clean-up (shared layers: small saving) |
| **Windows C:** | **60 G free of 476 G (88 %)**; 87.2 GB free on 10-25 | ⚠ main disk risk: vhdx files grow into C: → clean-up + vhdx compaction at the end of Task 3; check before Task 4 (StarRocks +2–3 GB) |

Budget per mode (requests vs 7,268 Mi): pipeline idle 4,036 · + Bronze run 7,236 (32 Mi spare) · + Silver run ≈ 6,212 · query mode ≈ 4,452 · demo-task3 ≈ 4,964 · Task 4 StarRocks (2.5 Gi) with Trino 0 ≈ 4,964 · StarRocks + Trino ≈ 7,012 (plan separate modes).
