# Lab Progress Tracker — VNPT AI Lakehouse Lab

Owner: Bee · Started: 2026-10-14 · **Target demo: Fri 2026-10-23** (self-set)
Update this file at the end of each work block (status, decisions, blockers).

---

## 1. Fixed constraints (decided 2026-10-14)

| Item | Value |
|---|---|
| Machine | Windows 11 + WSL2 · i7-13700HX (16 cores / 24 threads) · **16 GB RAM** · RTX 4050 6 GB (not used in this lab) · **130 GB+ free on C:** |
| Where it runs | Locally only: Docker Desktop + Minikube (company device not available) |
| Code hosting | Personal GitHub (Bee will say if this changes) |
| Data | Trial chatbot data, allowed on the laptop, **must not leave the laptop** (no cloud VM, no uploads, no data contents in alerts) |
| Required tools (from lab doc) | Docker Desktop, Minikube (`--memory=7168 --cpus=4`, needs 6 GB+ RAM), Helm v3, kubectl, Python 3.10+, DBeaver or a MySQL client for StarRocks |
| Alerts | Telegram group for the fresher team |
| Coding assistant | Claude Code, with `CLAUDE.md` in the repo as the hand-over context |

---

## 2. Version matrix (verified 2026-10-14)

| Component | Choice | Status | Why / risk |
|---|---|---|---|
| Python | **3.11** | ✅ keep | PySpark 3.5 supports Python ≤ 3.11 officially. Safe for Airflow/dbt too |
| Spark / PySpark | **3.5.9**, Scala 2.12, Java 17 | ✅ keep | Release exists; 3.5 line has the most Iceberg/Operator examples |
| Iceberg (Spark runtime) | **1.11.x latest patch** (instead of 1.12.0) | ⚠️ changed | 1.12.0 was announced 2026-10-01, only 2 weeks old. It still supports Spark 3.5, but it switched the AWS SDK HTTP client (`apache-client` → `apache5-client`) and upgraded the AWS SDK and Jackson, which can cause classpath conflicts with S3/MinIO. Move to 1.12 later if you need its features |
| Iceberg tables | Format **v2**, Parquet | ✅ keep | Readable by Spark, Trino 483 and StarRocks 3.5 |
| Airflow | **3.3.2**, official Helm chart, **LocalExecutor** | ✅ keep | Heavy work runs in Spark pods anyway; LocalExecutor avoids Celery/Redis and the extra pods. Airflow 3 runs API server + scheduler + DAG processor + triggerer (≈2 GB) |
| Hive Metastore | **apache/hive 3.1.3** (changed 10-16: HMS 4 incompatible with Spark's Hive 2.3 client, E16) — was 4.0.x, `SERVICE_NAME=metastore`, Postgres backend | ✅ keep (check exact tag on Docker Hub) | Metastore only, no HiveServer2. Needs the Postgres JDBC jar mounted at `/opt/hive/lib/postgres.jar` |
| PostgreSQL | **16.x**, one instance, separate DBs (`metastore`, `airflow`, `superset`) | ✅ keep | One instance saves RAM |
| MinIO | Latest stable image, single node | ✅ | Path-style access required in Spark/Trino |
| Trino | **483** (released 2026-07-17) | ✅ keep, add in Phase 4 | Single node, small `-Xmx`. Container includes its own Java |
| StarRocks | **3.5.21** (2026-08-28), 1 FE + 1 BE | ✅ keep, add in Phase 5 | 3.5.21 fixes an intermittent Iceberg-via-Hive-Metastore query error, which is your exact setup |
| dbt | `dbt-core` + `dbt-starrocks`, lock in Phase 5 | ⏳ | Check adapter ↔ core compatibility then |
| Superset | Lock in Phase 6 | ⏳ | Connects to StarRocks via MySQL protocol |
| Spark Operator | Kubeflow Spark Operator (Helm), lock in Phase 2 | ⏳ | Must support Spark 3.5 |

**Resource settings:** `.wslconfig` → `memory=11GB`, `processors=8`, `swap=4GB`. Minikube: start with `--memory=7168 --cpus=4` (lab default). If stable, raise to `--memory=8192 --cpus=6`. CPU is not your bottleneck; RAM is.

---

## 3. Scope for the 2026-10-23 demo

The deadline is tight for "learn K8s + 8 tools". **Learn K8s just-in-time while building**, not as a separate 2-week course.

| Priority | Item |
|---|---|
| **Must** | Steps 1–5 of the lab working end-to-end on the full trial data; UC5 + UC7 + UC8 at minimum on Superset; Airflow DAG with retries; numbers reconciled with DuckDB; README + architecture diagram |
| **Should** | All UC4–UC8; dbt tests; Telegram failure alert; one failure drill (kill executor pod); daily-file incremental load |
| **Could** | Write-Audit-Publish; Iceberg maintenance DAG; CI on GitHub Actions; Prometheus/Grafana |
| **Later** | Spark-on-K8s tuning, JDBC extraction from Oracle, CDC |

---

## 4. Task status (each task = standalone mini-project)

| Task | Scope | Docs | Status |
|---|---|---|---|
| 1 | MinIO + Spark → Iceberg (Docker Compose) | `docs/task1/` | 🔄 started 10-15 |
| 2 | Minikube + Airflow running the Spark job | `docs/task2/` | ⬜ |
| 3 | Trino querying Iceberg | `docs/task3/` | ⬜ |
| 4 | StarRocks Gold via SQL/dbt | `docs/task4/` | ⬜ |
| 5 | Superset dashboard UC4–UC8 | `docs/task5/` | ⬜ |

## 4b. Day plan and status

Status: ⬜ not started · 🔄 in progress · ✅ done · ⛔ blocked

| Day | Phase | Work | Done when | Status |
|---|---|---|---|---|
| Thu 10-15 | 0 + Task 1 | Install / configure tools, `.wslconfig`; repo skeleton + `CLAUDE.md`; DuckDB expected numbers; `docs/metrics.md`; K8s basics (Pod, Deployment, Service, PVC, Secret, ConfigMap) about 2 h | Expected numbers saved (✅ `docs/task1/06_validation.md`); `kubectl get nodes` works | 🔄 |
| Fri 10-16 | Task 1 | Compose: MinIO + Postgres + Hive Metastore; Spark writes Bronze (Silver stretch) | Bronze counts = `06_validation.md`; rerun gives no duplicates | ⬜ |
| Sat–Sun 10-17/18 | 2 | Minikube: MinIO/Postgres/HMS via Helm; Spark Operator; `SparkApplication` runs the job | `make up` from zero works | ⬜ |
| Mon 10-19 | 3 | Airflow Helm + DAG (sensor → bronze → silver → dbt → checks) | Scheduled + manual runs succeed; retry works | ⬜ |
| Tue 10-20 | 4–5a | Trino on Iceberg; StarRocks FE/BE + Iceberg catalog | Trino = Spark counts; StarRocks reads Iceberg | ⬜ |
| Wed 10-21 | 5b | dbt Gold (UC models) + tests + reconciliation | `dbt build` passes; Gold = DuckDB | ⬜ |
| Thu 10-22 | 6–7 | Superset dashboard; Telegram alert; one failure drill; README | Dashboard numbers reconciled; alert received | ⬜ |
| Fri 10-23 | 9 | Rehearse; record backup video; **demo** | Demo delivered | ⬜ |

---

## 5. Decisions log

| Date | Decision | Reason |
|---|---|---|
| 10-14 | Run everything locally on Minikube | No company device |
| 10-14 | Iceberg 1.11.x instead of 1.12.0 | 1.12.0 is two weeks old and changes AWS SDK dependencies |
| 10-14 | Airflow LocalExecutor | Saves RAM; heavy work runs in Spark pods |
| 10-14 | Full trial data for the demo; small fixtures for tests | The full set is only ~32k rows across both files |
| 10-14 | Data never leaves the laptop; alerts carry metadata only | Data handling rule |
| 10-15 | Leader: tasks 1–5 are **standalone mini-projects** on the same data, each must be understood (theory, errors, fixes) and also chainable into one pipeline | Leader guidance |
| 10-15 | Task 1 runs on **Docker Compose** (MinIO + Postgres + Hive Metastore + Spark), no K8s/Airflow | Isolate Spark/Iceberg/S3 issues from K8s issues |
| 10-15 | Hive Metastore used from Task 1 (not the Hadoop catalog) | Hadoop catalog is unsafe on S3; Trino/StarRocks need HMS later |
| 10-15 | Images: `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` (fallback `-r-ubuntu`), `apache/hive:4.0.1`, Iceberg **1.11.0** | Tags checked on Docker Hub / Iceberg releases page |
| 10-15 | Per-task docs: knowledge, config, plan, process, errors, validation | Bee's request; repo `docs/task<N>/` |
| 10-15 | **Task 1 step 1.0 done**: WSL 2.6.1, Ubuntu-22.04 (default, v2), `.wslconfig` applied (10 GiB RAM, 8 CPU, 4 GiB swap), Docker Desktop 4.93 / Engine 29.8.1, WSL integration on, hello-world OK, make 4.3 / git / unzip installed | Verified by command output |
| 10-15 | Bee uses **cmd** (not PowerShell); enters Ubuntu with `wsl` for make/bash | Preference |
| 10-16 | **E01 seen**: `minio/minio` and `minio/mc` removed from Docker Hub (and quay.io); MinIO CE archived → use `cgr.dev/chainguard/minio:latest` (pin digest after first pull), bucket created with `amazon/aws-cli`; fallback `pgsty/silo` | Upstream distribution ended 2025–2026 |
| 10-16 | Verified pulls: `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` ✅, `apache/hive:4.0.1` ✅ (ships hadoop-common **3.3.6** = HADOOP_AWS_VERSION ✅) | B6 output |
| 10-16 | Stage B done: repo unzipped, data copied (2 files, 2.3 MB + 1.1 MB), `.env` ignored by `*.env` rule ✅, first commits `f2b65d7`, `698a431`. MinIO pinned: `cgr.dev/chainguard/minio@sha256:2136d593…c0c8`. Transient: `docker` not found in WSL until the shell was reopened after enabling integration | Bee output |
| 10-16 | To fix: home dir is `/home/quang` (files owned by `quang`) → `RAW_DATA_DIR` must be `$HOME/data/vnpt-lab`; git email was saved with `< >` brackets | Bee output |
| 10-16 | Stage B done: repo at `~/vnpt-lakehouse` (2 commits: f2b65d7, 698a431), data in `~/data/vnpt-lab` (2 xlsx), `.env` git-ignored ✅ (`.gitignore:2:*.env`), Chainguard MinIO digest `sha256:2136d593…fee0c8`, aws-cli + busybox pulled | B5b/B8 output |
| 10-16 | Seen: `docker` not found inside WSL right after Docker Desktop restart → reopening `wsl` fixed it (integration reloads after engine start). Seen: git email set with `<>` brackets → fix + amend | Transient / typo |
| 10-16 | Linux + Windows user is **`quang`** (Bee calls it "user"); home = `/home/quang`; `RAW_DATA_DIR=/home/quang/data/vnpt-lab` | Command output |
| 10-16 | Docker Desktop's built-in Kubernetes (kind) found enabled → **disable it**; use Minikube as the lab requires. Remove unused images (`apache/spark:latest`, `trinodb/trino:latest`, `hello-world`, kind/envoy images); keep `postgres:16`, `apache/airflow:3.3.2` | Free RAM and disk; avoid two K8s clusters |
| 10-16 | Found running leftovers: an old Airflow 3.3.2 **Compose** stack (`airflow-*`, 4 services + postgres) and a **kind** cluster `lab-control-plane` (kindest/node v1.37.0). Plan: `docker compose -p airflow down` (keep volumes), remove the kind cluster, delete exited containers | They consume several GB of RAM; Task 2 will run Airflow on Minikube via Helm instead |
| 10-16 | **Task 1 steps 1.1 done**: repo in `~/vnpt-lakehouse`, git `main`, 2 commits (`f2b65d7` starter, `698a431` MinIO switch); Excel files in `~/data/vnpt-lab`; `.env` ignored by git | Command output |
| 10-16 | Verified: `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` pulls; `apache/hive:4.0.1` ships **hadoop-common 3.3.6** → `HADOOP_AWS_VERSION=3.3.6` correct | Command output |
| 10-16 | **E01 seen**: `minio/minio` and `minio/mc` → *pull access denied* (MinIO CE images no longer on Docker Hub). Switched to `cgr.dev/chainguard/minio` (pin by digest `sha256:2136d593…`) + `amazon/aws-cli` / `busybox` for bucket init | Bee's change; compose to be reviewed before `make up` |
| 10-16 | **Stage C done**: MinIO up (Chainguard), bucket `lakehouse` created, Postgres healthy, Hive Metastore image built (38 s) and **schema 4.0.0 initialised**, server started | Command output |
| 10-16 | Finding: the Hive image entrypoint runs with `set -x`, so `SERVICE_OPTS` (incl. DB password) is printed in logs → treat logs as sensitive; later move credentials to a mounted `hive-site.xml` | Security |
| 10-16 | **E16 seen**: Spark 3.5 (Hive 2.3 client) → HMS 4.0.1 fails on table calls: `Invalid method name: 'get_table'`. Decision: **downgrade HMS to `apache/hive:3.1.3`** (tag exists), wipe `vnpt-lake_pg-data`, re-match hadoop-aws | Keeps the lab's Hive Metastore requirement; Trino/StarRocks also speak HMS 3 |
| 10-16 | HMS 3.1.3 started: schema **3.1.0** initialised; Spark sees `SHOW NAMESPACES` → `default`. **E17**: `CREATE NAMESPACE` → connection lost; cause suspected `HADOOP_AWS_VERSION=3.1.3` vs hadoop-common **3.1.0**. Fix → 3.1.0 + `IS_RESUME=true` | Command output |
| 10-16 | Bee asked if Hive is needed in Task 1 → kept: Iceberg always needs a catalog; the lab's architecture names Hive Metastore; Trino (Task 3) can't use Iceberg's Hadoop catalog | Decision |
| 10-16 | C0 cleanup done: old Airflow Compose stack down (volume kept), kind cluster `lab` removed, unused images removed. Left: 1 untagged `kindest/node` image (1.34 GB), 2 old volumes (1.4 GB) | Command output |
| 10-16 | Note: an earlier entry said repo `~/lakehouse` / user `user`; latest command output shows **`~/vnpt-lakehouse`**, project `vnpt-lake`, user `quang` → verify with `pwd`/`whoami` | Reconciled at handoff |
| 10-16 | Docker Desktop's built-in Kubernetes to be **disabled**; unused images removed (spark:latest, trino:latest, kind/envoy/registry-mirror) | Lab uses Minikube; Docker Desktop K8s wastes RAM. Keep `apache/airflow:3.3.2` (Task 2) and `postgres:16` |
| 10-16 | Claude Code session: disk cleanup done (hive 4.0.1 + airflow 3.3.2 images, build cache, old airflow volume) → ~11.9 GB freed inside Docker; vhdx compaction deferred until after E17 | `docker system df` before/after |
| 10-16 | Found: Spark image ships **Python 3.10.12** (matrix said 3.11); PySpark 3.5 + pandas 2.2.3 / pyarrow 17 support 3.10, so keep the image's Python | `python3 --version` in container |
| 10-16 | Infra option 1 chosen: official images + compose config (mounted jars/configs, one-shot init services), no custom builds. Rejected: Starburst HMS image (vendor, 15 months old, 2 changes at once), tabulario/spark-iceberg (Spark 3.5.5/Iceberg 1.8.1, 2.3 GB), Iceberg REST (lab names HMS) | Bee's choice after options with RAM/disk/risk |
| 10-16 | **E17 fixed**: root cause Guava NoSuchMethodError (hadoop-aws 3.1.3 built on Guava 27, image has Guava 11/19) → hadoop-aws 3.1.0; HMS runs on `apache/hive:3.1.3` with mounted jars; smoke test passes | HMS log + smoke test output |
| 10-16 | Spark runs on the official `apache/spark:3.5.9…` image: Iceberg via `spark.jars.packages` + Ivy volume, Python deps via `spark-init` → `pydeps` volume. **No custom images left in use** (old ones kept until bronze passes) | Smoke test on new Spark: 3 rows, 2 snapshots |

## 5b. Reading notes (Bee's ratings, 10-15)

| Source | Bee's takeaway | Gap to fill |
|---|---|---|
| Data Engineer Things: Spark on K8s + custom Airflow operator | Working Airflow → SparkApplication setup with code | No dynamic allocation, shuffle tuning, History Server, Prometheus, multi-tenancy |
| Yusuf Ganiyu: realtime Airflow/Kafka/Spark/Cassandra | Producer → Kafka → Spark → DB flow | Airflow used only as a scheduler; Cassandra less common now |
| Dev Genius: K8s probes | Liveness / readiness / startup; HTTP/TCP/exec; avoid CrashLoopBackOff | — |
| Towards AI + Civo: requests vs limits | Request = guaranteed for scheduling; limit = cap; CPU is throttled, memory over limit = OOMKilled; no requests/limits = BestEffort QoS | — |
| Big Data Performance: Spark on K8s part 1 | spark-submit vs Operator/SparkApplication | Native `spark-submit` also works without an operator; Helm is packaging, persistence comes from PVCs |
| Stackable lakehouse demo | Iceberg snapshots, time travel, rollback, compaction, hidden partitioning, branches/tags | — |
| DataAIChronicles: Airflow triggerer | Deferrable operators free worker slots while waiting | Watch RAM when increasing parallelism |

## 6. Open questions / blockers

- [ ] UC6 (top 12 intents vs only 2 intents in data) and UC7 (30-day comparison vs ~18 days of data): raise with leader
- [x] `apache/hive:4.0.1` exists (also `standalone-metastore-4.1.0` / `4.2.1`) — no longer used (E16)
- [x] Hadoop bundled in `apache/hive:3.1.3` = hadoop-common **3.1.0** → `HADOOP_AWS_VERSION=3.1.0`
- [x] `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` pulls
- [ ] Pin MinIO and aws-cli by digest (`.env` still says `:latest`)
- [ ] Confirm the Spark Operator chart version that supports Spark 3.5.9

## 6b. Errors met (live)

| Date | Symptom | Likely cause | Fix | Status |
|---|---|---|---|---|
| 10-16 | In Ubuntu: `The command 'docker' could not be found in this WSL 2 distro` (worked on 10-15) | Docker Desktop's WSL integration for Ubuntu-22.04 was switched off or not yet started again after Apply & restart / Kubernetes disable / user rename | Docker Desktop running → Settings → Resources → WSL integration → toggle Ubuntu-22.04 OFF→ON → Apply & restart → reopen the Ubuntu terminal → `which docker` | fixed (`exit` → `wsl`) |

## 7. Risks being watched

| Risk | Early sign | Response |
|---|---|---|
| RAM exhaustion | Pods `Pending` / `OOMKilled`, Windows swapping | Scale idle services to 0; lower heaps; run in phases |
| Version incompatibility | `ClassNotFoundException`, S3 auth errors | Stick to the matrix; change one version at a time |
| Schedule slip | Phase not done by its day | Cut from "Could", then "Should"; never cut reconciliation |
| Disk growth | < 30 GB free | Prune images; expire snapshots; compact the WSL2 disk |


## 7. Handoff to Claude Code (2026-10-16 13:50)

- Chat phase ended. Continue from `docs/HANDOFF.md` (sections 6–10).
- Open: **E17** (confirm in HMS log; hadoop-aws 3.1.0), infra redesign toward pulled images + compose config, disk audit (setup used 30–50 GB).
- Data reminder: trial/imaginary data, lab only; keep local.
