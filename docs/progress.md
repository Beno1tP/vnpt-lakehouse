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
| Hive Metastore | **apache/hive 4.0.x**, `SERVICE_NAME=metastore`, Postgres backend | ✅ keep (check exact tag on Docker Hub) | Metastore only, no HiveServer2. Needs the Postgres JDBC jar mounted at `/opt/hive/lib/postgres.jar` |
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
- [x] `apache/hive:4.0.1` exists (also `standalone-metastore-4.1.0` / `4.2.1`)
- [ ] Confirm the Hadoop version bundled in `apache/hive:4.0.1` → set `HADOOP_AWS_VERSION`
- [ ] Confirm `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` pulls (else `-r-ubuntu`)
- [ ] Pin a MinIO image tag that pulls
- [ ] Confirm the Spark Operator chart version that supports Spark 3.5.9

## 7. Risks being watched

| Risk | Early sign | Response |
|---|---|---|
| RAM exhaustion | Pods `Pending` / `OOMKilled`, Windows swapping | Scale idle services to 0; lower heaps; run in phases |
| Version incompatibility | `ClassNotFoundException`, S3 auth errors | Stick to the matrix; change one version at a time |
| Schedule slip | Phase not done by its day | Cut from "Could", then "Should"; never cut reconciliation |
| Disk growth | < 30 GB free | Prune images; expire snapshots; compact the WSL2 disk |
