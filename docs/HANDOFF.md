# HANDOFF — VNPT AI Lakehouse Lab (chat → Claude Code)

Written 2026-10-16 13:50 (UTC+7) at the end of the claude.ai chat phase. Read this first, then `CLAUDE.md`, `docs/progress.md`, and `docs/task1/*`.

**Update 2026-10-27:** this file is the 10-16 chat → Claude Code handoff (Task 1 era). Current state: Task 1 ✅ (`task1-v3-operable`), **Task 2 ✅ (`task2-v1`, `docs/task2/README.md`)**, next Task 3 (Silver first, then Trino). Live status: `docs/progress.md` §4 and §8. Sections below are historical.

---

## 0. Ground rules (from Bee)

- **Lab work only.** The chatbot data is **trial/imaginary data** prepared for freshers, not real users, so no person can be affected. Still: keep it on the laptop, never upload it, never put row contents in alerts or public logs.
- Bee prefers **direct, concise answers with technical justification**, and challenges unsupported claims, so back claims with evidence (command output, docs, versions).
- **Step-by-step pace.** Give one stage at a time, say what to expect, ask for output (📋) before moving on.
- Shell: Bee starts in **Windows cmd** (not PowerShell) and types `wsl` to enter Ubuntu; all `make`/`bash`/`docker compose` work runs **inside Ubuntu**.
- Bee calls the Linux account "user"; **observed** account/home is `quang` / `/home/quang`. Verify with `whoami; pwd` instead of assuming.
- **New infra preferences (2026-10-16):**
  1. Prefer **ready-made images from public registries**, configured through `docker-compose.yml` (env, mounted config files, mounted jars, `--packages`), over custom-built images. Build only when unavoidable, and keep any build thin.
  2. **Disk usage matters.** The current setup has consumed **30–50 GB**. Every proposal must state its disk cost, and cleanup must be part of the workflow.

## 1. The lab

Leader's lab: an end-to-end lakehouse on chatbot logs. Tasks 1–5 are **standalone mini-projects on the same data** that can also chain into one pipeline. For each task Bee must understand what happens, which errors occur, how to fix them, and the theory behind them.

1. Excel → Spark → Iceberg tables on MinIO **(current task)**
2. Spark job on Kubernetes (Minikube) orchestrated by Airflow
3. Trino queries the Iceberg tables
4. StarRocks builds Gold tables with SQL/dbt
5. Superset dashboard for UC4–UC8

Lab tech stack names a **Hive Metastore** as the Iceberg catalog. Task 1 runs on **Docker Compose** (no K8s/Airflow yet), to keep Spark/Iceberg/S3 problems separate from K8s problems. Self-set demo target: **Fri 2026-10-23**.

## 2. Machine and environment (verified by command output)

| Item | Value |
|---|---|
| OS | Windows 11 (build 26300) + WSL 2.6.1, distro **Ubuntu-22.04** (default, v2) |
| CPU / RAM | i7-13700HX / 16 GB. `.wslconfig`: memory=11GB, processors=8, swap=4GB → WSL sees 10 GiB, 8 CPUs |
| GPU | RTX 4050 6 GB, not used in this lab |
| Disk | ~130 GB free on C: at start; **this setup now uses 30–50 GB** |
| Docker | Docker Desktop 4.93.0, Engine 29.8.1, WSL integration ON for Ubuntu-22.04. Docker Desktop's own Kubernetes **should be OFF** (lab uses Minikube) |
| Tools in Ubuntu | make 4.3, git 2.34, unzip |
| Repo | `~/vnpt-lakehouse` (observed), branch `main`, pushed to Bee's **personal GitHub**. Commits: `f2b65d7` starter, `698a431` MinIO → Chainguard |
| Data | `~/data/vnpt-lab/bot_dvc_hcm.xlsx` (2.3 MB), `~/data/vnpt-lab/bot_ddvc_hcm_bot_predict.xlsx` (1.1 MB) |
| Compose project | `vnpt-lake` (`name:` in compose). Shortcut: `DC="docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env"` |
| Secrets | `infra/compose/.env` (git-ignored, verified with `git check-ignore`) |

## 3. Current Task 1 stack (as committed + local `.env`)

| Service | Image | Notes |
|---|---|---|
| minio-perms | `busybox:1.36` | one-shot `chown 65532` on the data volume (Chainguard MinIO runs non-root) |
| minio | `cgr.dev/chainguard/minio` (pinned digest `sha256:2136d593…`) | official `minio/minio` and `minio/mc` are **no longer pullable** (E01) |
| minio-init | `amazon/aws-cli` | `aws --endpoint-url http://minio:9000 s3 mb s3://lakehouse` ✅ bucket exists |
| postgres | `postgres:16` | DB `metastore`, healthcheck `pg_isready` ✅ |
| hive-metastore | **custom build** on `apache/hive:3.1.3` + postgres JDBC 42.7.4 + `hadoop-aws` + `aws-java-sdk-bundle 1.11.271` + `core-site.xml` (S3A → MinIO) | schema **3.1.0** initialised ✅; currently built with the wrong `hadoop-aws` (E17) |
| spark | **custom build** on `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` + `iceberg-spark-runtime-3.5_2.12:1.11.0` + `iceberg-aws-bundle:1.11.0` + pandas 2.2.3 / openpyxl 3.1.5 / pyarrow 17 + `spark-defaults.conf` | catalog `lake` = Iceberg HiveCatalog → `thrift://hive-metastore:9083`, warehouse `s3a://lakehouse/warehouse`, S3FileIO → MinIO, path-style |

Job: `jobs/bronze_ingest.py` reads both Excel files with pandas (all strings), adds `_ingest_date/_ingested_at/_source_file/_batch_id`, writes `lake.bronze.chatlog` and `lake.bronze.bot_predict` (format v2, partitioned by `_ingest_date`, `overwritePartitions()` for idempotent reruns). **Not run yet.**

## 4. Timeline of what was done (2026-10-15 → 10-16)

1. **A**: WSL/Docker checks; `.wslconfig` applied; Docker Desktop engine started; WSL integration on; hello-world OK.
2. **C0 cleanup**: removed a leftover Airflow 3.3.2 Compose stack (`docker compose -p airflow down`, volume `airflow_postgres-db-volume` kept), a kind cluster `lab-control-plane`, old containers, and unused images (spark:latest, trino:latest, kind/envoy/registry-mirror, hello-world).
3. **B**: repo unzipped, Excel files copied, `.env` created, git init + commits, pinned images pulled. `apache/hive:4.0.1` ships hadoop-common 3.3.6.
4. **C**: MinIO + bucket ✅, Postgres ✅, HMS 4.0.1 built + schema 4.0.0 ✅.
5. **D (smoke test)** on HMS 4.0.1: `SHOW NAMESPACES` ✅, `CREATE NAMESPACE lake.tmp` ✅, `CREATE TABLE` ❌ **E16**.
6. **E**: switched to `apache/hive:3.1.3` (hadoop-common **3.1.0**), wiped `vnpt-lake_pg-data`, schema 3.1.0 ✅, `SHOW NAMESPACES` ✅, `CREATE NAMESPACE` ❌ **E17**. **Stopped here.** Plan "F" below was written but not run.

## 5. Errors seen (full table in `docs/task1/05_errors_and_fixes.md`)

| ID | Symptom | Root cause | Status |
|---|---|---|---|
| E01 | `pull access denied` for `minio/minio`, `minio/mc` | MinIO community images no longer published on Docker Hub | fixed: Chainguard MinIO + aws-cli |
| — | `docker: command not found` in Ubuntu right after Docker Desktop start | WSL integration reloads after engine start | fixed: `exit` → `wsl` |
| — | git `user.email` saved with `< >` | typo | fix: `git config --global user.email "…"` |
| — | HMS entrypoint runs `set -x` → **DB password printed in logs** | image design | open: move creds to a mounted `hive-site.xml`; treat logs as sensitive |
| E16 | `CREATE TABLE` → `TApplicationException: Invalid method name: 'get_table'` | Spark 3.5 bundles the **Hive 2.3** metastore client (used by Iceberg's HiveCatalog); **HMS 4.x no longer serves `get_table`** (Hive 4 moved to request-style APIs, cf. HIVE-26537). DB calls compatible, table calls not | fixed by HMS **3.1.3** |
| E17 | `CREATE NAMESPACE` → `RetryingMetaStoreClient: MetaStoreClient lost connection … createDatabase` + `TTransportException` | **Suspected**: `HADOOP_AWS_VERSION=3.1.3` but the image's hadoop-common is **3.1.0** (must match exactly). `createDatabase` is where HMS first uses S3A to mkdir on MinIO | **OPEN**: confirm in HMS server log, then fix |
| E07 | (preventive) schema init on every start | `apache/hive` entrypoint runs `schematool -initSchema` each start | set `IS_RESUME: "true"` now that the schema exists |

## 6. Plan "F" (written, NOT executed) to close E17
1. Confirm: `$DC logs hive-metastore 2>&1 | grep -n -i -E -A4 "exception|error" | head -40` (expect `NoSuchMethodError` / `NoClassDefFoundError` in `org.apache.hadoop.fs.s3a` or `com.amazonaws`).
2. `HADOOP_AWS_VERSION=3.1.0` in `.env` and `.env.example` (SDK bundle stays 1.11.271).
3. Uncomment `IS_RESUME: "true"` in compose.
4. `$DC up -d --build hive-metastore`; check `/opt/hive/lib` has `hadoop-aws-3.1.0.jar`; use `--no-cache` if a stale jar remains.
5. Rerun the smoke test: `CREATE NAMESPACE lake.tmp; CREATE TABLE lake.tmp.t (id INT, name STRING) USING iceberg; INSERT …; SELECT …; SELECT … FROM lake.tmp.t.snapshots;`

## 7. Direction change: prefer pulled images + compose config

Bee wants to stop maintaining custom images where possible. Evaluate **before** fixing E17 the old way. Verify each candidate (tag exists, versions, size, licence/maintenance) instead of trusting this list:

| Need | Current | Candidate approaches (to evaluate) |
|---|---|---|
| HMS with S3A | custom build | (a) a maintained public HMS image that already bundles `hadoop-aws` for MinIO (several Trino/Iceberg demo images exist, verify which are current); (b) official `apache/hive:3.1.3` + **jars mounted** from a host folder or a one-shot `curl`/`busybox` init container into a named volume, + `core-site.xml` and `hive-site.xml` mounted; (c) if the lab allows, an Iceberg **REST** catalog image (lighter, no Thrift/Hive-client issues) — the lab names Hive Metastore, so ask Bee/leader first |
| Spark + Iceberg | custom build | official `apache/spark:3.5.9…` + `--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0` with an Ivy cache volume, + mounted `spark-defaults.conf` |
| Excel reading (pandas/openpyxl) | pip in custom Spark image | options: a small pre-step converting xlsx → CSV/Parquet in a public Python image; or an image that already ships pandas+openpyxl; or keep one thin layer. State trade-offs |

Constraints that still hold: **Spark 3.5's Hive client needs HMS 3.x** (E16), and **hadoop-aws must equal the HMS image's hadoop-common** (E06/E17).

## 8. Disk usage (30–50 GB): where it goes, and what to do
Likely contributors: base images (apache/hive 4.0.1 ≈ 2.8 GB and 3.1.3, apache/spark 3.5.9 ≈ 1.9 GB, apache/airflow 3.3.2 ≈ 3.2 GB, aws-cli 0.65 GB, leftover kindest/node ≈ 1.3 GB), **two custom images rebuilt several times** (each build adds layers), BuildKit build cache, old volumes (~1.4 GB), and the **WSL/Docker virtual disks (`.vhdx`), which grow but do not shrink by themselves**.

Suggested sequence (ask Bee before deleting anything):
1. Measure: `docker system df -v`, `du -sh ~/* 2>/dev/null`.
2. Remove what's no longer used: `apache/hive:4.0.1` + its built image, the untagged `kindest/node`, the old `airflow_postgres-db-volume` (if Bee agrees), dangling images, **build cache** (`docker builder prune`).
3. Reclaim on Windows: `wsl --shutdown`, then compact the Docker data `.vhdx` (cmd + `diskpart`: `select vdisk file="…\docker_data.vhdx"`, `attach vdisk readonly`, `compact vdisk`, `detach vdisk`). Docker Desktop 4.x keeps it under `%LOCALAPPDATA%\Docker\wsl\` (check the exact file name on the machine). Also consider Docker Desktop **Settings → Resources → Advanced → disk limit**, and sparse VHD support in current WSL (verify the command on the machine).
4. Prevent: pulled images instead of rebuilds; one Postgres; scale idle services down; prune after each task.

## 9. Expected numbers (for validation; full list in `docs/task1/06_validation.md`)
chatlog 21,224 rows / 15 cols · predict 10,729 / 17 · 0 duplicate IDs · 89 distinct senders · chatlog SOURCE client 10,613 / bot 10,611 · `/home` 10,378 of client rows · 18 distinct days (2024-07-02 → 07-25; 07-24/25 ≈ 97%) · predict→chat match 10,613, **116 unmatched** · 0 timestamp parse failures.

## 10. Next steps, in order
1. Read context; verify repo state (`git log --oneline`, `cat infra/compose/docker-compose.yml`, `grep -v PASSWORD infra/compose/.env`).
2. Disk audit + cleanup plan (section 8), with Bee's approval.
3. Decide the infra approach (section 7): propose 2–3 options with RAM/disk/complexity and the E16/E17 constraints; implement the chosen one.
4. Close E17 (confirm in server log first).
5. Smoke test → `bronze_ingest.py` → validate against section 9 → rerun (idempotency) → explore `.snapshots`/time travel → failure drills.
6. Update `docs/progress.md`, `docs/task1/04_process.md`, `docs/task1/05_errors_and_fixes.md` after each step; commit with clear messages.
