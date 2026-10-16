# CLAUDE.md — context for Claude Code (VNPT AI lakehouse lab)

**Start every session by reading `docs/HANDOFF.md` and `docs/progress.md`.**

## Who / how to work
- Bee (Pham Quang Minh), Data Engineer fresher at VNPT AI. Wants direct, concise answers with technical justification, and challenges unsupported claims, so back claims with command output, docs or versions.
- Guide **step by step**: one stage at a time, say what to expect, ask for output before continuing. Ask before deleting anything or making big design changes.
- Bee starts in **Windows cmd** (not PowerShell), then `wsl` into Ubuntu-22.04 for all project commands. Verify user/home with `whoami; pwd` (observed `quang`, `/home/quang`; Bee calls it "user").

## Lab scope and data
- Onboarding lab only. Tasks 1–5 are standalone mini-projects on the same data (1 Spark→Iceberg on MinIO, 2 K8s+Airflow, 3 Trino, 4 StarRocks/dbt Gold, 5 Superset). Current: **Task 1 on Docker Compose**.
- Data = **trial/imaginary chatbot logs for freshers, not real people**. Still keep it on the laptop: no uploads, no row contents in alerts/Git/public logs.
- Raw files: `~/data/vnpt-lab/` (outside the repo).

## Machine limits
- Windows 11 + WSL2 (10 GiB RAM / 8 CPU visible), 16 GB physical RAM, i7-13700HX.
- **Disk is a concern**: the setup already used 30–50 GB. State disk cost of every proposal; prune after each task.
- Every container/pod needs memory limits; JVM `-Xmx` ≈ 70–75% of the limit.

## Infra preferences
- Prefer **pulled public images configured via docker-compose** (env, mounted configs/jars, `--packages`) over custom builds; build only when unavoidable and keep it thin.
- Pin versions (tags or digests). Change one version at a time.

## Stack / versions (see docs/progress.md §2)
Python 3.11 · Spark 3.5.9 (Scala 2.12, Java 17) · Iceberg 1.11.0 runtime, format v2 · MinIO via `cgr.dev/chainguard/minio` (pinned digest) · **Hive Metastore 3.1.3** (HMS 4 breaks Spark 3.5's Hive 2.3 client: E16) on Postgres 16 · hadoop-aws must equal the HMS image's hadoop-common (3.1.0 for apache/hive:3.1.3) · later: Airflow 3.3.2, Trino 483, StarRocks 3.5.21, dbt, Superset.

## Data facts
- `bot_dvc_hcm.xlsx` (chat log, key `ID`) and `bot_ddvc_hcm_bot_predict.xlsx` (NLU result, `ID_CHATLOG` → chat `ID`); sheet `Export Worksheet`.
- `CREATED_TIME` is Oracle text (`02-JUL-24 03.40.09.895455000 PM`); ~98% of client messages are `/home`; 2 internal email sender IDs; 116 predict rows without a chat row.
- `final_intent`: fallback stays fallback; confidence < NLU_THRESHOLD → `bot_khong_hieu`; else INTENT_NAME.
- Expected numbers: `docs/task1/06_validation.md`.

## Engineering rules
- Idempotent jobs (`overwritePartitions` / `MERGE`), parameterised by date/interval.
- Bronze = raw strings + load metadata; Silver = typed/deduped/flagged; Gold = dbt marts.
- No secrets in Git or images; `.env` is git-ignored. The Hive image prints `SERVICE_OPTS` (incl. DB password) in its logs, so treat logs as sensitive.
- Log every error in `docs/task1/05_errors_and_fixes.md` (symptom → root cause → fix → theory) and every step in `docs/task1/04_process.md`; update `docs/progress.md` at session end.

## Commands
- `DC="docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env"` then `$DC up -d …`, `$DC logs -f <svc>`, `$DC ps -a`
- `make up` / `make down` / `make bronze DATE=YYYY-MM-DD` / `make sql`
