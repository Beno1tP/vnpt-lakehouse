# CLAUDE.md — context for Claude Code (VNPT AI lakehouse lab)

## Who / what
- Bee (Pham Quang Minh), Data Engineer fresher at VNPT AI. Prefers direct, concise answers with technical justification.
- Onboarding lab: an end-to-end lakehouse on chatbot log data, running locally on Minikube.
- Progress and decisions: `docs/progress.md` (update it at the end of each session).

## Machine limits (respect them)
- Windows 11 + WSL2, 16 GB RAM (the bottleneck), i7-13700HX, ~130 GB free disk.
- Minikube: `--memory=7168 --cpus=4` (may raise to 8192/6). Scale idle services to 0.
- Every pod needs resource requests/limits; JVM `-Xmx` ≈ 70–75% of the container limit.

## Stack and versions (see docs/progress.md §2)
Python 3.11 · Spark 3.5.9 (Scala 2.12, Java 17) · Iceberg 1.11.x runtime, table format v2 ·
MinIO · Hive Metastore 4.0.x (metastore only) on Postgres 16 · Airflow 3.3.2 (Helm, LocalExecutor) ·
Kubeflow Spark Operator · Trino 483 · StarRocks 3.5.21 (1 FE + 1 BE) · dbt (dbt-starrocks) · Superset.

## Data
- Two trial files: `bot_dvc_hcm.xlsx` (chat log; join key ID) and `bot_ddvc_hcm_bot_predict.xlsx` (NLU result; ID_CHATLOG → chat ID).
- Data stays on this laptop. Never upload it, never put data contents in alerts, logs sent outside, or Git.
- Raw files live in `~/data/vnpt-lab/` (outside the repo).
- Known issues: `CREATED_TIME` is Oracle text (`02-JUL-24 03.40.09.895455000 PM`); ~98% of client messages are `/home`; 2 internal email sender IDs; 116 predict rows without a matching chat row.
- `final_intent` rule: fallback stays fallback; confidence < NLU_THRESHOLD → `bot_khong_hieu`; else INTENT_NAME.

## Engineering rules
- Idempotent jobs parameterised by `[start, end)` intervals; `MERGE` / partition overwrite, never blind append.
- Bronze = raw + load metadata, immutable. Silver = typed, deduped, flagged. Gold = dbt marts + `gold_uc*`.
- Every Gold number must match the DuckDB expected values in `docs/expected_numbers.md`.
- One new component at a time. Change one version at a time.
- No secrets in Git or images: K8s Secrets / `.env` (git-ignored).
- Alerts go to Telegram with task/status metadata only.

## Commands
- `make up` / `make down` — start or stop the stack
- `make test` — pytest; `make lint` — ruff
- `make demo` — end-to-end demo run
