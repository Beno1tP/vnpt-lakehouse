# Task 2 — Process log

One entry per work block. Newest first. Keep it factual: what you did, what you saw, what you learned.

## Template
```
### YYYY-MM-DD HH:MM — <step id> <short title>
Did:
Observed (numbers, logs, screenshots path):
Learned (theory link):
Next:
Time spent:
```

---

### 2026-10-27 — from-zero run 2 (after E52): PASSED, 0 restarts, demo 16 min
Did (Bee): PR #36 merged (main `2156c7e`); `minikube delete`; `make k8s-up` → interrupted with Ctrl+C at 23m15s during the Airflow install (all Airflow pods `Init:0/1` behind the migration job). Cause: **image pull** of `vnpt-lake-airflow@sha256:cf03…` took **~9m50s** (events: Pulling 14m ago → Pulled 4m13s ago), the migration itself 1 s (`Database migration done!` 02:01:06Z); release left `failed` (not pending) → `make k8s-up` again: **20.7 s**, idempotent (everything unchanged/configured, Jobs recreated after TTL, operator rev 2, Airflow rev 2, admin created). `time make k8s-demo` → **16m08s**: landing 18 days × 2 OK; 24 runs, 0 failed; poll smooth (no exit 137, no `container not found`); `dag-report`: **36 `[bronze]` lines** (18 days × 2 tables, none missing), `23/23` ×18, `no/no` ×6 (+ scheduled 2026-10-26 run skipped), 79 log files; restart check passed; `PASSED`.
After: scheduler `RESTARTS 0`, 615 Mi in `top` while the `lake_maintenance` catch-up run executed (2 LocalExecutor workers: one busy with that task, one idle; scheduler 190 MiB, serve-logs 152 MiB); api-server 351 Mi, dag-processor 393 Mi, triggerer 516 Mi.
Learned: the 44 → 16 min difference was the OOM retries (E52). Don't interrupt long waits: every step has its own timeout; inspect from a second terminal. **2.8 acceptance (from zero) met.**

### 2026-10-27 — from-zero run 1: k8s-up 12m31s ✅, k8s-demo 43m56s PASSED but scheduler OOMKilled ×9 (E52)
Did (Bee): PR #35 merged; baseline `dag-report` (only 07-24: remote logs exist since 2.6, the 2.5 backfill logs were local); `minikube delete`; `time make k8s-up` → **12m31s**, all pods Running/Completed, Allocatable 7442652Ki, operator rev 1, Airflow rev 1 (`git 7a318b7`, airflow `cf03f5d3…`), admin created; `time make k8s-demo` → **43m56s**: landing 18 days × 2 tables OK (21,224 / 10,729), backfill 24 runs, 0 failed, `[validate] 23/23` ×18, landing no/no ×6 (+ the scheduled 2026-10-25 run, skipped), `PASSED`.
But: exit 137 + `container not found ("scheduler")` ~8× during the poll; scheduler RESTARTS 9, OOMKilled at 1Gi; 7 bronze attempt=2; `[bronze]` lines missing for 07-03/15/17/19/23/25 (retry reattached to the running driver) → **E52**; fix on branch `task2/scheduler-oom`; from-zero to be repeated with 0 restarts as acceptance.

### 2026-10-27 — pre-delete checks: token copied, baseline report empty (E51)
Did (Bee): copied the GHCR pull token from Secret `ghcr-pull` into secrets.env without printing it; `make k8s-secrets` → `ghcr-dockerconfig.json`; sha256 prefix cluster = render = `7616eea4db01`; PR #34 merged (main `8b6cfa1`, airflow `cf03f5d3…`); `make dag-report` → **0 lines** (E51) → fix: report from `s3://airflow-logs` (branch `task2/dag-report-s3`), run again before `minikube delete`.

### 2026-10-27 — PR #33 merged (main `c0babb5`); airflow digest promoted (PR #34)
Images run 40372279799: airflow → `sha256:cf03f5d3…3054` (hello_lake removed); spark-job unchanged.

### 2026-10-27 — 2.8 prepared: from-zero targets (decision: real from-zero test, A) + hello_lake removed
Decided (Bee): **A** = real from-zero test (`minikube delete`, then `make k8s-up && make k8s-demo`), accepting the loss of everything in the cluster (tables, Airflow history, and the backups, since they live in the same MinIO = a gap to record); **remove `hello_lake`** (2.3 smoke DAG with the E38 `timedelta` pattern).
Prepared (Claude, branch `task2/docs-2-8`): `make k8s-up` (deploy-guard → minikube start with the E25 flags if not running → metrics-server → k8s-secrets → `kubectl apply -k` → wait postgres/minio/bucket-init/minio-users/HMS → operator 2.2.1 by digest → airflow-install → airflow-admin → pod list); `make k8s-demo` (landing → unpause chatbot_bronze → backfill FROM..TO → poll until all runs finished → dag-report → fail on any failed run → unpause lake_maintenance); Secret `ghcr-pull` now rendered from `GHCR_PULL_TOKEN` in secrets.env (kustomize, 3 namespaces) instead of typed by hand, so the from-zero path has no manual step; `dags/hello_lake.py` deleted. Render tested in a scratch copy with dummy values (real `infra/k8s/secrets/` untouched, E42).

### 2026-10-27 — 2.7 done: operator by digest, GHCR pruned (E50)
Did (Bee): paused `alert_smoke` + `hello_lake` (CLI prints the state *before* the change); checked failed runs = only the deliberate tests (chatbot_bronze `manual__…10:18:49` E47, alert_smoke 07:05 + 07:49 from 2.6); running operator imageID = `@sha256:865ff4da…948a` = the pin; PR #31 merged; `helm upgrade spark-operator` → revision 2, both pods `controller:2.2.1@sha256:865ff4da…`; `make maintenance DRY=yes` → COMPLETED (snapshots 7 = 6 + the 07-24 rerun, all files 23, live 18, rows unchanged, orphans ≥ 72 h: 0); `make ghcr-prune` dry run → 16 versions listed; `CONFIRM=yes` → HTTP 422 on the first DELETE, nothing deleted (E50, gh 2.4.0); upgraded gh to 2.103.2 (official apt repo, +7.8 MB); PR #32 (delete via urllib) merged; `CONFIRM=yes` → **deleted 16 versions**; dry run → 0; `gh auth refresh --remove-scopes delete:packages` → scopes `gist, read:org, read:packages, repo, workflow`.
GHCR now: spark 1, hms 1, spark-job 3, airflow 3 (deployed + 2 rollback each). **Step 2.7 done.**

### 2026-10-26 — 2.7 image hygiene prepared (branch `task2/close-docs`)
Audit (Claude, `grep image:` over infra/, dags/, Makefile): every K8s manifest already pins a digest (postgres, minio, mc directly; spark/hms via kustomize `images:`; Airflow + spark-job via images.env), **except the Spark Operator** (`controller:2.2.1`, tag only). Chart 2.2.1 builds `registry/repository:tag` (`_helpers.tpl`), so `image.tag: "2.2.1@sha256:865ff4da…948a"` pins it (index digest of tag 2.2.1 from the ghcr.io registry API; Bee checks it equals the running pod's imageID before upgrading).
Added: CI kustomize job fails on any rendered `image:` without `@sha256:`; unit test for the operator pin; `make ghcr-prune [KEEP=3] [CONFIRM=yes]` (`.github/scripts/ghcr_prune.py`: keeps images.env digests + newest 3 per package, dry run by default; safe because images are pushed with `provenance: false` = no child manifests) + unit tests.

### 2026-10-26 — PR #30 deployed (revision 12): reformatted images rerun a real day
Did (Bee): PR #30 merged, `make airflow-install` (`git c1c61d3`, revision 12), `spark_job_image` = `…@sha256:05ff7b97…9eb2`; trigger with `--logical-date 2024-07-24` refused (E49); `airflow tasks clear chatbot_bronze --start-date 2024-07-24 --end-date 2024-07-24 --yes`.
Observed: `backfill__2024-07-24T00:00:00+00:00` rerun **success** 16:49:22 → 16:50:38 (76 s) on the new images. **Lab-doc tooling items done.**

### 2026-10-26 — PR #29 merged (lab-doc tooling); spark-job + airflow digests promoted (PR #30)
Did (Bee): `make venv` failed on `pre-commit==4.7.0` (E48 → 4.6.2); `make all` / `make hooks` / `pre-commit run --all-files` passed; first CI run: DAG integrity `OSError: File name too long` (my check treated `application_file` as a path, but DagBag already resolves `.yaml` templates to their text) → fixed, 4/4 green; merged (main `7fd88ec`); images run 40360779190: spark-job → `sha256:05ff7b97…9eb2`, airflow → `sha256:fd7c8088…378a` (formatting only).

### 2026-10-26 — lab-doc tooling: make lint/test/all, pre-commit, Bronze I/O tests, CI template rendering
Decided (Bee): no trigger-form `day` field (explicit logical dates); format the repo once with `ruff format` and enforce `ruff format --check` in CI.
Did (Claude, branch `task2/lab-doc-items`): `requirements-dev.txt` (ruff 0.16.10, pytest 9.1.1, pyyaml 6.0.2, pre-commit 4.6.2 + job deps (E48); CI installs the same file); `make venv/lint/fmt/test/all/hooks`; `.pre-commit-config.yaml` (ruff-pre-commit v0.16.10 check+format, pre-commit-hooks v6.0.0: check-yaml, end-of-file, trailing whitespace, large files > 500 KB, detect-private-key, merge conflicts); `tests/unit/test_bronze_io.py` (pandas → strings / empty → None, S3 landing read via pyarrow with a fake filesystem, `table_exists` → overwritePartitions vs create v2 partitioned by `_ingest_date`) → 49 tests; DAG integrity now renders every `application_file` in strict Jinja with and without a logical date (E47 prevention); one `ruff format` commit (cosmetic).
Lab-doc pytest topics: constants ✅, Oracle timestamp → event day ✅ (`event_day`, full parsing comes with Silver), table_exists ✅, pandas → Spark types ✅, S3 read ✅, watermark → **mapped, not coded**: Airflow data intervals + idempotent per-day `overwritePartitions` replace a stored watermark.

### 2026-10-26 — E47 verified (revision 11) → step 2.5c done
Did (Bee): PR #28 merged, `make airflow-install` (revision 11, `git 4f5060d`); triggered `lake_maintenance` (dry run) and `chatbot_bronze`, both **without** a logical date.
Observed: lake_maintenance manual run **success** in 40 s (no UndefinedError); chatbot_bronze failed immediately in check_landing (AirflowFailException, no retry wait) → exactly one Telegram `❌ chatbot_bronze.check_landing FAILED for day none (run without a logical date) (attempt 1/1, final)`. **Step 2.5c done.**
Decision (open, Bee): optional `day` field in the Airflow trigger form; current rule = loading a day must be explicit (logical date / backfill), runs without one fail fast.

### 2026-10-26 — PR #27 merged (E46 results + E47 fix, #26 closed into it); Airflow digest promoted (PR #28)
Did (Bee): PR #26 closed, PR #27 4/4 green (DAG integrity 2m18s), merged (main `87804e7`); images run 40303175320: airflow → `sha256:0c7cc70c…c988`.

### 2026-10-26 — lake_maintenance through Airflow: scheduled run OK, manual run without logical date failed (E47)
Did (Bee): unpause + trigger `lake_maintenance` with `--conf '{"dry_run": "true"}'` (no logical date).
Observed: unpause created `scheduled__2026-10-25T02:00` (logical 2026-10-18, real defaults 168 h / 5 / 72 h) → **success** in 41 s: already clean (6 snapshots, 22 files, 0 expired, 0 orphans; time travel to today's oldest kept snapshot still possible); its log also printed `log read interrupted … might get duplicated` = the doubled-lines cause, now confirmed. Manual run → `UndefinedError: 'ds' is undefined` ×2 attempts (retry 10 min) → ❌ alert received (`attempt 2/2, final`) → E47; fix: maintenance name without a date variable, bronze fails fast without a logical date.

### 2026-10-26 — 2.5c maintenance runs (E46)
Did (Bee): PR #25 merged, `make airflow-install` (revision 10, `lake_maintenance` listed); `make maintenance DRY=yes ORPHAN_HOURS=1` → refused by Iceberg (E46); dry run with 24 h; real run `SNAP_HOURS=1 ORPHAN_HOURS=24`; validate 2024-07-24.
Observed:
| table | snapshots | all data files | live files | live bytes | metadata.json | rows |
|---|---|---|---|---|---|---|
| chatlog before → after | 50 → **6** | 50 → **22** | 18 → 18 | 1,328,792 → same | 50 → **11** | 21,224 → same |
| bot_predict before → after | 49 → **6** | 49 → **22** | 18 → 18 | 752,508 → same | 49 → **11** | 10,729 → same |
expire_snapshots deleted 28 / 27 data files, 55 / 53 manifests, 45 / 44 manifest lists; rewrite_data_files 0 (one live file per partition already); rewrite_manifests 19 → 1; orphans older than 24 h: 0 (today's drill orphans become eligible tomorrow); 6 snapshots = 5 retained + 1 from rewrite_manifests; 11 metadata files = current + 10 previous (`write.metadata.previous-versions-max`); time travel to the oldest snapshot (2026-10-25 04:18) → **gone (IllegalArgumentException) = the expected trade-off**; validate **23/23**.
Learned: retention = time-travel window; maintenance changes files and history, never rows (asserted).

### 2026-10-26 — PR #24 merged; spark-job + airflow digests promoted (PR #25)
Did (Bee): PR #24 merged (main `37b5dfa`); images run 40294133237: spark-job `37b5dfa` → `sha256:d9e2fe94…02fc`, airflow `37b5dfa` → `sha256:2c1e554d…edbb`.

### 2026-10-26 — 2.5c maintenance prepared on branch `task2/maintenance`
Checked (Iceberg 1.11.0 Spark 3.5 source): `expire_snapshots(table, older_than, retain_last, …)` (no dry_run), `remove_orphan_files(table, older_than, dry_run, prefix_listing, equal_schemes, …)`, `rewrite_data_files(table, strategy, options, where, …)`, `rewrite_manifests(table)`. Orphan listing goes through Hadoop FileSystem by default — the Spark image has no S3A jar — so `prefix_listing => true` (S3FileIO implements `listPrefix`).
Prepared (Claude): `jobs/maintenance.py` (per table: stats before → metadata retention properties → expire → compaction + manifests → orphans → stats after → row count must not change → time travel to the oldest snapshot: proves the expiry trade-off; `--dry-run` = counts + orphans that would go; timestamps as explicit UTC literals because the session zone is Asia/Ho_Chi_Minh); `dags/templates/maintenance_sparkapp.yaml` (shared by DAG and make); `dags/lake_maintenance.py` (weekly Sunday 02:00 UTC, params snapshot_hours 168 / retain_last 5 / orphan_hours 72 / dry_run, deadline 2 h, failure alert, retries 1); `make maintenance [SNAP_HOURS] [RETAIN] [ORPHAN_HOURS] [DRY=yes]`; tests 45.

### 2026-10-26 — Drill 7 (register_table) passed → step 2.5b done
Did (Bee): PR #22 merged (main `f31c931`), `make airflow-install` → revision 9; drill 7 (see `07_drills.md` #7).
Observed: chatlog `00047-6e05c394…` 48 snapshots → reload → `00048-4c174f4a…` 49 (08:19:28) → restore of backup `20261026T075751Z` → `00047…` 48 (RPO: newest commit invisible) → `forget` (files kept) → `register` from `00048-4c174f4a…` → 49, 08:19:28 again. Validate then failed (FAIL lines not captured; most likely the one-batch rule: chatlog at the 08:19 load, bot_predict at the backup's) → reload → 23/23; bot_predict now `00048-d2103ce7…` (49 snapshots, 08:27:00). Note: the 08:19 bot_predict `00048-…` file written before the restore is now an **orphan** (a second `00048` with another UUID was created from `00047`) → removed later by `remove_orphan_files` (2.5c). E45: my placeholder in a command block.
Learned: Iceberg tables are self-describing; the catalog is an index that can be rebuilt from the newest `metadata.json`. Backup + register_table together give RPO ≈ 0 for committed data. **Step 2.5b done.**

### 2026-10-26 — PR #21 merged; spark-job digest promoted (PR #22)
Did (Bee): PR #21 merged (main `56e7b95`); images run 40290365480: spark-job `56e7b95` → `sha256:40ece292…1208` (contains catalog_tool.py). `make airflow-install` after the merge so the DAG's `spark_job_image` Variable follows images.env.

### 2026-10-26 — 2.5b backup + drill 6 (lose the catalog, restore) passed; drill 7 prepared
Did (Bee): PR #20 merged (main `e9a83a8`); `make k8s-secrets` (MINIO_BACKUP_SECRET), jobs re-run (bucket `backup`, user `backup-writer` → `lake-backup`), `cronjob.batch/pg-backup created`; `make backup-now` → **`backup ts=20261026T075751Z`**, metastore.dump 125,274 B + airflow.dump 222,051 B, 339 KiB uploaded; drill 6: validate 23/23 → `make drill-lose-metastore CONFIRM=yes` (DROP/CREATE DATABASE, HMS re-initialised schema 3.1.0) → validate **FAILED** `[TABLE_OR_VIEW_NOT_FOUND] lake.bronze.chatlog` → `make restore-metastore TS=20261026T075751Z` → `restored … 2 tables, schema 3.1.0`, `HMS schema 3.1.0 exists` → validate **23/23**.
Learned: the whole catalog is 125 KB of pointers; the data (Parquet + Iceberg metadata in MinIO) was never touched. Restore ≈ 1 min.
Prepared (Claude, branch `task2/register-table`): `jobs/catalog_tool.py` (info / forget = DROP TABLE without PURGE / register = `CALL lake.system.register_table`; table names and metadata paths validated by strict patterns, unit-tested incl. an SQL-injection string), `infra/k8s/spark-jobs/catalog-sparkapp.yaml`, `make catalog CMD= TABLE= [METADATA=]`. Needs a new spark-job image (jobs/ changed).

### 2026-10-26 — 2.6 closed (E44 texts verified); 2.5b backup + restore prepared on branch `task2/backup`
Did (Bee): PR #19 merged, revision 8; `alert_smoke` with `--logical-date 2024-07-24` → `⏰ alert_smoke run for 2024-07-24T00:00:00Z … deadline 2026-10-26 07:49:51.758193+00:00 passed / run: manual__2026-10-26T07:49:21.750587+00:00` and `❌ alert_smoke.fail_on_purpose FAILED for day 2024-07-24 (attempt 1/1, final)` → texts correct. **Step 2.6 done.**
Prepared (Claude): `lake/pg-backup-cronjob.yaml` (daily 01:00 UTC, `startingDeadlineSeconds` 12 h for a laptop that was off, Forbid concurrency; initContainer `pg_dump -Fc` of metastore + airflow with the server's own postgres:16 digest; container Chainguard `mc cp` to `s3://backup/postgres/<UTC ts>/`); MinIO user `backup-writer` (list/get/put on `backup`, **no delete**); bucket `backup`; `lake/pg-restore-job.yaml` (download dump → `DROP DATABASE … WITH (FORCE)` → `pg_restore` → print table count + schema version); make `backup-now`, `drill-lose-metastore CONFIRM=yes` (destructive, refuses without CONFIRM), `restore-metastore TS=` (scales HMS to 0 and back; secret names resolved from the CronJob, not by grep, because old hashed Secrets stay around); tests 36; drills 6–7 in `07_drills.md`.

### 2026-10-26 — PR #18 merged (E44); Airflow digest promoted (PR #19)
Did (Bee): PR #18 4/4 green, merged (main `1590fef`); images run 40286034074: airflow `1590fef` → `sha256:97382ff9…bad2`.

### 2026-10-26 — 2.6 deployed (revision 7); both alert paths proven
Did (Bee): PR #17 merged (main `2d47dca`); re-ran bucket-init + minio-users (bucket `airflow-logs` created, user `airflow-logs` → `lake-airflow-logs`); `make airflow-install` → revision 7; `alert_smoke` + `chatbot_bronze` listed, `remote_base_log_folder = s3://airflow-logs`, triggerer has the token and `plugins/lake_alerts.py`; triggered `alert_smoke`; restarted the scheduler.
Observed in Telegram (Alert group): `⏰ alert_smoke run for None is still not finished: deadline 2026-10-26 07:05:45.690106+00:00 passed` (= queued 07:05:15 + 30 s) and `❌ alert_smoke.fail_on_purpose FAILED for day ? (attempt 1/1, final)` with the run id. Both paths work (triggerer async deadline callback; task failure callback after the last try); metadata only. Text gaps → E44 (fix on branch `task2/alert-text`).
Durable logs (after the scheduler restart, via `make airflow-ui`): the UI shows the `fail_on_purpose` log with **source `s3://airflow-logs/dag_id=alert_smoke/run_id=manual__2026-10-26T07:05:15.677020+00:00/task_id=fail_on_purpose/attempt=1.log`** and the full content (`RuntimeError: alert smoke test: intentional failure` + traceback) → task logs now survive scheduler restarts (gap #8, the evidence lost in E41 / drill 3). **Step 2.6 done** except Spark event logs + History Server (Could).

### 2026-10-26 — 2.6 PR #16 merged; Airflow digest promoted (PR #17)
Did (Bee): `make k8s-secrets` (E42 repaired: 0 files with dummy values; MINIO_AIRFLOW_LOGS_SECRET generated; airflow-alerts.env written); PR #16 4/4 green (DAG integrity imported both DAGs + `lake_alerts` from plugins inside the image), merged (main `43f1b46`); images run 40282284105: airflow `43f1b46` → `sha256:048b18ba…ee0f`.

### 2026-10-26 — 2.6 Telegram channel set up; 2.6 prepared on branch `task2/alerts`
Did (Bee): dedicated group "Alert group" with bot `bee_lake_alerts_bot` (leader agreed; teammates later). Token pasted in the chat → revoked and replaced (E43); new token stored with `read -rs` in `secrets.env`; `getMe ok: True`; `getUpdates` empty after the revoke → bot removed/re-added → chat ID **-5368542184** (basic group; changes to -100… if Telegram upgrades it to a supergroup); test `sendMessage` → 200, message received.
Checked (Airflow 3.3.2 source/docs): `DeadlineAlert(reference=DeadlineReference.DAGRUN_QUEUED_AT, interval, callback=AsyncCallback|SyncCallback)` from `airflow.sdk`; async callbacks run in the **triggerer** and must be importable there → plugins folder; Airflow passes `context` (dag_run, deadline). Amazon provider 9.36.0 in the constraints → S3 remote logging.
Prepared (Claude): `plugins/lake_alerts.py` (failure + deadline texts: DAG/task/day/attempt/run only, never exception text; urllib, 10 s timeout, errors swallowed and logged without the URL); chatbot_bronze: `on_failure_callback` in default_args, DeadlineAlert 2 h after queued, dagrun_timeout 3 h (hard stop after the alert); `dags/alert_smoke.py` (30 s deadline + intentional failure); Airflow image bakes `plugins/` (named build context in CI + images workflow); Secret `airflow-alerts`; remote logging to `s3://airflow-logs` with MinIO user `airflow-logs` (policy only on that bucket; bucket-init creates it); DAG integrity rules: lake DAGs need on_failure_callback + DeadlineAlert; unit tests 33. E42: my local test overwrote `infra/k8s/secrets/` with dummy values (nothing applied) → re-render needed.

### 2026-10-26 — Drill 3 passes after the E41 fix → step 2.5 done
Did (Bee): PR #15 merged, `make airflow-install` (revision 6: image `a447aa4c…`, scheduler grace 120 s), `make drill-scheduler`.
Observed: run 2024-07-24 **success**: check_landing 03:46:21, bronze 03:46:30–03:46:53, validate 03:48:09–03:48:32, notify 03:48:33. The 75 s between bronze and validate is the scheduler hand-over (old pod drains, new pod schedules the next task) instead of a killed task. **Step 2.5 done**: backfill 24 days ×2 identical, 18 × 23/23, 6 skipped; drills: driver kill → Airflow retry; executor delete → Spark recovers in the same attempt; scheduler restart → run completes.

### 2026-10-26 — 2024-07-24 recovered; PR #14 merged; Airflow digest promoted (PR #15)
Did (Bee): `make backfill FROM=TO=2024-07-24 REPROCESS=failed` → all 4 tasks `success` (03:23:27 → 03:24:21); PR #14 merged (main `1c09d26`); images run 40264945988: airflow `1c09d26` → `sha256:a447aa4c…29a0`.

### 2026-10-26 — Drill 3 (scheduler restart) failed the run → E41 fix prepared
Did (Bee): `make drill-scheduler`, rollout status, task states, sparkapplications, scheduler events + strategy.
Observed: bronze `success` (02:52:39 → 02:53:04), validate `failed` after 10 s (02:53:05 → 02:53:15), notify `upstream_failed`, run `failed`; no validate app from this run; no task logs (new pod). Events: new scheduler pod started 02:53:01, `Startup probe failed: No alive jobs found` (normal during start), old pod `Killing` 14 s later; strategy `RollingUpdate`, grace `10` → **E41**. Leftover `bronze-2024-07-24-32sdj8zv FAILED` = drill 1's app (operator keeps failed apps for inspection; TTL 1 day removes it).
Prepared (Claude, branch `task2/drills`): scheduler grace 120 s, validate retries=1 / 1 min, CI rule, docs.

### 2026-10-26 — Drill 2 (executor deleted) passed
Did (Bee): `make drill-executor`. Observed: bronze attempt 5 log `Executor lost: 1 (epoch 0)` / `Lost executor 1 … deleted by a user`; the same attempt completed (02:44:41 → 02:45:01, 12,970 / 6,485); only attempt 5 exists → Airflow did not need to retry; validate + notify success. Executor loss is handled inside Spark (driver re-requests the executor, recomputes lost tasks).

### 2026-10-26 — Drill 1 (driver killed) passed
Did (Bee): PR #13 merged; `make drill-driver` (2024-07-24).
Observed: run re-queued 02:35:30; bronze attempt 3 lost its driver → `Task failed`, no `[bronze] … rows in partition` line (nothing committed); Airflow retry started 02:40:44 (retry_delay 5 min), attempt 4 loaded 12,970 / 6,485 in 22 s; validate success (exact per-day counts → no duplicates); notify success. One retry layer works: SparkApplication restartPolicy Never, Airflow retries; the new attempt got a new app name (random suffix), no `AlreadyExists`.

### 2026-10-26 — Doubled log lines and the midnight run explained; drills prepared
Observed (Bee): `grep -c "23/23"` in one attempt's log (07-19 validate attempt=2) → **2**: the line is repeated inside a single log (the operator re-streams the driver log), not a second validation. `airflow dags details`: `timetable_summary 0 0 * * *`, `is_paused False`, next run **logical 2026-10-26, interval [10-26, 10-27), run_after 2026-10-27 00:00 UTC**. The 10-25 day was not scheduled because the last automated run (old timetable, ended 10-25 16:29) is the reference; the next midnight-aligned interval after it starts 10-26 (transition artifact, one-time).
Prepared (Claude): `make drill-driver` / `drill-executor` / `drill-scheduler` (DRILL_DATE=2024-07-24: re-queue the day, wait until the bronze driver/executor pod is Running, then delete it / restart the scheduler); `07_drills.md` with expectations and theory.

### 2026-10-26 — 2.5 E38 proof on a fresh day; attempt numbers explained
Did (Bee): `make backfill FROM=2024-07-26 TO=2024-07-26` (a day with no run and no data); grep of its check_landing log; listing of `attempt=*` log files.
Observed: **`logical_date=2024-07-26 … run_after=2024-07-27`** and **`ds=2024-07-26 data_interval=[2024-07-26 00:00:00+00:00, 2024-07-27 00:00:00+00:00)`** → real one-day intervals at midnight, logical date = interval start: **E38 fix proven** on revision 5. Log files are `attempt=2` for every task of every backfill run and `attempt=3` for the two manual runs; no `attempt=1` files in the new pod. Reason: `REPROCESS=completed` **clears** the existing task instances, and each clear increments `try_number`; the attempt=1 logs lived in the scheduler pod replaced by revision 5. So the numbers count clears, not failures (no task failed: every run `success`).
Note: no midnight scheduled run was seen because the laptop was off at midnight; with catchup False the scheduler only creates the most recent missed interval after it starts (check `airflow dags details` / `next-execution`).

### 2026-10-26 — 2.5 second backfill on revision 5 (idempotency proof)
Did (Bee): `make backfill REPROCESS=completed` (backfill_id=2) on helm revision 5 (`git dc530d4`); `make dag-runs`; `make dag-report` (scheduler pod new → report = this backfill only).
Observed: all 24 runs `success` in ~13 min (02:10 → 02:23 UTC; faster than the first, images cached); landing `yes,yes` on 18 days, `no,no` on 07-07/12/13/14/20/21; **bronze rows identical to the first backfill on all 18 days × 2 tables** (07-24 12,970 / 6,485; 07-25 7,552 / 3,892; …); `23/23` on all 18 days → **rerun of the full history gives the same result (idempotent)**. Doubled log lines again but on *different* days (07-06 / 07-24 bot_predict, validate 07-19) than in the first backfill → random duplication when the operator re-streams the driver log, not data reloads (attempt=2 check pending).
Learned: `REPROCESS=completed` **reuses** existing runs (clears + re-queues tasks; no new run, the stored data interval stays as created) → it cannot show a new timetable; a fresh date is needed (07-26). Only one active backfill per DAG.

### 2026-10-26 — PR #12 merged; cluster restarted (E40); E38 fix deployed (revision 5)
Did (Bee): PR #12 4/4 green, merged (main `dc530d4`); `make airflow-install` → guard printed `deploying git dc530d4 from main`, then failed: cluster stopped overnight (E40); `minikube start` (profile kept: Allocatable 7442652Ki), `HMS schema 3.1.0 exists`; `make airflow-install` again → **revision 5, description `git dc530d4`** (E39 traceability works).

### 2026-10-26 — 2.5 first backfill 2024-07-02 → 07-25 (old DAG version)
Did (Bee): `make backfill DRY=yes` (22 dates: `REPROCESS=none` leaves out 07-02 and 07-07, which had manual runs), `make backfill REPROCESS=completed` (backfill_id=1: 22 new `backfill__…` runs + the 2 manual runs re-queued), `make dag-runs`, `make dag-report`.
Observed: 25 runs `success` (24 days + old scheduled run); about **50 s per data day**, ~20 min total; landing `yes,yes` on 18 days, `no,no` on exactly 07-07/12/13/14/20/21 (skipped, D2); bronze rows read = rows in partition for 18 days × 2 tables, all equal to `bronze_daily.json` (e.g. 07-24 12,970 / 6,485; 07-25 7,552 / 3,892); `[validate] 23/23` on all 18 days. Some single-table bronze lines and three validate lines appear twice (07-04/05/22 bot_predict; validate 07-05/15/17): suspected duplicated log lines when the operator re-streams the driver log, not retries (check pending: attempt=2 files).
Note: deployed was still helm revision 4 = image `20b2929d…` (timedelta schedule): `run_after = logical_date` in the backfill log. Backfill days are explicit, so the partitions are right either way; the E38 fix (PR #12, revision 5) is deployed before the second backfill. Helm's `APP VERSION 3.2.2` column is the chart's default appVersion metadata; the pods run the 3.3.2 image.

### 2026-10-26 — PR #11 merged (E38 + D6); Airflow digest promoted, backfill targets (PR #12)
Did (Bee): PR #11 merged (main `1ec904c`); images run 40214717360: airflow `1ec904c` → `sha256:fb7bf910…0976` (CronDataIntervalTimetable).
Prepared (Claude, branch `task2/backfill`): images.env bump; `make backfill FROM= TO= REPROCESS= DRY=` (Airflow 3.3.2 CLI checked: `airflow backfill create --dag-id --from-date --to-date` both **inclusive**, `--reprocess-behavior none|completed|failed` (default none = keep days that already have a run), `--max-active-runs`, `--dry-run`); `make dag-runs`; `make dag-report` (per-day landing/bronze/validate lines from the task logs).

### 2026-10-25 — Lab doc re-read: Silver required (D6)
Did (Claude): read `~/data/Engineering Onboarding Labs.docx` (copied by Bee). Findings: architecture = **Lakehouse Medallion (Bronze → Silver → Gold)**: Oracle → Excel → **S3 landing zone** → **Spark ETL → Iceberg on MinIO** → **dbt builds Gold** for StarRocks/Trino/Superset; **Airflow orchestrates everything**. Step 1 goal = raw Iceberg tables (Bronze). Silver has no step of its own. D6 first proposed in Task 2 (2.5d); **Bee decided Task 3, before Trino**, because step 2's goal is only "the step-1 Spark job on K8s, orchestrated by Airflow"; to confirm with the leader. Other requirements to track: pytest topics (constants, table_exists, **watermark**, **parse Oracle timestamp**, pandas → spark types, S3 reads), `make lint` / `make test` / `make all`, CI jobs lint/test/**dbt-lint** (dbt → Task 4), **pre-commit hooks**, dbt tests incl. `dbt_test_gold` task in the DAG (Task 4), Prometheus + Grafana optional, Minikube `--memory=7168 --cpus=4` (we run 8192/6, Allocatable 7268 Mi).

### 2026-10-25 — 2.4b first DAG runs OK; E38 schedule fix prepared
Observed (Bee): 3 runs `success`. 2024-07-02: check_landing → bronze (24 s, `12 rows read, 12 rows in partition`, `6 / 6`) → validate (`23/23 checks passed`) → notify, driver log lines streamed into Airflow (`[spark-kubernetes-driver] …`). 2024-07-07: `chatlog=no, bot_predict=no` → check_landing **skipped** → all downstream skipped (D2). Scheduled run: `ds=2026-10-25 data_interval=[16:29:09, 16:29:09)` → **E38** (timedelta = DeltaTriggerTimetable in Airflow 3). Fix on this branch: CronDataIntervalTimetable at midnight + CI rule.

### 2026-10-25 — 2.4b deployed; first chatbot_bronze runs (in progress)
Did (Bee): `make k8s-secrets` (MINIO_AIRFLOW_LANDING_SECRET + airflow-connections.env), deleted + re-applied the minio-users Job, `kubectl apply -k`, `make airflow-install` (revision 4), unpause + trigger 2024-07-02 and 2024-07-07; PR #10 merged (main `1e652df`).
Observed: Role + RoleBinding created; Job: `user airflow-landing: policy lake-landing-reader`; `auth can-i create sparkapplications` → **yes**, `can-i list secrets` → **no**; `chatbot_bronze` and `hello_lake` listed; Variable `spark_job_image` = spark-job `f56c2321…`. Scheduled run created on unpause: `scheduled__2026-10-25T16:29:09…`, **logical_date = run_after = 2026-10-25T16:29:09** (not midnight, not the interval start) → success (skipped: no export). Manual runs: logical_date 2024-07-02 / 07-07, data interval = [trigger time, trigger time).
Finding for 2.5: with `timedelta(days=1)` + catchup False, scheduled intervals start at the unpause time (16:29), and Airflow 3 sets the scheduled run's logical date to its run_after → `ds` of a scheduled run is the day the interval ENDS. Decide before the backfill: align to midnight (`CronDataIntervalTimetable("0 0 * * *", "UTC")`) and take the day from data_interval_start for scheduled/backfill runs.

### 2026-10-25 — 2.4b PR #9 merged; Airflow digest promoted (PR #10)
Did (Bee): PR #9 4/4 green (DAG integrity imported chatbot_bronze inside the 3.3.2 image), merged (main `d107f0e`); images run 40207562055: airflow `d107f0e` → `sha256:20b2929d…7c07` (spark-job rebuilt with unchanged code → not promoted).

### 2026-10-25 — 2.4a done (23/23 ×3 days); 2.4b DAG prepared on branch `task2/dag`
Did (Bee): PR #8 merged (main `821f981`); validate 2024-07-02 / 07-24 / 07-25 with the new image: **23/23 each** (10 reconciliation + 13 rules), incl. `event_day_matches_partition` = 0 and `unmatched_to_chat` = 116 on 07-25. **Step 2.4a done.**
Checked (provider cncf.kubernetes 10.22.0 source): SparkKubernetesOperator `random_name_suffix=True` by default → each try gets a new name (no `AlreadyExists`, review item A4 closed); `application_file` is Jinja-rendered (`template_ext` yaml); `delete_on_termination=True` deletes the app after streaming the driver log; `reattach_on_restart` finds the driver by labels (needs pods get/list/patch).
Prepared (Claude): `dags/chatbot_bronze.py` (timedelta(days=1), start 2024-07-02 UTC, catchup False, max_active_runs 1, dagrun_timeout 1h, retries 2 / 5 min, execution_timeout 20 min; `check_landing` (S3Hook, skip day without files, fail if only one table) → `bronze` → `validate` (retries 0) → `notify`); SparkApplication templates moved to `dags/templates/*_sparkapp.yaml` as **one** Jinja template for both Airflow and `make sparkapp` (sed fills `{{ ds }}` and `{{ var.value.spark_job_image }}`); MinIO user `airflow-landing` (read landing only); Secret `airflow-connections` (`AIRFLOW_CONN_MINIO_LANDING`, `AIRFLOW_CONN_KUBERNETES_IN_CLUSTER`) via chart `extraEnvFrom`; `AIRFLOW_VAR_SPARK_JOB_IMAGE` from images.env via `make airflow-install`; Role `airflow-spark-submitter` in spark-jobs bound to SA `airflow-scheduler`; DAG integrity rules for `lake` DAGs (execution_timeout everywhere, validate never retries). Tests 27 passed; kustomize + helm template render OK.

### 2026-10-25 — 2.4a PR #7 merged; spark-job digest promoted (PR #8)
Did (Bee): PR #7 4/4 green, merged (main `efa10a8`); images run 40203340278: spark-job `efa10a8` → `sha256:f56c2321…88cd` (airflow also rebuilt, DAGs unchanged → its digest is not promoted).

### 2026-10-25 — 2.3 done (E37 fixed); 2.4a prepared: day-generic DQ rules
Did (Bee): PR #6 merged (main `52d11b3`), `make airflow-install` → revision 3 (hostname_callable), manual trigger `--logical-date 2024-07-03`.
Observed: UI log view works (`http://10.244.0.63:8793/log/…` = pod IP). `[hello] ds=2024-07-03 interval=[2026-10-25 14:23:28.535927+00:00, 2026-10-25 14:23:28.535927+00:00)` → **manual runs: ds = logical date, data interval = [trigger time, trigger time)** → the DAG must take the day from `ds` (01_knowledge A3). **Step 2.3 done.**
Prepared (Claude, branch `task2/dq-rules`): `tests/sql/bronze_rules.sql` — 13 rules that return violation counts (expected 0) for any day: partition not empty, ID not null + unique, SOURCE ∈ {client, bot}, CREATED_TIME parseable, event day = partition (new with the daily split), ID_CHATLOG not null, one batch for both tables; validator `--rules`; validate SparkApplication passes it; unit test.

### 2026-10-25 — 2.3c password rotated, admin outside Helm; E37 task logs
Did (Bee): new admin password (`read -rs`), `make k8s-secrets`, `make airflow-install` (from this branch) → revision 2 (no credentials in NOTES, no deprecation warnings), `make airflow-admin` → `User "admin" created`, `helm get values | grep -c password` → 0. UI login OK; `hello_lake` ran (a scheduled run `scheduled__2026-10-25T14:13:01…`: catchup=False + unpaused).
Observed: UI log view failed (E37: scheduler pod hostname not resolvable). Fix: `hostname_callable` → pod IP (same PR #6).

### 2026-10-25 — 2.3b Airflow installed (E35, E36)
Did (Bee): PR #5 checks 4/4 green; `make airflow-install` (chart from the CDN, SHA-512 OK; volume before 8.79 GB).
Observed: `STATUS: deployed`, Airflow 3.3.2, revision 1. **E35:** the chart NOTES printed the admin password (it came from `--set-file createUserJob.defaultUser.password`) → password must be rotated. E36: 3 deprecation warnings.
Prepared (Claude, branch `task2/airflow-admin`): `createUserJob.enabled: false`, `make airflow-admin` (user created inside the scheduler pod from the secret file), deprecated key removed, `enableServiceLinks: false`, `recommendedProbeSetting: true`. `helm template` renders 4 Deployments + 1 Job (migrations), no create-user Job.

### 2026-10-25 — 2.3a PR #4 merged (Airflow image), digests promoted (PR #5)
Did (Bee): PR #4: first run red (E34, Airflow 2 `DagBag(include_examples=…)`), fixed, 4/4 green incl. the new DAG integrity check inside the 3.3.2 image; merged (main `a226c4f`); images run 40199225449 success.
Digests: spark `aee0deb9…` and hms `9446f7d1…` (`built=false`); spark-job `a226c4f` → `sha256:4e873c12…388e1f`; **airflow `a226c4f` → `sha256:6fbd3133…25d5a4`**.
Prepared (Claude, branch `task2/airflow-deploy`): `images.env` gets `AIRFLOW_IMAGE_REF` and the new spark-job digest — promotion of built images to "deployed" happens only through a reviewed PR.

### 2026-10-25 — 2.1b applied (PR #3); 2.3 Airflow image prepared on branch `task2/airflow-image`
Did (Bee): PR #3 3/3 green, merged (main `2a616ed`); `make k8s-secrets` (3 MinIO user passwords generated once), `kubectl apply -k`, minio-users Job, HMS rollout, `make s3-access-test`, bronze + validate 2024-07-02, `make landing`.
Observed: Job: 3 policies created, users `spark-jobs`/`landing-writer`/`hive-metastore` attached; **s3-access-test 4/4 + 4/4** (spark-jobs: write landing **denied**, create bucket **denied**; landing-writer: lakehouse **denied**); bronze 12/12, 6/6 and validate **10/10** with the spark-jobs user; landing 36 OK with landing-writer; HMS restarted on its own user. Airflow Secrets showed `configured` (re-render; Airflow not installed yet). **Step 2.1b done.**
Prepared (Claude): `docker/airflow/Dockerfile` (FROM apache/airflow:3.3.2-python3.12 by digest + `COPY dags/`, context dags/ only, no pip); CI job `dags` builds it on the runner (not pushed) and runs `tests/dags/test_dag_integrity.py` inside it (imports, catchup False, max_active_runs 1, tags, tz-aware start_date); images.yml job `airflow` (push `vnpt-lake-airflow:<sha>`, Trivy, DIGEST line); values: `images.airflow.repository` = GHCR, digest passed by `make airflow-install` from `AIRFLOW_IMAGE_REF` in images.env, `imagePullSecrets: [ghcr-pull]` (chart's `registry:` is deprecated), default image kept on the official base for chart helper containers, DAG ConfigMap mount and `make airflow-dags` removed (D4 → D5). `helm template`: 13 containers on `ghcr.io/beno1tp/vnpt-lake-airflow@sha256:…`, 6 pods with ghcr-pull, no dags volume.

### 2026-10-25 — 2.0c done; 2.1b prepared on branch `task2/minio-user`
Did (Bee): deleted the 6 old SparkApplications; `make sparkapp DATE=2024-07-02` + validate from `ghcr.io/beno1tp/vnpt-lake-spark-job@sha256:2e5f830e…` (imageID confirmed): 12/12, 6/6, **10/10**; `make landing` from the image (baked script): 36 OK, 21,224 / 10,729; `minikube image rm` of the 4 local images. Node volume 10.29 → **8.74 GB** (−1.55 GB: CI images share the Spark base; old copies gone). `minikube image ls | grep vnpt-lake` printed nothing: images pulled **by digest only** have no tag, and `minikube image ls` lists tags → check with `minikube ssh -- sudo crictl images --digests`. **Step 2.0c done.**
Prepared (Claude): 2.1b (gap #9) — three MinIO service users with their own policies: `spark-jobs` (read landing, rw lakehouse), `landing-writer` (rw landing), `hive-metastore` (rw lakehouse); root keys left only for MinIO itself, bucket-init and the new `minio-users` Job. Job: Chainguard `minio-client` (no shell; digest `sha256:1a1b84b3…`, image created 2026-10-24 — only `latest` is public) copies `mc` into an emptyDir, a bash script in the Spark base runs it: policies (re)created, users (re)set, attach only if missing → idempotent. Renderer generates the 3 passwords once; `s3-credentials` now = spark-jobs user; new `landing-writer-credentials`, `hms-s3-credentials`. `make s3-access-test`: pods with each user's keys prove allowed **and** denied operations. Unit tests 23 (policy least-privilege checks, renderer writes service-user keys, not root); kustomize render on a clean tree OK.

### 2026-10-25 — 2.0c-2 PR #2 merged; HMS + bucket-init from GHCR (E32, E33)
Did (Bee): PR #2 (CI red once: E32 images.env ignored by `*.env`; fixed, 3/3 green), merged (main `ffe69c9`); images run 40191763230: both bases `built=false` (18 s each), spark-job `ffe69c9` → `sha256:583f95a0…` (not deployed; code unchanged). Baseline: Minikube volume 10.29 GB, 4 local `docker.io/library/vnpt-lake-*` images in the node. First apply failed (E33: Airflow Secrets in kustomization before 2.3) → added `AIRFLOW_ADMIN_PASSWORD`, `make k8s-secrets` (fernet/api/jwt keys generated once), applied again.
Observed: 4 Airflow Secrets created early (for 2.3); HMS rolled out from `ghcr.io/beno1tp/vnpt-lake-hms@sha256:9446f7d1…` (pulled with `ghcr-pull`), `HMS schema 3.1.0 exists, nothing to do`; bucket-init from the GHCR spark base: both buckets `exists`.

### 2026-10-25 — 2.0c-1 pull token + Secrets; 2.0c-2 prepared on branch `task2/ghcr`
Did (Bee): classic token `minikube-ghcr-pull` with only `read:packages`; `read -rs` + Secret `ghcr-pull` (dockerconfigjson) in lake, spark-jobs, airflow (E30: my check command). Digests from the run summary (E31: not in the log):
vnpt-lake-spark `3.5.9-iceberg1.11.0-r1` `sha256:aee0deb9…cad5c`; vnpt-lake-hms `3.1.3-hadoopaws3.1.0-r1` `sha256:9446f7d1…b91ff`; vnpt-lake-spark-job `041717a` `sha256:2e5f830e…ca243` (base = spark @aee0deb9…).
Prepared (Claude): `infra/k8s/images.env` (3 refs by digest, read by make) + kustomize `images:` (same digests for HMS and bucket-init; CI test keeps them equal); HMS, bucket-init, landing pod, SparkApplication templates → GHCR + `imagePullSecrets: ghcr-pull` + `IfNotPresent`; landing pod uses the spark-job image (script and expected file baked, only the Excel files are copied in); `make k8s-job-image` kept as fallback (G6) with `LOCAL_JOB_IMAGE`; `make help` shows the k8s-* targets (regex now allows digits); workflow prints digests to the log and Trivy counts (JSON → `.github/scripts/trivy_counts.py`). Tests: 19 passed, ruff clean.

### 2026-10-25 — 2.0b CI green, PR #1 merged, first image build
Did (Bee): pushed `task2/ci`, `gh pr create` → PR #1; `gh pr checks` → 3/3 green (lint + unit tests, kustomize render, Dockerfile checks); `gh pr merge --merge --delete-branch` (main `fc93829..041717a`); `gh run watch 40189208252`.
Observed: images run **success**: base vnpt-lake-hms 3m1s, base vnpt-lake-spark 3m43s (in parallel), spark-job 2m8s → ≈ 9 runner-minutes, ~6 min wall clock. Classic token with `read:packages` created (GHCR does not accept fine-grained tokens).
Next: 2.0c `ghcr-pull` Secrets, digests from the run summary, switch manifests to GHCR.

### 2026-10-25 — 2.0b prepared on branch `task2/ci` (Claude)
Did (Bee): pushed main (`2788fa9..fc93829`, 19 commits — the last direct push to main); `gh auth status` logged in as Beno1tP.
Prepared (Claude, branch `task2/ci`): `pyproject.toml` (ruff 0.16.10 with an explicit rule set E/F/I/B/SIM/DTZ, line 120; pytest paths); `tests/unit/` (17 tests: check parsing, expected files consistent with Task 1 totals and the 24-day range, `expected_for_day`, `source_path`, `event_day`, renderer refuses change-me / generates keys once / URL-encodes / mode 600 using a temp dir, SparkApplication templates valid after placeholder fill, workflow YAML parses); small lint fixes (tz-aware DAG start_date, import split, `with open`, long lines). `.github/workflows/ci.yml` (PR + main: lint + unit tests on Python 3.10, kustomize render with dummy secrets, `docker build --check` ×3). `.github/workflows/images.yml` (main + manual: base images spark/hms built only when their version tag is missing in GHCR; spark-job tagged with the git SHA, FROM the spark base by digest; Trivy CRITICAL/HIGH report-only; digests in the run summary). Actions pinned by commit SHA (checkout v7.0.1, setup-python v7.1.0, setup-buildx v4.4.1, login v4.7.0, build-push v7.4.0, trivy v0.36.0). Local run in a scratch venv: ruff clean, 17 passed.
Decision: Trivy **reports, does not block** for now (apache/hive 3.1.3 and Spark bases carry upstream CVEs we cannot fix); gate later on our own layers.

### 2026-10-25 — Plan v2 (operable/standard approach)
Did (Claude, docs only): merged Claude desktop's plan update into `03_plan.md` v2: Part G (D5: images built by GitHub Actions, stored in GHCR, pulled by Minikube; no laptop builds), Part H (12 production gaps → Task 2 actions: CI/PR, day-generic DQ rules, MinIO service user, backup/restore + register_table drills, maintenance DAG, alerts/remote logs), step table with status, reconciliation §R (desktop plan points already corrected by measurements: webhook on, Allocatable 7268 Mi, containerd, Spark SQL expected counts, initContainer, E25+). D4 (DAG ConfigMap) superseded by D5. Repo is private (GitHub API 404 without login) → GHCR pull token needed.
Rule reconfirmed by Bee: Bee runs every command incl. checks; Claude only writes files/docs and guides. Step 2.3 (Airflow) was **not** executed; its prepared files are revised in 2.3.
Next: 2.0b CI + PR habit.

### 2026-10-25 — 2.2e Big days + 2.3 prepared
Did (Bee): `make sparkapp` + validate for 2024-07-24 and 2024-07-25.
Observed: 07-24 12,970 / 6,485 rows in ~21 s, validate 10/10; 07-25 7,552 / 3,892, validate 10/10 incl. `unmatched_to_chat` 116. No OOM with driver 1g + 1g overhead. `kubectl top` in `watch` caught only the node after the runs (2400 Mi): metrics-server samples every ~15–60 s, too slow for 20-s pods → peak pod memory not measured (TODO: measure in 2.5 drills on a longer run).
Prepared (Claude): Airflow per D4 (DAGs as ConfigMap, official image by digest, no build): `infra/helm/airflow/values-local.yaml` (LocalExecutor, external Postgres secret, fernet/api/jwt secrets by name, redis/statsd/pgbouncer/flower off, worker + triggerer persistence off so the scheduler stays a Deployment without a 100Gi PVC, migration/create-user Jobs without Helm hooks for `--wait`, parallelism 4, resources on all containers); renderer generates fernet/api/jwt keys once (appended to secrets.env) and writes the Airflow secret files; 4 fixed-name Secrets in kustomize; `dags/hello_lake.py`; make `airflow-chart` (CDN + SHA-512, E29), `airflow-dags`, `airflow-install`, `airflow-ui`. `helm template` with these values: 4 Deployments (api-server, scheduler, dag-processor, triggerer) + 2 Jobs, 13 containers all on `apache/airflow@sha256:9df9c8…`, dags volume mounted; Airflow requests 1,632 Mi → node ≈ 3.5 Gi, + one Spark run 3.1 Gi = 6.6 of 7.27 Gi.

### 2026-10-25 — 2.2d Validate on K8s + rerun idempotency (step 2.2 done for one day)
Did (Bee): `make k8s-job-image` (tag 74c5a42), `make sparkapp APP=validate DATE=2024-07-02`, `make sparkapp DATE=2024-07-02` (reload), `make sparkapp APP=validate DATE=2024-07-02 IDEMPOTENCY=yes`; Task 1 `make validate DATE=2026-10-16` (regression for the validator change).
Observed: validate **10/10** (8 checks + 2 column counts, 7 SKIP); reload 12/12, 6/6 (no duplicates); with idempotency **16/16** (last snapshot `overwrite`, added = deleted = 12 / 6); each app ~21 s. Task 1 validate **17/17** (full mode unchanged).

### 2026-10-25 — 2.2c-B First SparkApplication COMPLETED; 2.2d prepared
Did (Bee): `make k8s-job-image` (9.1 s build, one `COPY *.py` layer; `~/.minikube` back to 856 MB); `make sparkapp DATE=2024-07-02`.
Observed: PENDING → RUNNING → COMPLETED in **21 s** (04:18:19 → 04:18:40), 1 attempt; executor `…-exec-1` COMPLETED and removed; driver `Completed` (pod limits/requests 2Gi = 1g + 1g overhead); webhook injected `S3_ENDPOINT`, `AWS_REGION` and envFrom `s3-credentials`; `[bronze] bronze.chatlog: 12 rows read, 12 rows in partition 2024-07-02`, `bot_predict: 6 / 6` = `bronze_daily.json`. Only WARNs: native-hadoop library, k8s client closed at shutdown (both benign). Operator 2.2.1 (Spark 3.5.5 submit) → our 3.5.9 driver: no skew problem.
Prepared (Claude): validator per-day mode (`--expected bronze_daily.json` → expected values of that day; checks without a per-day value print SKIP; full mode unchanged), `columns` added to `bronze_daily.json`; job image now also bakes `tests/` (named build context, tag = last commit of jobs/ tests/ docker/spark-job/); `validate-sparkapp.yaml`; `make sparkapp APP=bronze|validate DATE= [IDEMPOTENCY=yes]`.

### 2026-10-25 — 2.2c-A Task 1 regression after the bronze_ingest.py change
Did (Bee): `make up && make acceptance DATE=2026-10-16`; `make down` (Minikube kept running alongside).
Observed: `[smoke] OK`; bronze 21,224 / 10,729; validate **17/17**, then **23/23** with idempotency; `[acceptance] PASSED`, exit=0. Task 1 behaviour unchanged by `--source` (default path).

### 2026-10-25 — 2.2b landing applied; 2.2c prepared
Did (Bee): `make k8s-secrets` (now also s3.env), `kubectl apply -k` (secret/s3-credentials created), `make landing`; listing pod.
Observed: 36 `[landing] … OK` lines, 21,224 / 10,729 written = read, 18 days each, pod deleted by the trap; listing: **36 files, 4,057,683 bytes** in `s3://landing`.
Prepared (Claude): `bronze_ingest.py --source` (default = Task 1 folder; `s3://landing` → `<table>/dt=<date>/<file>` via pyarrow, `_source_file` = full S3 path; Task 1 code path unchanged); `docker/spark-job/Dockerfile` (FROM thin Spark image + `jobs/*.py`, build context `jobs/` only so secrets never reach the builder); `make k8s-job-image` (tag = last commit of jobs/, refuses uncommitted jobs/, docker build → minikube image load → delete tar cache); `infra/k8s/spark-jobs/bronze-sparkapp.yaml` (driver 1g + 1g overhead, 1 executor 768m + 384m, `restartPolicy: Never`, TTL 1 day, sparkConf = Task 1 spark-defaults with FQDNs, envFrom s3-credentials); `make sparkapp DATE=` (delete old app → apply → poll state → driver `[bronze]` lines → exit non-zero unless COMPLETED). Server dry-run of the SparkApplication accepted.

### 2026-10-25 — 2.2b prepared: landing simulator (Claude)
Did: `jobs/landing_simulator.py` (pandas split by the date written in `CREATED_TIME`, one xlsx per day and table, sheet name kept, replaces the table's landing prefix → idempotent, optional `--expected` check); `infra/k8s/spark-jobs/landing-simulator-pod.yaml` (one-off pod, Spark image, 30 min deadline); `make landing` (apply pod → `kubectl cp` files → exec → delete pod via trap); secret `s3-credentials` in spark-jobs with a **stable name** (`disableNameSuffixHash`: SparkApplications/Airflow templates reference it from outside kustomize); renderer writes `secrets/s3.env`.
Observed (local test in the Spark image, `--network none`, output to scratch): 18 days per table, **every day = `bronze_daily.json`** (pandas split agrees with the Spark SQL counts: two engines), 21,224 / 10,729 written = read; round trip (read all daily files back, sort by ID) → columns and all cells **identical** to the source; 36 files, 4.1 MB. Test output removed.

### 2026-10-25 — 2.2a Spark Operator 2.2.1 installed
Did (Bee): `helm repo add spark-operator https://kubeflow.github.io/spark-operator`; `helm upgrade --install … --version 2.2.1 -f infra/helm/spark-operator/values-local.yaml --wait`.
Observed (Claude, read-only): release `deployed`, chart spark-operator-2.2.1; controller + webhook Running (image `ghcr.io/kubeflow/spark-operator/controller:2.2.1`, 10 Mi / 13 Mi); CRDs sparkapplications + scheduledsparkapplications (v1beta2); SA/Role/RoleBinding `spark-operator-spark` in spark-jobs; `auth can-i create pods` → yes; mutating webhook `spark-operator-webhook` (3 webhooks); no errors in controller log. Node requests 1892 Mi of 7268 (26 %), limits 3036 Mi. Disk: node volume 6.833 → 8.596 GB (+1.76 GB since 2.0e: Postgres, MinIO, busybox, operator images + PV data).

### 2026-10-25 — 2.1 done: HMS restart + cross-namespace test; 2.2 prepared
Did (Bee): applied E28 fix (HMS restarted); busybox `net-test` pod in `spark-jobs`.
Observed: second start → `HMS schema 3.1.0 exists, nothing to do` (no .beeline warning); bucket-init re-created after its TTL and reran. From `spark-jobs`: `hive-metastore.lake.svc.cluster.local` → 10.97.127.92 (= Service ClusterIP), `thrift 9083: open`, MinIO `/minio/health/ready` → `HTTP/1.1 200 OK`. Read-only (Claude): Postgres `VERSION` = 3.1.0, 74 HMS tables; 0 exceptions in HMS log; top: HMS 408 Mi, MinIO 64 Mi, Postgres 69 Mi. **Step 2.1 done.**
Prepared: `infra/helm/spark-operator/values-local.yaml` (chart 2.2.1, jobNamespaces=[spark-jobs], webhook on, 2 workers, resources, metrics off). Chart 2.2.1 present in https://kubeflow.github.io/spark-operator index; its Spark Role grants pods/configmaps/services/PVCs.

### 2026-10-25 — 2.1c Hive Metastore applied (E28)
Did (Bee): `kubectl apply -k infra/k8s`; rollout.
Observed: rolled out in < 1 min; schema-init: `No HMS schema yet -> schematool -initSchema` … `schemaTool completed`, `Metastore schema version: 3.1.0`; warning E28 (`/home/hive/.beeline`, harmless, emptyDir added); HMS log `Starting Hive Metastore Server`, no exception. `kubectl top`: hive-metastore 401 Mi, minio 64 Mi, postgres 69 Mi.

### 2026-10-25 — 2.1b MinIO + buckets applied; 2.1c prepared
Did (Bee): `kubectl apply -k infra/k8s`; rollout + `kubectl wait` job.
Observed: minio-0 Running in ~20 s; bucket-init: 5× `NETWORK_CONNECTION … curlCode: 7` (connection refused while MinIO started; the retry loop absorbed it), then `bucket lakehouse: created`, `bucket landing: created`; PVC data-minio-0 Bound 5Gi. `statefulset.apps/postgres configured` without a pod restart (no-op apply; `kubectl diff` showed no Postgres change).
Prepared (Claude): `lake/hive-metastore.yaml` (Service + Deployment, `Recreate`, schema init as **initContainer** with Task 1's check-then-create script + wait for Postgres, fsGroup 1000 = uid/gid hive (checked with `id`), hive-site.xml secret + core-site.xml ConfigMap via subPath, tcp probes 9083, 768Mi/1Gi, `-Xmx768m`), `lake/hive/core-site.xml` (FQDN endpoint). Deviation from plan: initContainer instead of a separate Job (ordering is automatic; idempotent).

### 2026-10-25 — 2.1a-fix Postgres re-initialised; 2.1b prepared
Did (Bee): `make k8s-secrets` (new Secret hash: password changed), deleted StatefulSet + PVC, `kubectl apply -k`. Checks (Claude, read-only): log only `airflow role and database created`; secret files `-r--r----- 0 999`; `psql -h postgres.lake.svc.cluster.local -U airflow` → `airflow|airflow`. **E27 fixed.**
Prepared (Claude): `lake/minio.yaml` (StatefulSet, Task 1 digest, httpGet probes `/minio/health/ready|live`, 256/512 Mi, PVC 5 Gi; minikube PV dirs are 0777 so no chown step); `lake/bucket-init-job.yaml` + `bucket_init.py` (pyarrow S3FileSystem in the already-loaded Spark image instead of amazon/aws-cli: saves ~650 MB; fsGroup 185, `imagePullPolicy: Never`, TTL 600 s). Dry-run + `kubectl diff`: only new MinIO objects.

### 2026-10-25 — 2.1a Postgres applied (E26, E27)
Did: removed `~/.minikube/cache/images` (1.5 GB; `~/.minikube` back to 856 MB); created `infra/k8s/secrets.env`; `make k8s-secrets`; `kubectl apply -k infra/k8s`.
Observed: postgres-0 Running in 32 s, PVC `data-postgres-0` Bound 2Gi `standard`, headless Service; DBs airflow, metastore, postgres. Network login as `airflow` failed: **E27** (secret file unreadable by uid 999 → empty password). E26 = my quoting mistake in a check command.
Fix: fsGroup 999 + mode 0440 + stricter init script; re-init Postgres (empty) by deleting StatefulSet + PVC.

### 2026-10-25 — 2.1a prepared: secrets renderer + Postgres manifest (Claude)
Did: `infra/k8s/secrets.env.example`, `infra/k8s/render-secrets.py` (+ `make k8s-secrets`) → `infra/k8s/secrets/{postgres.env, minio.env, hive-site.xml}` (git-ignored, 0600, one Secret per consumer); `infra/k8s/lake/postgres.yaml` (headless Service + StatefulSet, Task 1 digest, password via `POSTGRES_PASSWORD_FILE`, 256/512 Mi, PVC 2 Gi); init script creates role + DB `airflow`; `infra/k8s/kustomization.yaml` (secretGenerator/configMapGenerator).
Observed: test with dummy secrets in a scratch copy: renderer refuses `change-me` values, kustomize builds 4 Namespaces + ConfigMap + Secret (hash suffixes) + Service + StatefulSet; `kubectl apply -k --dry-run=server` accepted; `git check-ignore` confirms secrets.env and secrets/ ignored, example tracked.
Next: Bee renders secrets and applies.

### 2026-10-25 — 2.0e Namespaces + thin images in Minikube (step 2.0 done)
Did: `kubectl top node` (191m CPU, 537 Mi); `kubectl apply -f infra/k8s/00-namespaces.yaml` (lake, spark-operator, spark-jobs, airflow Active); `minikube image load` both thin images.
Observed: `minikube image ls` → `docker.io/library/vnpt-lake-spark:3.5.9-iceberg1.11.0-r1`, `docker.io/library/vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1`. Disk: node volume 1.209 → **6.833 GB (+5.62 GB** for 4.16 GB of images: containerd keeps compressed blobs + unpacked snapshots); `~/.minikube` 856 MB → **2.3 GB**: `cache/images/amd64/` holds tar copies (spark 840 MB, hms 719 MB). Total Task 2 disk so far ≈ **+10.9 GB** (kicbase image 1.89 + node volume 6.76 + ~/.minikube 2.3) — already at the top of the 9–10 GB estimate before Airflow/operator/Postgres/MinIO.
Learned: `minikube image load` from the Docker daemon caches a tar in `~/.minikube/cache/images` (a third copy, on the Ubuntu disk); safe to delete once the image is in the node (re-created on the next load).
Next: delete the image cache; 2.1a secrets + Postgres.

### 2026-10-25 — 2.0d-fix Recreate with kubelet.system-reserved (E25)
Did: `minikube delete`; `minikube start … --extra-config=kubelet.system-reserved=memory=3686Mi`; metrics-server.
Observed: no re-download (kicbase + preload cached); Allocatable 11217116Ki → **7442652Ki (7268 Mi)**, `kubepods.slice/memory.max` = 7621275648; 9 kube-system pods Running within 60 s; metrics-server not ready until its first scrapes ("no metrics to serve"). Disk unchanged vs 2.0d.
Learned: Allocatable = Capacity − reserved − eviction threshold; minikube's kubelet config has `evictionHard: 0%`, so my expected 7168 Mi was 100 Mi low.
Next: 2.0e namespaces + load thin images.

### 2026-10-25 — 2.0d minikube start
Did: `minikube start --driver=docker --kubernetes-version=v1.37.0 --memory=8192 --cpus=6`; metrics-server addon.
Observed: kicbase v0.0.51 (507.6 MiB download) + K8s v1.37.0 preload (347.6 MiB); runtime **containerd 2.3.4** (not Docker inside the node); node Ready after ~80 s, 9 kube-system pods Running. Minikube container 538 MiB idle. Disk: `~/.minikube` 856 MB (Ubuntu disk); Docker images 5.685 → 7.576 GB (+1.89 GB kicbase), volumes 74 MB → 1.209 GB (+1.135 GB node volume) → **≈ +3.9 GB** so far. **E25**: Allocatable reports 10.7 GiB / 8 CPU while the container cap is 8 GiB.
Learned: containerd runtime → inspect images with `minikube image ls` / `minikube ssh -- sudo crictl images`, not `docker` inside the node; `minikube image load/build` still work.
Next: recreate with `kubelet.system-reserved` (E25), then 2.0e namespaces + image load.

### 2026-10-25 — 2.0c Task 1 down, minikube v1.39.0 installed
Did: `make down` (volumes kept); resource check; downloaded `minikube-linux-amd64` v1.39.0 + `.sha256`, `sha256sum --check` OK, `sudo install` to `/usr/local/bin`, temp dir removed.
Observed: Docker `CPUs=8 Mem=11486326784` (10.70 GiB); Ubuntu `free -m` total 10,954 MiB, used 2,132; `/` 6.2 GB used; `minikube version: v1.39.0` (commit 7a9f6a84). Binary 136 MiB.
Decision: Docker shows ≥ 9.5 GiB → start Minikube with `--memory=8192 --cpus=6` (`02_config.md` §2 rule); fall back to 7168/4 if Windows/WSL swaps.
Next: 2.0d `minikube start`.

### 2026-10-25 — 2.0b Per-day expected counts
Did: pushed 5 commits (`6151651..2788fa9`); `make up && make smoke` (Task 1, OK); ran `tests/sql/bronze_daily_counts.sql` with spark-sql on Task 1 Bronze.
Observed: 18 days, totals 21,224 / 10,729 / 116, 0 NULL days; all 116 unmatched on 07-25 (same-day = any-day); empty days 07-07, 07-12–14, 07-20, 07-21. Saved to `tests/expected/bronze_daily.json` and `06_validation.md`.
Next: 2.0c `make down`, install minikube v1.39.0, `minikube start`.

### 2026-10-25 — 2.0a Decisions D1–D3
Did: D1 = chart 1.22.0 + Airflow 3.3.2 (chart gates only at >=3.0.0); D2 = A (24 daily runs, 18 load, 6 skip); D3 = expected counts with Spark SQL on Task 1 Bronze (`tests/sql/bronze_daily_counts.sql`), no extra tool. Task 2 tech = Minikube + Airflow + Spark; Trino = Task 3.
Next: 2.0b run the per-day query (Task 1 stack up), then `make down`, install minikube v1.39.0.

### 2026-10-25 — 2.0 Plan reviewed
Did: plan from Claude desktop reviewed in Claude Code; versions checked upstream; docs `01`–`07` created.
Observed: Spark Operator v2.2.1 = Spark 3.5.5 base, v2.3.0+ = Spark 4.0; operator webhook applies volumes/env/ConfigMaps (must stay on); Airflow chart 1.22.0 ships 3.2.2 (no 3.3.x chart yet); minikube v1.39.0 defaults to K8s v1.37.0; data has 18 days in a 24-day window.
Learned: see `01_knowledge.md` [review] notes.
Next: decisions D1–D3 (`02_config.md`), then 2.0 Prep.
