# Task 3 — Process log

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

### 2026-11-10 — 3.7 decided + freeze prepared
Decided (Bee): from-zero acceptance deferred to one full-stack run after Task 5 (plan §I option b; results stay alive for the final presentation, Q53); full freeze like Task 2; README first, then git tag `task3-v1`.
Prepared (Claude, branch `task3/3.7-freeze`): `catalog_tool` tag/verify-tag/info accept the 3 Silver tables (validated names; expected rows from `silver.json`; `register` stays Bronze-only); `FREEZE_TABLES_2` / `FREEZE_TABLES_3` (Bronze + Silver) selected by `TASK`; `make trino-tag-check TASK=3` (`tools/trino_tag_check.py`, same expectations as catalog_tool, unit-tested); `demo-task3` (demo-task2 + Trino); `demo-check DEMO_TASK=3` adds Trino ready + `/v1/info` 200 and checks the tag **through Trino**: a Spark freeze-check (~2.6 Gi) would not fit next to Trino (2 Gi) in that profile; `EVIDENCE_DIR` per task. Predicted (my local runs): lint clean, tests pass, recipes `bash -n` OK for DEMO_TASK 2 and 3, variable expansions printed.
Next (Bee): PR → images run (catalog_tool) → bump spark-job → `make k8s-up` → `make freeze TASK=3` → `make evidence TASK=3` + `make evidence-export TASK=3` → `make demo-task3` → `make demo-check DEMO_TASK=3` → measure → `make mode-pipeline`.

### 2026-11-10 — 3.5 done: plans, metadata, two engines, time travel, read-only
Did (Bee): `make catalog CMD=info TABLE=bronze.chatlog` (Spark) → `make mode-query` → `make trino-explore` (01:44 UTC = 08:44 ICT) → `sed` of the Silver snapshot lines → `ANALYZE` → time travel to the first Silver snapshot → `make mode-pipeline`.
Observed (`07_explain_plans.md`): partition pruning 18 splits → 1 split for one day; Spark and Trino name the same current `metadata.json` of bronze.chatlog (00079-8085977d…, 60 snapshots); Silver 38 snapshots (2 hand runs + 2 × 18); tag `task2_v1` reads 21,224 while `main` moved on; first Silver snapshot reads 86 rows (only 07-09); SHOW STATS without distinct counts; ANALYZE denied; `session_id` is empty in every chat row (data fact).
Learned: Spark's `collect()` formats timestamps in the pod zone (UTC) even with the session zone set to ICT; Trino with `SET TIME ZONE` shows ICT. Label zones. Next: 3.7 close (README, freeze as `task3_v1`, from-zero decision).

### 2026-11-10 — 3.5 prepared: make trino-explore
Prepared (Claude): `sql/trino/explore.sql` (session zone Asia/Ho_Chi_Minh; EXPLAIN ANALYZE without/with `event_date` filter; `$snapshots`, `$partitions`, `$files` of silver.chat_message; bronze.chatlog `$metadata_log_entries` + `$snapshots` to compare with Spark's `make catalog CMD=info`; time travel `FOR VERSION AS OF 'task2_v1'`; `$refs`; `SHOW STATS`), make `trino-explore` (query mode only; output saved outside Git), `07_explain_plans.md` rewritten to these sections. Adapted from the template: Spark comparison on Bronze (catalog_tool allows only Bronze names, no code change); MERGE demo kept as knowledge (would need new Spark write code). Predicted: recipe `bash -n` OK (my local run).
Next (Bee): `make catalog CMD=info TABLE=bronze.chatlog` (pipeline mode) → `make mode-query` → `make trino-explore` → time travel by id + ANALYZE → `make mode-pipeline`.

### 2026-11-10 — 3.4 done: Trino = pandas = Spark on every Silver number
Did (Bee): PR #68 merged → `make mode-query` → `make trino-check` → `make mode-pipeline`.
Observed: `191/191 values equal (Trino = pandas = Spark-validated), 18 days`; final_intent per day and in total (34 · 107 · 10,588) equal; the `CROSS JOIN UNNEST` unpivot ran first time; cluster back in pipeline mode.
Learned: three independent computations agree: pandas on the Excel files (3.0), Spark's validator on Silver (3.2), Trino SQL on the same Iceberg files (3.4). Plan goal met. Next: 3.5 explore + explain.

### 2026-11-10 — 3.4 prepared: make trino-check
Prepared (Claude, branch `task3/3.4-trino-check`): `sql/trino/reconcile.sql` (every Silver number per `event_date` in Trino SQL, unpivoted with `CROSS JOIN UNNEST` into day,key,value; final_intent as `final_intent.<intent>`), `tools/trino_check.py` (stdlib; per-day vs `silver_daily.json`, totals vs `silver.json`, missing or unexpected keys fail), make `trino-check` (refuses unless Trino has a ready replica; runs the SQL in the coordinator with `--output-format CSV_UNQUOTED`, pipes into the comparer; the Makefile's bash `-o pipefail` makes a Trino error fail the target); 3 unit tests. Predicted: lint clean, tests pass (my local run).
Next (Bee): `make all` → PR → merge → `make mode-query` → `make trino-check` → `make mode-pipeline`.

### 2026-11-10 — 3.3 done: Trino reads the lake, read-only twice
Did (Bee): PR #66 → `make k8s-up` (17:42 UTC = 00:42 ICT: namespace trino, `MINIO_TRINO_SECRET`, 2 Secrets, minio-users) → `make s3-access-test` → `make mode-query` → `make trino-install` (17:46:34 UTC: chart 1.44.0 `OK`, release trino rev 1) → image check + 5 SQL statements + one write attempt.
Observed: 4 × 6/6 access test (trino reads lakehouse only); pod image `docker.io/trinodb/trino@sha256:db58cc93…0534` (pinned digest); schemas bronze, silver (+ default, information_schema, system); counts 21,224 / 21,224 / 10,729 / 10,613 = pandas = Spark; `event_ts` = `timestamp(6)`; `CREATE TABLE` → `Access Denied` (17:51:35 UTC).
Learned: one copy of the data, two engines: Spark writes, HMS points, Trino reads the same Iceberg files with its own Thrift client and native S3 file system. Next: 3.4 `make trino-check` (all Silver numbers via Trino vs `silver.json`).

### 2026-11-10 — 3.3 Trino prepared
Checked (Claude, public reads): chart index → 1.43.0 and 1.44.0 = Trino 483, 1.45.0 = 484 → **1.44.0**; its SHA-256 equals GitHub's asset digest and the index digest (unlike E58); `trinodb/trino:483` index digest `sha256:db58cc93…0534` (Docker Hub). Chart facts: release `trino` → `deploy/trino-coordinator`, `svc/trino`; `server.workers: 0` removes the worker Deployment; catalogs as properties with `${ENV:…}`.
Prepared (branch `task3/3.3-trino`): namespace `trino`; MinIO user `trino` (`lake-trino-read.json`: list + read `lakehouse/` only) + Secrets `trino-s3-credentials` (namespace trino) and `trino-access-test-credentials` (AWS names, for the access test); `infra/helm/trino/values-local.yaml` (single node, heap 1536M, query 600 MB, 2 Gi Guaranteed, catalog `lake` with `iceberg.security=read_only`); make `trino-chart`, `trino-install` (refuses unless in query mode), `trino-cli`, `trino-ui` (port 8081), `mode-query`, `mode-pipeline`; `s3-access-test` gets a positive op "read lakehouse" and profile `trino`; tests. Predicted: lint clean, tests pass, recipes `bash -n` OK, kustomize render with dummy secrets OK (my local runs).
Next (Bee): `make all` → PR → merge → `make k8s-up` → `make s3-access-test` → `make mode-query` → `make trino-install` → `make trino-cli`.

### 2026-11-10 — 3.2 done: Silver in the DAG, validated, idempotent (E63)
Did (Bee): PR #63 → images run 42600394002 → bump PR #64 (airflow `a46867a0…`, spark-job `fe695b8d…`) → `make k8s-up` (10:29 UTC = 17:29 ICT) → `make sparkapp APP=validate_silver` for 07-09 and 07-25 (16:02–16:03 UTC = 23:02–23:03 ICT) → overnight `nohup bash tools/backfill_twice.sh` (laptop awake): pass 1 16:13 → ~16:43 UTC, pass 2 → 17:15 UTC (00:15 ICT).
Observed: validate_silver 28/28 on both hand days, `event_ts` = `timestamp_ntz` in all 3 tables; both passes 0 failed backfill runs, `validate_silver 28/28` on all 18 data days, scheduler 0 restarts; the script's verdict said DIFFERENT because it compared log noise (E63); comparing only data lines: 155 = 155, identical.
Learned: Silver now runs daily inside the same DAG (`check_landing → bronze → validate → silver → validate_silver → notify`) and is idempotent per day like Bronze. Next: 3.3 Trino (query mode).

### 2026-11-09 — 3.1 verified on Minikube; 3.2 prepared (validator + DAG)
Did (Bee): `make all` (81 passed) → PR #61 → images run 42597542204 → bump PR #62 (spark-job `f55ebb2f…`) → `make k8s-up` → `make sparkapp APP=silver DATE=2024-07-09` (17:11:19–17:11:52 ICT = 10:11 UTC) and `DATE=2024-07-25` (17:14:58–17:15:19 ICT = 10:14–10:15 UTC).
Observed: both summaries **identical** to `silver_daily.json` (07-09: 86/43/43, final_intent 6/18/19, internal 16 (8 client), /home 13; 07-25: 7,552/3,892/3,776, 116 without chat row). `make sparkapp` filtered the `[silver]` lines (fixed in 3.2: the grep knows silver/validate_silver).
Prepared (Claude, branch `task3/3.2-silver-dag`): `tests/sql/silver_rules.sql` (12 day-generic rules), `jobs/validate_silver.py` (exact counts vs `silver_daily.json`, rules, `event_ts`=`timestamp_ntz` / `event_date`=`date` per table), `dags/templates/validate_silver_sparkapp.yaml` (name `validate-silver-D`: K8s names have no `_`; `make sparkapp` maps `_`→`-`), DAG `… validate >> silver >> validate_silver >> notify` (silver retries 2, validate_silver retries 1), `dag-report` learns the Silver lines; tests (predicted 85 pass, my local run).
Next (Bee): `make all` → PR → images (airflow + spark-job) → bump both → `make k8s-up` → validate_silver by hand on 07-09/07-25 → backfill ×2.

### 2026-11-09 — 3.1 Silver job prepared
Prepared (Claude, branch `task3/3.1-silver-job`): `jobs/silver_transform.py --ingest-date D` (Spark SQL built by pure functions: one-`_batch_id` check across both Bronze tables; `to_timestamp_ntz` (D8); dedup by `row_number()`; flags; `final_intent` with the row's threshold (D9); `has_chat_row` against all Bronze chat IDs; `user_message` left join; refuses rows with a null `event_ts` or another `event_date` (else `overwritePartitions()` would replace another day, D7 edge) and a chat id with two predictions; writes 3 tables partitioned by `event_date`, property `source.timezone=Asia/Ho_Chi_Minh`); `dags/templates/silver_sparkapp.yaml` (driver 640m + 384m = 1 GiB, R13; event logs on); 5 unit tests (CI has no Spark, so the SQL text and input validation are tested; numbers are checked on Minikube). Caught in review before any run: an `IN (SELECT …)` in the SELECT list, which Spark rejects → left join.
Next (Bee): PR → images run → bump spark-job → `make sparkapp APP=silver DATE=2024-07-24` → counts vs `silver_daily.json`.

### 2026-11-09 — 3.0 expected Silver numbers
Decided (Bee): D7–D12 accepted; D13 schedule stays UTC (show ICT); D14 test Silver on Minikube one day by hand (no Compose loop).
Did: Claude wrote `tools/expected_silver.py` (pandas on the Excel files: parse `CREATED_TIME` to a naive wall clock, flags, `final_intent`, `has_chat_row`, `user_message` left join; fails unless totals = Task 1) + 5 unit tests; checked the 2 internal senders = SENDER_IDs containing `@` (counts only, no IDs). Bee ran it.
Observed: `PASS: totals equal the Task 1 numbers`; 18 days; new: internal sender rows 32 (16 client), user_message 10,613 with 10,613 predictions; 07-25 holds all 116 unmatched predict rows.
Next: 3.1 Silver job (`DESCRIBE lake.bronze.bot_predict` not needed: columns read from the Excel header: `INTENT_CONFIDENCE`, `NLU_THRESHOLD`, `UPDATED_INTENT`, `ID_CHATLOG`).

### 2026-11-06 — plan written
Did (Claude): input plan reviewed against the repo (`03_plan.md` §R, 14 points); `docs/task3/01–07` created. Proposed decisions D7–D12 for Bee to confirm.
Next: before step 3.0: `minikube start` (E40/E53), `kubectl get pods -A`, `kubectl describe node minikube | grep -A8 "Allocated resources"`, `minikube ssh -- df -h /var`, `df -h /mnt/c`; `DESCRIBE lake.bronze.bot_predict` (confidence and threshold column names).
