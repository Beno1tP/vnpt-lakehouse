# Task 4 — Process log

One entry per work block. Newest first.

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

### 2026-11-10 — 4.0a deployed: step in Silver (18 days) + E67
Did (Bee): PR #77 (CI green, `make all` 95 passed) → images run 42778808527 (spark-job `3da7f249…`, airflow `6de4988e…`) → bump PR → `make k8s-up` (git b525a70; Airflow rev 15, spark-operator rev 14) → `make backfill FROM=2024-07-02 TO=2024-07-25 REPROCESS=completed` (08:08–08:43 UTC = 15:08–15:43 ICT).
Observed: 24 backfill runs `success`. `trino_reconcile`: 17 runs failed `reconcile` (FAIL keys `step` + `'None'`, 18 → 2), the run after the last day 08:43:05 UTC `210/210 values equal`; `list-runs` shows the failed runs as `success` → **E67**. Side notes: k8s-up reported StatefulSets/plain Secrets `configured` as in Task 2 (`docs/task2/04_process.md` 10-28); Telegram showed run `…08:33:38…_jfTLngWZ` twice (15:33 and 15:35 ICT) while the logs have a separate run 08:35:28: not explained yet (check the 15:35 message in Telegram).
Learned: schema evolution worked without a rewrite of old files (the ALTER ran in the first Silver run; days not yet rewritten read `step` = NULL, which is exactly what the half-way reconciles saw). A check that spans the whole lake needs a consistent point.
Prepared (Claude, branch `task4/e67-reconcile-guard`): sensor `wait_pipeline_idle` (+ `chatbot_bronze` running/queued via `ti.get_dr_count`), leaf `verdict`, CI DAG rule. Predicted: lint clean, unit tests pass (my local run); the DAG import + new rule run in CI inside the image.

### 2026-11-10 — 4.0 checks + UC4 profile + 4.0a prepared (`step` in Silver)
Did (Bee): disk + version checks; two pandas profiles of the Excel files (counts and labels only).
Observed: C: 60 G free (88 %), node `/var` 19 G; chart `kube-starrocks` 1.11.8 (APP VERSION `4.1-latest`: the chart default image tag is `4.1-latest` and the FE default `-Xmx8192m`, read in the v1.11.8 `values.yaml` → override both, check `helm template`); `fe-ubuntu:3.5.21@sha256:cbc2460a…d9fc6`, `be-ubuntu:3.5.21@sha256:6ed9e75b…cbe9f`; dbt-starrocks 1.12.2 (`dbt-core>=1.9.0`, Python <3.15), dbt-core 1.12.6. UC4 "kịch bản": predict `STEP` has 2 values: `406665` 10,494 rows (all `xin_chao`; 10,378 = the `/home` messages + 116 = the predict rows without a chat row), `406666` 235 (94 xin_chao · 107 fallback · 34 bot_khong_hieu, no `/home`: typed text); `BOT_ID` 1 value; `META_DATA` JSON but only 276 rows with a key (`button_variables`, no sub-keys); `SOURCE` of predict = `action`.
Decided (Bee): UC4 period = the whole dataset; base = all 10,729 predict rows (same base as `final_intent`); `STEP` enters **Silver** (option A: Gold must not read Bronze).
Prepared (Claude, branch `task4/4.0a-silver-step`): `silver_transform.py` adds `step` to `nlu_prediction` and `user_message`; existing tables get `ALTER TABLE … ADD COLUMN step string` (Iceberg schema evolution, idempotent `add_columns_sql`) before the write; summary + validator check the per-day `step` split; 2 new rules (`nlu_prediction.step_not_null`, `user_message.step_iff_prediction`) → 31 checks per day; `expected_silver.py` counts `step` and asserts the profiled totals; Trino `reconcile.sql` + `lake_reconcile` handle `step.<value>` like `final_intent` → 210 values. Predicted (my local runs): lint clean; expected generation into a scratch folder: step totals as profiled, every other key unchanged on all 18 days; unit tests pass except the one that needs the regenerated `silver_daily.json`.
Next (Bee): regenerate the expected files → `make all` → PR → images (airflow + spark-job) → bump → `make k8s-up` → backfill all days (`REPROCESS=completed`) → `make mode-query` → `make trino-check` (210/210) → `make mode-pipeline`.

### 2026-11-10 — decision IDs renumbered
Did (Claude): plan of 11-06 kept as is (Bee 11-10: StarRocks + dbt Gold incl. `gold_uc4…uc8` stay in Task 4); proposed decisions D13–D18 renumbered **D15–D20** because Task 3 used D13 (UTC schedule) and D14 (Minikube test loop). Task 5 docs not changed: its D19–D20 now collide with Task 4's and its references to "Task 4 D13/D18" mean D15/D20; renumber when Task 5 starts.
Next (Bee): before 4.0, the checks in the 11-06 entry below.

### 2026-11-06 — plan written
Did (Claude): input plan reviewed (`03_plan.md` §R, 11 points); `docs/task4/01–07` created; proposed decisions D13–D18.
Next: Task 3 green first. Before 4.1: `kubectl describe node minikube` (requests), `minikube ssh -- df -h /var`, version checks in `02_config.md` §1.
