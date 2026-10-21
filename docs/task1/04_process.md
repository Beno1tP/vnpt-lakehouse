# Task 1 — Process log

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

### 2026-10-21 — 1.12 Standard build (thin images, secrets as files, data-only volumes)
Did: after Bee's review (rollback, schema-init branches, one JDBC driver, health checks, `pipefail`, digests, schematool logs, `# syntax`, password leak) rewrote the plan (`08_standard_build.md`), pre-checked HMS-with-`hive-site.xml`, Compose secret mount and the init script branches; Bee built and tested on branch `standard-build`: 2 thin images (`make build` 42.3 s), secrets rendered by `make secrets`, from-zero test in a separate Compose project, then switched the demo stack.
Observed: from zero 19.1 s; HMS healthy, schema-init took the "no schema" branch (test) and the "exists" branch (demo); password hits inspect/ps/logs 0/0/0, Postgres env 0; Bronze 21,224 / 10,729, no Ivy step; demo kept 7 snapshots and time travel; images 5.69 GB, volumes 2 (63 MB, data only). Also found E23: after a reboot HMS stays `Exited (127)` (bind mounts not ready) → `make up`.
Learned: dependencies belong in images, configuration outside, credentials in files, data in volumes; test a from-zero path in an isolated Compose project to protect demo data; never put backticks in an unquoted heredoc; `grep -c X` inside the same container matches itself (use `[X]…`).
Next: slides for Thursday; Silver; Task 2 (CI → GHCR, K8s).

### 2026-10-18 — 1.11 README, learning guide, Task 1 closed
Did: `docs/task1/README.md` (architecture, services, run, versions, design decisions vs alternatives, footprint, commands, results, drills, lessons), `docs/task1/07_learning_guide.md` (per-tool understanding, patterns, checked reading list, likely questions), root `README.md`. Corrected an external draft (make clean scope, measured vs limit RAM, unproven claims).
Observed: Spark peak 1.41 GiB during `make bronze`; Bronze 10.4 MB on MinIO.
Next: slides + presentation; then Silver; optional vhdx compaction; Task 2.

### 2026-10-18 — 1.10 Failure drills
Did: Drill A (HMS stopped during `make bronze`); Drill B (Spark killed between the chatlog and bot_predict commits); recovery rerun. Details in `06_validation.md`.
Observed: A → exit 2, clear catalog error, no snapshot. B → exit 137, tables on different batches, no orphan files; rerun → one batch `5717899a…`. Earlier B attempts missed (job ~10 s, commit window ~2.7 s). Correction: the attempt with `-T` but without `< /dev/null` (08:05 local) **did run** inside the container (batch `44ab84d3…`); only the suspended WSL client never wrote the log, so "nothing ran" was a wrong reading. Lesson: check the table's snapshots, not only the client's log. 7 chatlog data files now = 7 loads, 1 current (see README time-travel note).
Learned: atomic per table, no multi-table transactions; failed job must exit non-zero so the orchestrator retries; background `docker exec` needs `-T` and `< /dev/null`.
Next: Task 1 README + architecture; optional vhdx compaction.

### 2026-10-18 — 1.9 From-zero test (`make clean` → `make up` → `make bronze`)
Did: recorded snapshots before wiping (append 1007854840075047324 → overwrite 6835313269239717841 → overwrite 2906472628333106691 → delete 3918703497401125186 → overwrite 235462788859916261); `make clean`, removed downloaded jars (simulates a fresh clone; only `.env` kept).
Observed: 1st try failed (E21: Docker created root-owned dirs for missing jar mount sources → old 9.4 JDBC driver → SCRAM auth error). Pre-test also found E20 (git-ignored jars dir missing in a clone). After fixes: `make up` 49.5 s from nothing, `HMS schema created`, `pydeps installed`, all services Up / Exited (0).
Learned: a from-zero test finds bugs that a long-lived dev environment hides; discard tool output only where needed, and keep a `-dryRun`/verbose path for diagnosis.
Bronze on the fresh stack: 21,224 / 21,224 IDs / 89 senders; predict 10,729; exactly 1 snapshot `append 21224` → **from-zero test passed**.
Next: failure drill, README.

### 2026-10-18 — 1.8 Idempotent HMS schema init + restart test
Did: added `hms-schema-init` (patch applied with `git apply`, no nano paste); `make down` → `make up` → `make bronze DATE=2026-10-16`.
Observed: `HMS schema exists, nothing to do`; all 10 containers recreated, one-shots exit 0; Bronze reload 21,224 / 10,729 (new overwrite snapshot, no duplicates).
Learned: `-Xmx` in shared `SERVICE_OPTS` also applies to the init container → its `mem_limit` must fit it (1g). YAML anchors (`&hms_opts` / `*hms_opts`) avoid duplicating the DB options.
Next: optional `make clean` from-zero test, vhdx compaction, failure drill, Task 1 README; Silver stretch.

### 2026-10-18 — 1.7 Cleanup: no custom images left
Did: removed images `vnpt-lake-hive-metastore`, `vnpt-lake-spark`; `git rm` both Dockerfiles; `make up` no longer uses `--build`; `.env` pins MinIO and aws-cli by digest; dropped `lake.tmp`; deleted the duplicate `2026-10-17` Bronze partition (both tables).
Observed: Bronze = only `2026-10-16` (21,224); 4th snapshot `delete` 21,224 (metadata-only, file kept for time travel until `expire_snapshots`). `docker system df`: images **4.98 GB** (7, all in use), build cache 0, volumes 0.54 GB (pg, minio, ivy-cache, pydeps). Start of session: images 11.73 GB + cache 6.19 GB.
Learned: a digest pin is the content hash, a tag is a movable pointer.
Gap found: `IS_RESUME=true` is hard-coded, so on **empty** volumes (`make clean`) HMS would skip schema init and fail. Needs an idempotent schema-init step before a true from-zero test.
Next: restart test, idempotent HMS schema init, from-zero test, vhdx compaction.

### 2026-10-16 — 1.6 Bronze ingest, validation, idempotency, time travel
Did: `make bronze DATE=2026-10-16` (twice), `DATE=2026-10-17`; validation SQL (counts/labels only); `.snapshots`, `.files`, `.all_data_files`; `VERSION AS OF` / `TIMESTAMP AS OF`.
Observed: all Bronze checks match `06_validation.md` (results table there). Snapshots: append → overwrite (deleted = added = 21,224) → overwrite (deleted NULL, total 42,448). Time travel returns the first `_batch_id`.
Learned:
- Idempotency = the job may run more than once (Airflow retries/backfills) but the table ends as if it ran once. `overwritePartitions()` = dynamic partition overwrite: replaces only partitions present in the new DataFrame, committed atomically as one snapshot.
- Time travel reads an older snapshot's file list; it works because overwritten files are kept until `expire_snapshots` (disk grows meanwhile).
- The `Q` shell wrapper hid stderr, so an invalid query (`<ID1>` placeholder) printed nothing: drop `2>/dev/null` when output is empty.
Next: cleanup (old images, Dockerfiles, `--build`, digests, `lake.tmp`, 10-17 test partition), from-zero test, vhdx compaction.

### 2026-10-16 — 1.5 Spark on the official image (no build)
Did: `spark` now `image: apache/spark:3.5.9-scala2.12-java17-python3-ubuntu`; `spark-defaults.conf` moved to `infra/compose/spark/` and mounted, with `spark.jars.packages` (iceberg-spark-runtime-3.5_2.12 + iceberg-aws-bundle 1.11.0) and `spark.jars.ivy=/opt/ivy` (named volume; the image has `HOME=/nonexistent`). One-shot `spark-init` (same image, root) chowns the Ivy volume to uid 185 and pip-installs `infra/compose/spark/requirements.txt` (pins copied from the old image: pandas 2.2.3, openpyxl 3.1.5, pyarrow 17.0.0, numpy 2.2.6 …) into volume `pydeps` → `PYTHONPATH=/opt/pydeps`; reinstalls only if requirements change (`cmp`).
Observed: first `spark-sql` run → Ivy `2 artifacts copied` (109 MB); table from the old Spark still readable (data lives in MinIO + HMS); insert → 2nd snapshot `append`.
Hiccup: pasting a long YAML block into `nano` through Windows Terminal silently dropped characters on 5 lines (one left `AWS_SECRET_ACCESS_KEY:` empty — valid YAML, would have caused a 403 later), plus a typo in a bind-mount source (`spark-defauls.conf` — Docker would create an empty dir there). `docker compose config` catches schema errors only. Heredoc (`cat > f <<'EOF'`) pastes were intact.
Learned: `spark.jars.packages` = Ivy resolves Maven coordinates at startup and adds jars to driver/executor classpath — replaces `ADD *.jar` in a Dockerfile.
Next: step 5 bronze ingest + validation; then delete the old custom images.

### 2026-10-16 — 1.4 E17 fixed; HMS on the official image (no build)
Did:
- Confirmed E17 in `/tmp/hive/hive.log` (the real HMS log; `docker logs` only shows the console): Guava `NoSuchMethodError` inside `S3AFileSystem.initialize` during `create_database`.
- Added one-shot `jars-init` (`curlimages/curl:8.22.0`, script `infra/compose/fetch-jars.sh`) → downloads `hadoop-aws-3.1.0`, `aws-java-sdk-bundle-1.11.271`, `postgresql-42.7.4` into git-ignored `infra/compose/jars/`, SHA-1 checked against Maven Central.
- `hive-metastore` now `image: apache/hive:3.1.3` + jars and `core-site.xml` bind-mounted; `IS_RESUME=true` (E07).
- `.env.example` rewritten: digest pins (MinIO, aws-cli), `HADOOP_AWS_VERSION=3.1.0`, `CURL_IMAGE`, correct comments and `RAW_DATA_DIR`.
Observed: jars in `/opt/hive/lib` = 3.1.0 / 1.11.271 / postgres.jar (42.7.4); smoke test OK (2 rows, snapshot `append`); MinIO shows `tmp.db/t/{metadata/*.metadata.json, *.avro, data/*.parquet}`. Harmless warnings: SLF4J multiple bindings (HMS); `ObjectStore … schema version 2.3.0` in Spark = Spark's own embedded Derby metastore for `spark_catalog`, not our HMS.
Learned: transitive dependency drift inside a patch line (Hadoop 3.1.3 → Guava 27) breaks binary compatibility; the client only sees "connection lost", so read the server log.
Next: 4d Spark on the official image (`spark.jars.packages` + Ivy volume, pip deps in a volume).

### 2026-10-16 — 1.3b Repo audit + disk cleanup (Claude Code session)
Did: read handoff docs vs real files; audited disk; removed `apache/hive:4.0.1`, `apache/airflow:3.3.2`, all build cache, volume `airflow_postgres-db-volume` (Bee typed the commands).
Observed:
- Before: images 11.73 GB, build cache 6.19 GB, volumes 0.12 GB. After: images 6.06 GB, cache 0 B, volumes 0.05 GB → **~11.9 GB freed inside Docker**.
- On C: `docker_data.vhdx` 22.9 GB, Ubuntu `ext4.vhdx` 6.4 GB (holds 4.7 GB). The vhdx files do not shrink until compacted (planned after E17).
- Running HMS container has `hadoop-aws-3.1.3.jar` + `aws-java-sdk-bundle-1.11.271.jar`, image has `hadoop-common-3.1.0.jar` → E17 mismatch confirmed to exist (cause not yet proven). HMS 3.1.3 also ships its own `postgresql-9.4.1208.jre7.jar` next to our `postgres.jar` (42.7.4).
- Spark image Python is **3.10.12** (not 3.11 as in the matrix); `HOME=/nonexistent` for user `spark`.
- Doc/file drift: MinIO and aws-cli still `:latest` (not digest-pinned); `IS_RESUME` still commented out (E07 not applied); `.env.example` comments describe Hive 4.0.1/Hadoop 3.3.6 and `RAW_DATA_DIR=/home/user/...`.
Learned: deleting images frees space inside the WSL virtual disk only; Windows gets it back after `compact vdisk`.
Next: step 3, replace custom images with pulled images + compose config.

### 2026-10-15 — 1.0 Kick-off
Did: confirmed with the leader that tasks 1–5 are standalone mini-projects on the same data. Chose Docker Compose for Task 1. Prepared knowledge/config/plan docs and starter config.
Observed: data profile → chatlog 21,224 rows / predict 10,729 rows / 89 senders / 18 days (see `06_validation.md`).
Learned: why Task 1 needs a catalog (Hive Metastore) even without Trino yet: atomic pointer swap; Hadoop catalog unsafe on S3.
Next: 1.0 prerequisites, 1.1 data copy, 1.2 `make up`.
