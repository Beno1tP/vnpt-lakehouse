# Task 1 — Command paths (what each WSL command does, and where errors branched)

How to read it:
- `Step Xn` = one stage. Under it: the commands (typed in **WSL Ubuntu**, from `~/vnpt-lakehouse`), then **Expect**.
- `├── Error Eyy` = what went wrong at that step (details in `05_errors_and_fixes.md`).
- `│   └── Fix` = the commands that fixed it, then `→ back to Step Xn` (retry) or `→ new Step Xm` (an extra step was added).
- Commands are typed in WSL; only Windows-side actions (Docker Desktop, vhdx compaction) are marked **(Windows)**.

Shortcuts used everywhere:
```bash
cd ~/vnpt-lakehouse
DC="docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env"   # the Compose project of Task 1
Q() { $DC exec spark /opt/spark/bin/spark-sql --master local[1] -e "$1" 2>/dev/null | grep -v -E "^Time taken|^::|Ivy|^\s|artifacts|found org|added as"; }   # run Spark SQL, hide noise
```

---

## Part A — The path Task 1 actually took (lab version, 2026-10-15 → 10-21)

```
Step A0: Enter Ubuntu and check the environment
   wsl                                            (from Windows cmd: open Ubuntu)
   whoami; pwd                                    who am I, where am I            Expect: quang, /home/quang
   docker version --format '{{.Server.Version}}'  is Docker Engine reachable?     Expect: 29.8.1
   |
   ├── Error: "docker could not be found in this WSL 2 distro"
   │   │   Root cause: Docker Desktop's WSL integration reloads after the engine starts
   │   └── Fix: exit, then wsl again (or Docker Desktop → Settings → Resources → WSL integration ON)
   │        → back to Step A0
   |
Step A1: Measure disk, then clean up (images, build cache, old volume)
   docker system df                               disk used by images / containers / volumes / build cache
   docker image rm apache/hive:4.0.1              remove an image no longer used
   docker image rm apache/airflow:3.3.2
   docker builder prune -a -f                     delete all build cache (6.19 GB)
   docker volume rm airflow_postgres-db-volume    delete an old volume
   docker system df                               Expect: images 6.06 GB, cache 0 B
   |
Step A2: Configure (once)
   cp infra/compose/.env.example infra/compose/.env   create the secrets/versions file (git-ignored)
   git check-ignore -v infra/compose/.env             prove Git ignores it
   |
   ├── Error E01: "pull access denied for minio/minio, minio/mc"
   │   │   Root cause: MinIO stopped publishing community images
   │   └── Fix: MINIO_IMAGE = cgr.dev/chainguard/minio@sha256:…, bucket via amazon/aws-cli, busybox chown
   │        → back to Step A3
   |
Step A3: Start object storage and create the bucket
   $DC up -d minio-perms minio minio-init         start MinIO (+ one-shot permission fix and bucket creation)
   $DC logs minio-init                            Expect: "make_bucket: lakehouse" or "bucket exists"
   |
Step A4: Start Postgres (the Metastore's database)
   $DC up -d postgres
   $DC ps postgres                                Expect: Up (healthy)
   |
Step A5: Start the Hive Metastore (catalog)
   $DC up -d hive-metastore
   $DC logs hive-metastore 2>&1 | grep -v -i password | tail -5      (logs contain the DB password: filter!)
   |
   ├── Error E16 (at Step A6): CREATE TABLE → "Invalid method name: 'get_table'"
   │   │   Root cause: HMS 4.0.1 dropped get_table; Spark 3.5 uses the Hive 2.3 client
   │   └── Fix: HIVE_IMAGE=apache/hive:3.1.3, wipe the old schema
   │          $DC down; docker volume rm vnpt-lake_pg-data; $DC up -d postgres hive-metastore
   │        → back to Step A5
   │
   ├── Error E17 (at Step A6): CREATE NAMESPACE → "MetaStoreClient lost connection"
   │   │   Diagnose on the SERVER, not the client:
   │   │     $DC exec hive-metastore sh -c 'grep -n -i -E "exception|error|caused by" /tmp/hive/hive.log' | grep -v -i password | head
   │   │     $DC exec hive-metastore sh -c 'sed -n "63,+25p" /tmp/hive/hive.log' | grep -v -i password
   │   │   Root cause: NoSuchMethodError (Guava) in S3A: hadoop-aws 3.1.3 needs Guava 27, image has Guava 11/19
   │   └── Fix: HADOOP_AWS_VERSION=3.1.0; jars downloaded by jars-init (SHA-1 checked) and bind-mounted
   │          mkdir -p infra/compose/jars; $DC up jars-init; $DC up -d hive-metastore
   │        → back to Step A6
   │
   ├── Error E07: a new HMS container re-runs initSchema → fails "already exists" → restart loop
   │   └── Fix: IS_RESUME="true"  → later replaced by Step A12 (check-then-create)
   |
Step A6: Smoke test (the whole chain: Spark → Iceberg → HMS → Postgres, and → MinIO)
   $DC exec spark /opt/spark/bin/spark-sql --master local[1] -e "
     CREATE NAMESPACE IF NOT EXISTS lake.tmp;
     CREATE TABLE IF NOT EXISTS lake.tmp.t (id INT, name STRING) USING iceberg;
     INSERT INTO lake.tmp.t VALUES (1,'a'),(2,'b');
     SELECT * FROM lake.tmp.t;
     SELECT snapshot_id, operation FROM lake.tmp.t.snapshots;"
   Expect: 2 rows, 1 snapshot "append"
   $DC run --rm --no-deps minio-init "aws --endpoint-url http://minio:9000 s3 ls --recursive s3://lakehouse/warehouse/"
   Expect: metadata/*.metadata.json, *.avro, data/*.parquet
   |
Step A7: Spark on the official image (Iceberg via spark.jars.packages, pip deps via spark-init volume)
   (edit compose: spark-init + spark services, volumes ivy-cache/pydeps)
   $DC config --quiet && echo compose-ok          validate the YAML
   $DC up -d spark                                runs spark-init (pip) then recreates spark
   $DC logs spark-init | tail -3                  Expect: "pydeps installed"
   |
   ├── Error E18: config → "additional properties 'conditiofully' not allowed"
   │   │   Root cause: long paste into nano dropped characters (other broken lines were still valid YAML)
   │   └── Fix: retype lines; check with grep -n …; use heredocs / patches instead of nano pastes
   │          sed -i 's#spark-defauls.conf:#spark-defaults.conf:#' infra/compose/docker-compose.yml
   │        → back to Step A7
   |
Step A8: Load Bronze
   make bronze DATE=2026-10-16 2>&1 | grep -E "^\[bronze\]|Exception"
   Expect: chatlog 21224 rows, bot_predict 10729 rows
   |
Step A9: Validate against 06_validation.md (counts and labels only)
   $DC exec spark /opt/spark/bin/spark-sql --master local[2] -e "SELECT count(*), count(DISTINCT ID), … ;"
   Expect: 21224/21224/89/0 · 10729/10729/89/0 · client 10613 / bot 10611 · 116 unmatched
   |
Step A10: Idempotency, snapshots, time travel
   make bronze DATE=2026-10-16                    rerun the same date
   Q "SELECT committed_at, snapshot_id, operation, summary['added-records'], summary['deleted-records'], summary['total-records'] FROM lake.bronze.chatlog.snapshots ORDER BY committed_at;"
   Expect: append → overwrite (deleted = added = 21224)
   Q "SELECT 'v1', _batch_id, count(*) FROM lake.bronze.chatlog VERSION AS OF <first snapshot_id> GROUP BY 2;"
   Q "SELECT count(*) FROM lake.bronze.chatlog TIMESTAMP AS OF '<time between commits>';"
   Q "SELECT 'current', count(*) FROM lake.bronze.chatlog.files UNION ALL SELECT 'all', count(*) FROM lake.bronze.chatlog.all_data_files;"
   make bronze DATE=2026-10-17                    a new date = a new partition, old one untouched
   |
   ├── Error (no ID): Q prints nothing
   │   │   Root cause: placeholder <ID1> left in SQL; Q hides stderr
   │   └── Fix: put the real snapshot_id; when output is empty, rerun without 2>/dev/null
   │        → back to Step A10
   |
Step A11: Clean up test data and old images
   docker image rm vnpt-lake-hive-metastore:latest vnpt-lake-spark:latest    old custom images
   git rm -q docker/spark/Dockerfile infra/compose/hive/Dockerfile
   sed -i 's/up -d --build/up -d/' Makefile
   Q "DROP TABLE lake.tmp.t PURGE; DROP NAMESPACE lake.tmp; DELETE FROM lake.bronze.chatlog WHERE _ingest_date = DATE '2026-10-17'; …"
   $DC up -d                                      apply digest pins
   |
Step A12: Idempotent HMS schema init (new step, from E19)
   git apply <patch>                              adds hms-schema-init (schematool -info → -initSchema only if missing)
   $DC up -d; $DC logs hms-schema-init            Expect: "HMS schema exists, nothing to do"
   make down; make up; make bronze DATE=2026-10-16    restart test
   |
Step A13: From-zero test (⚠ deletes all volumes)
   make clean                                     down -v: containers + volumes
   rm -f infra/compose/jars/*.jar                 simulate a fresh clone
   time make up
   $DC logs hms-schema-init spark-init 2>&1 | grep -E "schema|pydeps"
   |
   ├── Error E20 (found before it hit): fresh clone → jars-init "Permission denied"
   │   │   Root cause: git-ignored jars/ missing → Docker creates it as root
   │   └── Fix: commit infra/compose/jars/.gitkeep (.gitignore: jars/* + !jars/.gitkeep)
   │        → back to Step A13
   │
   ├── Error E21: hms-schema-init exit 1 → "authentication type 10 is not supported"
   │   │   Diagnose: ls -la infra/compose/jars   → the "jars" are root-owned DIRECTORIES
   │   │   Root cause: compose creates all containers first; missing bind-mount sources became directories;
   │   │               jars-init skipped them; only the old 9.4 JDBC driver (no SCRAM) was left
   │   └── Fix: make down; rmdir infra/compose/jars/*.jar; git apply e21.patch
   │          (make up now runs "$DC run --rm jars-init" BEFORE "up -d"; fetch-jars.sh fails on directories)
   │        → back to Step A13
   |
Step A14: Failure drills
   A: $DC stop hive-metastore
      make bronze DATE=2026-10-16 > ~/drillA.log 2>&1; echo "exit: $?"        Expect: exit 2, no new snapshot
      $DC start hive-metastore
   B: $DC exec -T -e PYTHONUNBUFFERED=1 spark /opt/spark/bin/spark-submit … < /dev/null > ~/drillB.log 2>&1 & timeout 90 sh -c 'until grep -q "^\[bronze\] bronze.chatlog" ~/drillB.log; do sleep 0.2; done' && $DC kill spark; wait
      $DC start spark
      Expect: exit 137; chatlog new batch, bot_predict old batch → rerun → one batch
   |
   ├── Error E22: background job "Stopped"; kill %1 does nothing; exit code 141
   │   │   Root cause: exec reads the terminal (SIGTTIN); stopped jobs ignore SIGTERM; grep -m closes the pipe (SIGPIPE)
   │   └── Fix: exec -T … < /dev/null;  kill -9 %1;  write output to a file, then echo $?
   │        → back to Step A14
   |
Step A15: After a reboot / Docker Desktop restart
   (Windows) start Docker Desktop
   $DC ps -a --format "table {{.Service}}\t{{.Status}}"
   |
   ├── Error E23: hive-metastore "Exited (127)", spark stopped; minio/postgres Up
   │   │   Diagnose: docker inspect vnpt-lake-hive-metastore-1 --format '{{.State.Error}}'
   │   │   Root cause: Docker Desktop auto-restarted HMS before the WSL bind-mount sources were ready
   │   └── Fix: make up        (idempotent; run it once after every reboot)
   │        → back to Step A15
   |
Step A16: Demo checks
   http://localhost:9001 (Windows browser)        MinIO console: lakehouse/warehouse/bronze.db/…
   Q "SELECT file_path, record_count FROM lake.bronze.chatlog.files;"   1 current file (of 7 on disk)
```

---

## Part B — Standard build (tonight; full commands in `08_standard_build.md`)

```
Step B0: Recover after reboot, back up, branch, baseline          (08 §4.0)
   make up                                        E23 recovery: bring HMS and Spark back
   make smoke 2>/dev/null || Q "SHOW NAMESPACES IN lake;"          (make smoke exists only after B5)
   install -m 600 infra/compose/.env ~/vnpt-env-backup-lab.env      back up the git-ignored .env
   git switch -c standard-build                   work on a branch
   docker system df; docker image inspect … --format "{{index .RepoDigests 0}}"   record digests
   |
   ├── Error: ____
   │   │   Root cause: ____
   │   └── Fix: ____
   │        → back to Step B0
   |
Step B1: Write the two Dockerfiles, lint them                       (08 §4.1)
   cat > docker/spark/Dockerfile <<'EOF' … EOF; cat > docker/hive/Dockerfile <<'EOF' … EOF
   docker build --check docker/spark && docker build --check docker/hive    Expect: no warnings
   |
   ├── Error: ____
   │   └── Fix: ____  → back to Step B1
   |
Step B2: Update .env and .env.example (digest pins, runtime image tags)      (08 §4.2)
   PG_DIGEST=$(docker image inspect postgres:16 --format '{{index .RepoDigests 0}}')
   for f in …; do sed -i …; cat >> "$f" <<EOF … EOF; done
   |
   ├── Error: ____
   │   └── Fix: ____  → back to Step B2
   |
Step B3: Render secrets (hive-site.xml, pg_password; mode 600, git-ignored)  (08 §4.3)
   python3 -I infra/compose/render-secrets.py; ls -l infra/compose/secrets
   |
   ├── Error: ____
   │   └── Fix: ____  → back to Step B3
   |
Step B4: Replace the compose file, validate                         (08 §4.4)
   cat > infra/compose/docker-compose.yml <<'EOF' … EOF
   $DC config --quiet && echo compose-ok
   |
   ├── Error: ____
   │   └── Fix: ____  → back to Step B4
   |
Step B5: Spark config, Makefile, removed files, build, inspect     (08 §4.5)
   sed … spark-defaults.conf; sed/printf … Makefile; git rm … fetch-jars.sh jars/.gitkeep
   time make build                                build the 2 thin images
   docker run --rm vnpt-lake-spark:… sh -c 'python3 -c "import pandas…"'      deps inside the image
   docker run --rm --entrypoint sh vnpt-lake-hms:… -c 'ls /opt/hive/lib | grep postgres'   1 JDBC driver
   docker builder prune -f; git add -A && git commit -m "WIP standard build"
   |
   ├── Error: ____
   │   └── Fix: ____  → back to Step B5
   |
Step B6: From-zero test in a separate project (demo data untouched) (08 §4.6)
   make down
   DCS="docker compose -p vnpt-lake-std -f infra/compose/docker-compose.yml --env-file infra/compose/.env"
   time $DCS up -d; $DCS ps -a; $DCS logs hms-schema-init
   $DCS exec -T spark … spark-sql -e "SHOW NAMESPACES IN lake"              smoke
   $DCS exec -T spark … spark-submit … bronze_ingest.py --ingest-date 2026-10-16
   secret checks (inspect / ps / logs / postgres env)                       Expect: 0 0 0 0
   $DCS down -v
   |
   ├── Error: ____
   │   │   Root cause: ____
   │   └── Fix: ____  → back to Step B6   (or rollback: 08 §6)
   |
Step B7: Switch the demo stack, remove old volumes                 (08 §4.7)
   make up; $DC up -d --remove-orphans; make smoke
   docker volume rm vnpt-lake_pydeps vnpt-lake_ivy-cache; docker image rm curlimages/curl:8.22.0
   Q "SELECT count(*) FROM lake.bronze.chatlog.snapshots;"                 Expect: 7
   |
   ├── Error: ____
   │   └── Fix: ____  → back to Step B7
   |
Step B8: Merge and push                                            (08 §4.8)
   git commit --amend -m "…"; git switch main && git merge --ff-only standard-build && git push origin main
```

---

## Part C — Everyday commands (after the standard build)

| Goal | Command | What it does |
|---|---|---|
| Start everything | `make up` | Renders secrets, starts all services (idempotent; also the fix after a reboot) |
| Check the chain | `make smoke` | Spark → Iceberg → HMS → Postgres answers `SHOW NAMESPACES` |
| Load a date | `make bronze DATE=YYYY-MM-DD` | Replaces that `_ingest_date` partition |
| SQL shell | `make sql` | Interactive Spark SQL on catalog `lake` |
| Status / RAM | `make ps` · `make stats` | Containers and memory |
| Logs | `make logs SVC=hive-metastore` | Follow a service's log |
| Stop (keep data) | `make down` | Containers removed, volumes kept |
| Reset (delete data) | `make clean` | Containers **and** volumes removed |
| Rebuild images | `make build` | Only after a version change in `docker/*/Dockerfile` or `requirements.txt` |
