# Task 1 — Standard build (dependencies in images, config outside, data in volumes)

Goal: replace the lab shortcuts (pip packages and Iceberg jars in volumes, HMS jars bind-mounted from the host) with the standard layout, so Task 2 (Kubernetes) can pull images instead of filling volumes. Same versions, same job, same results.

Status: **plan written 2026-10-20; build and test by Bee (results in §6, filled in later).**

## 1. Principle

| What | Where it belongs | Task 1 lab version | Standard version |
|---|---|---|---|
| Programs + dependencies (pip packages, jars) | **Image** (immutable, versioned, same everywhere) | `pydeps` + `ivy-cache` volumes, `jars/` bind mounts | 2 thin images |
| Configuration (`spark-defaults.conf`, `core-site.xml`) | **Outside** the image (mount; K8s ConfigMap) | mounted | mounted (unchanged) |
| Credentials | **Outside** (`.env`; K8s Secret) | `.env` | `.env` (unchanged) |
| Data and state (tables, HMS database) | **Volumes** (K8s PVC) | volumes | volumes (unchanged) |

## 2. Options evaluated (Bee's list, 2026-10-20)

All options aim at: few build steps, no runtime downloads, low disk, easy maintenance. Facts checked 2026-10-20.

| Option | What | Verdict | Why |
|---|---|---|---|
| A. CI build (GitHub Actions) → push to GHCR | Same Dockerfiles, built by CI on every version change | **Yes, in Task 2** | Standard production flow: no build cache on the laptop, images versioned in a registry, Minikube pulls them. Uses the Dockerfiles from option B unchanged, so nothing is thrown away |
| B. `FROM apache/spark:…` + `pip install` (+ Iceberg jars) | Thin local image on top of the official one | **Yes, now** | Smallest change from today; only ~0.35 GB of new layers (base layers already on disk). Keep **3.5.9** (pinned), not 3.5.1. Note: an image changes **disk**, not **RAM**; RAM depends on the JVM heap (`spark.driver.memory`) and the job |
| C. Community image `tabulario/spark-iceberg` | Ready-made Spark + Iceberg (+ Jupyter) | **No** | Newest tag `3.5.5_1.8.1` (2025-03-11, 2.28 GB): Spark 3.5.5 and Iceberg 1.8.1 ≠ our pinned 3.5.9 / 1.11.0; built as a quickstart/demo image (Jupyter, notebooks), not maintained as a runtime; no release in 19 months. Doesn't cover the rest of the pipeline either |
| D. `spark-submit --packages …` | Ivy downloads jars at startup (what we do today) | **Dev only** | Needs Maven Central at runtime; cache volume; jars stored twice (213 MB for 109 MB of jars). Example in the request used Iceberg 1.6.1 → keep 1.11.0 |
| E. Shared base image | One base with common deps, job images `FROM` it | **Later** | Worth it once several job images exist (bronze, silver, maintenance in Task 2+). Today there is one |

Chosen: **B now (local, thin, pinned, checksummed), A in Task 2 (same Dockerfiles via CI → GHCR), E when there are several job images.**

## 3. Target layout

| Item | Before (lab) | After (standard) |
|---|---|---|
| `docker/spark/Dockerfile` | — | `FROM apache/spark:3.5.9…` + 2 Iceberg jars (SHA-256 checked) + `pip install -r requirements.txt` |
| `docker/hive/Dockerfile` | — | `FROM apache/hive:3.1.3` + `hadoop-aws` 3.1.0, `aws-java-sdk-bundle` 1.11.271, `postgresql` 42.7.4 as `postgres.jar` (SHA-256 checked) |
| Image tags | — | `vnpt-lake-spark:3.5.9-iceberg1.11.0-r1`, `vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1` (`r1` = our revision) |
| Services removed | `jars-init`, `spark-init` | — |
| Volumes removed | `pydeps`, `ivy-cache` | only `minio-data`, `pg-data` |
| Files removed | `fetch-jars.sh`, `jars/`, `spark.jars.packages` lines, `make up`'s jars-init step | — |
| Side effect | E20/E21 workarounds needed | **E20/E21 disappear** (no bind-mounted jar files) |

SHA-256 values: Iceberg jars = values published on Maven Central (`.sha256` files, verified equal). `hadoop-aws`, SDK and JDBC jars: Maven publishes only `.sha1`, so the SHA-256 was computed from our SHA-1-verified downloads.

## 4. Steps (tonight)

Work on a **branch**, and test with a **separate Compose project name** so the demo stack and its 7 snapshots stay intact for Thursday.

### 4.0 Branch and baseline
```bash
cd ~/vnpt-lakehouse
git switch -c standard-build
docker system df
```
Record the `docker system df` numbers in §6.

### 4.1 Dockerfiles and requirements
```bash
mkdir -p docker/spark docker/hive
git mv infra/compose/spark/requirements.txt docker/spark/requirements.txt

cat > docker/spark/Dockerfile <<'EOF'
# Thin Spark runtime image: official Spark + job dependencies. No configuration, no credentials, no data.
ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}
USER root
# Iceberg 1.11.0 jars (SHA-256 = values published on Maven Central)
ADD --chmod=644 --checksum=sha256:94b8e36fc329f0293d44ba9e01b784a56e9501affec1842d898144c51f6e486a \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/spark/jars/
# Python dependencies for the image's own Python 3.10 (pinned)
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --disable-pip-version-check -r /tmp/requirements.txt && rm /tmp/requirements.txt
USER spark
EOF

cat > docker/hive/Dockerfile <<'EOF'
# Thin Hive Metastore image: official Hive 3.1.3 + S3A and JDBC jars. No configuration, no credentials.
ARG HIVE_IMAGE=apache/hive:3.1.3
FROM ${HIVE_IMAGE}
USER root
# hadoop-aws MUST equal the image's hadoop-common 3.1.0 (E17); SDK = version hadoop 3.1.0 was built with
ADD --chmod=644 --checksum=sha256:a18508b9348af095ea41301e439354dbd449e304ac44c6885b2b4fe78de88126 \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar /opt/hive/lib/
ADD --chmod=644 --checksum=sha256:faf78ac4880f56cf52791d84ec1068ce7c66acc4295d580a726104b734c01fcd \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/
# JDBC driver with SCRAM support; the name postgres.jar sorts before the image's old 9.4 driver (E21)
ADD --chmod=644 --checksum=sha256:188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/postgres.jar
USER hive
EOF
docker build --check docker/spark && docker build --check docker/hive
```
Expect: `Check complete, no warnings found.` twice.

### 4.2 `.env` and `.env.example`
```bash
for f in infra/compose/.env infra/compose/.env.example; do
  sed -i '/^\(CURL_IMAGE\|HADOOP_AWS_VERSION\|AWS_SDK_BUNDLE_VERSION\|PG_JDBC_VERSION\|ICEBERG_VERSION\)=/d' "$f"
  printf '\n# Thin runtime images built from docker/ (versions + checksums live in the Dockerfiles)\nSPARK_RUNTIME_IMAGE=vnpt-lake-spark:3.5.9-iceberg1.11.0-r1\nHMS_RUNTIME_IMAGE=vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1\n' >> "$f"
done
grep -v -i -E "password|secret" infra/compose/.env
```
Then open both files in VS Code and delete comments that mention the removed variables (hadoop-aws / SDK / Iceberg version comments).

### 4.3 Compose file (whole file, via heredoc; validated 2026-10-20)
```bash
cat > infra/compose/docker-compose.yml <<'EOF'
# Task 1 (standard build): MinIO + Postgres + Hive Metastore + Spark runner
# Dependencies live in two thin images (docker/spark, docker/hive); configuration is mounted; volumes hold data only.
# Usage (from repo root):  make build  (once per version change), then  make up  and  make bronze
name: vnpt-lake

services:
  minio-perms:           # one-shot: give the MinIO data volume to uid 65532 (Chainguard non-root user)
    image: busybox:1.36
    command: ["sh", "-c", "chown -R 65532:65532 /data && echo perms ok"]
    volumes: [minio-data:/data]

  minio:
    image: ${MINIO_IMAGE}
    command: ["server", "/data", "--console-address", ":9001"]
    depends_on:
      minio-perms: { condition: service_completed_successfully }
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    ports: ["9000:9000", "9001:9001"]
    volumes: [minio-data:/data]
    mem_limit: 512m
    restart: unless-stopped

  minio-init:            # one-shot: wait for MinIO, create bucket, then exit
    image: ${AWSCLI_IMAGE}
    depends_on: [minio]
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        until aws --endpoint-url http://minio:9000 s3 ls >/dev/null 2>&1; do echo waiting for minio; sleep 2; done
        aws --endpoint-url http://minio:9000 s3 mb s3://lakehouse || echo "bucket exists"
        aws --endpoint-url http://minio:9000 s3 ls
    environment:
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
      AWS_DEFAULT_REGION: us-east-1

  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASSWORD}
      POSTGRES_DB: metastore
    volumes: [pg-data:/var/lib/postgresql/data]
    mem_limit: 384m
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER} -d metastore"]
      interval: 5s
      retries: 20
    restart: unless-stopped

  hive-metastore:        # thin image: apache/hive:3.1.3 + S3A/JDBC jars (docker/hive); only core-site.xml is mounted
    image: ${HMS_RUNTIME_IMAGE}
    build:
      context: ../../docker/hive
      args: { HIVE_IMAGE: "${HIVE_IMAGE}" }
    depends_on:
      postgres: { condition: service_healthy }
      minio-init: { condition: service_completed_successfully }
      hms-schema-init: { condition: service_completed_successfully }
    environment:
      SERVICE_NAME: metastore
      DB_DRIVER: postgres
      IS_RESUME: "true"    # entrypoint never inits; hms-schema-init creates the schema only if missing (E07, E19)
      SERVICE_OPTS: &hms_opts >-
        -Xmx768m
        -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
        -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore
        -Djavax.jdo.option.ConnectionUserName=${PG_USER}
        -Djavax.jdo.option.ConnectionPassword=${PG_PASSWORD}
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
    volumes:
      - ./hive/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro
    ports: ["9083:9083"]
    mem_limit: 1g
    restart: unless-stopped

  hms-schema-init:       # one-shot: create the HMS schema only if Postgres has none (idempotent, safe on every `up`)
    image: ${HMS_RUNTIME_IMAGE}
    pull_policy: never   # built locally by `make build`; never look for it on Docker Hub
    depends_on:
      postgres: { condition: service_healthy }
    entrypoint: ["bash", "-c"]
    command:             # $$ = literal $ for the shell (compose would interpolate a single $)
      - |
        export HADOOP_CLIENT_OPTS="$$SERVICE_OPTS"
        if /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
          echo "HMS schema exists, nothing to do"
        else
          /opt/hive/bin/schematool -dbType postgres -initSchema >/dev/null 2>&1 && echo "HMS schema created"
        fi
    environment:
      SERVICE_OPTS: *hms_opts
    mem_limit: 1g         # SERVICE_OPTS carries -Xmx768m (~75% of 1g)

  spark:                 # thin image: apache/spark:3.5.9 + Iceberg jars + pip deps (docker/spark); runner, idle until `make bronze`
    image: ${SPARK_RUNTIME_IMAGE}
    build:
      context: ../../docker/spark
      args: { SPARK_IMAGE: "${SPARK_IMAGE}" }
    depends_on:
      hive-metastore: { condition: service_started }
    command: ["sleep", "infinity"]
    environment:
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
      AWS_REGION: us-east-1
    volumes:
      - ./spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
      - ../../jobs:/opt/jobs:ro
      - ${RAW_DATA_DIR}:/data/raw:ro          # Excel files, read-only
    ports: ["4040:4040"]                         # Spark UI while a job runs
    mem_limit: 3g

volumes:                 # data only
  minio-data:
  pg-data:
EOF
DC="docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env"
$DC config --quiet && echo compose-ok
```

### 4.4 Spark config, Makefile, removed files
```bash
sed -i '/Iceberg jars resolved from Maven Central/d; /^spark.jars.packages/d; /^spark.jars.ivy/d' infra/compose/spark/spark-defaults.conf
tail -3 infra/compose/spark/spark-defaults.conf

sed -i '/run --rm jars-init/d; s/## fetch jars, then start/## start/' Makefile
printf 'build:         ## build the 2 thin runtime images (once per version change)\n\t$(COMPOSE) build\n' >> Makefile
grep -n -A2 -E "^(up|build):" Makefile

git rm -q infra/compose/fetch-jars.sh infra/compose/jars/.gitkeep
sed -i '/^infra\/compose\/jars\/\*$/d; /^!infra\/compose\/jars\/.gitkeep$/d' .gitignore
rm -rf infra/compose/jars
git status --short
```
Expect: no `spark.jars.*` lines; `build:` target with a tab; `.gitignore` without the jars lines.

### 4.5 Build and inspect
```bash
time make build
docker images | grep vnpt-lake
docker history vnpt-lake-spark:3.5.9-iceberg1.11.0-r1 --format "{{.Size}}\t{{.CreatedBy}}" | head -6
docker run --rm vnpt-lake-spark:3.5.9-iceberg1.11.0-r1 sh -c 'python3 -c "import pandas,openpyxl,pyarrow,numpy;print(pandas.__version__,openpyxl.__version__,pyarrow.__version__,numpy.__version__)"; ls /opt/spark/jars | grep iceberg'
docker run --rm --entrypoint sh vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1 -c 'ls -l /opt/hive/lib | grep -E "hadoop-aws|aws-java|postgres"'
docker builder prune -f
```
Expect: `2.2.3 3.1.5 17.0.0 2.2.6`; both Iceberg jars; the 3 HMS jars + the image's own `postgresql-9.4…`.

### 4.6 From-zero test in a separate project (demo data untouched)
```bash
make down                                   # stop the demo stack, keep its volumes
DCS="docker compose -p vnpt-lake-std -f infra/compose/docker-compose.yml --env-file infra/compose/.env"
time $DCS up -d
$DCS ps -a --format "table {{.Service}}\t{{.Image}}\t{{.Status}}"
$DCS logs hms-schema-init | grep -i schema
$DCS exec -T spark /opt/spark/bin/spark-submit --master local[2] /opt/jobs/bronze_ingest.py --ingest-date 2026-10-16 < /dev/null 2>&1 | grep -E "^\[bronze\]|Exception"
$DCS exec -T spark /opt/spark/bin/spark-sql --master local[1] -e "
SELECT count(*), count(DISTINCT ID), count(DISTINCT SENDER_ID) FROM lake.bronze.chatlog;
SELECT count(*) FROM lake.bronze.bot_predict;
SELECT operation, summary['total-records'] FROM lake.bronze.chatlog.snapshots;" < /dev/null 2>/dev/null | grep -v -E "^Time taken"
$DCS down -v                                # remove the test stack and its volumes
```
Expect: `HMS schema created`; no `:: resolving dependencies` (no Ivy any more); `21224 21224 89`, `10729`, one `append 21224`.

### 4.7 Switch the demo stack to the new images (keeps the 7 snapshots)
```bash
$DC up -d --remove-orphans                  # recreates hms + spark from the new images; removes jars-init/spark-init containers
docker volume rm vnpt-lake_pydeps vnpt-lake_ivy-cache
docker image rm curlimages/curl:8.22.0
$DC exec -T spark /opt/spark/bin/spark-sql --master local[1] -e "
SELECT count(*) FROM lake.bronze.chatlog;
SELECT count(*) FROM lake.bronze.chatlog.snapshots;
SELECT count(*) FROM lake.bronze.chatlog VERSION AS OF 7805204974073997753;" < /dev/null 2>/dev/null | grep -v "^Time taken"
docker system df
```
Expect: `21224`, `7` snapshots, time travel to the first snapshot still returns `21224`.

### 4.8 Commit, merge, push
```bash
git add -A docker infra/compose Makefile .gitignore docs
git status --short                          # check: no .env, no jars
git commit -m "Task 1: standard build — dependencies in thin images, config mounted, data-only volumes"
git switch main && git merge --ff-only standard-build && git push origin main
```

## 5. Rollback
On any failure before 4.8: `git switch main` (the old compose/files come back), `make up`. The old volumes are only removed in 4.7, after the new stack works.

## 6. Results (fill in after the build)

| Check | Before (lab) | After (standard) |
|---|---|---|
| `make build` time | — | ___ |
| Image sizes (`docker images`) | spark 1.86 GB, hive 1.55 GB (official) | spark ___ , hms ___ |
| New layers (`docker history`) | — | spark ___ MB, hms ___ MB |
| Volumes | 4 (data 64 MB + deps 483 MB) | 2 (data ___ MB) |
| `docker system df` images / volumes / build cache | 4.98 GB / 0.54 GB / 0 | ___ / ___ / ___ |
| `make up` from zero (test project) | 49.5 s (incl. ~200 MB downloads) | ___ s (no downloads) |
| First Spark run | Ivy resolves 2 artifacts (109 MB) | no Ivy step: ___ |
| Bronze counts / snapshots | 21,224 / 10,729 / 1 append | ___ |
| Demo stack after switch | — | 21,224, 7 snapshots, time travel ___ |
| Errors met | — | ___ (log in `05_errors_and_fixes.md`) |
