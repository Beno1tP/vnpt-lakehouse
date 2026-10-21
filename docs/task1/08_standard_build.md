# Task 1 — Standard build (dependencies in images, config outside, secrets as files, data in volumes)

Goal: replace the lab shortcuts (pip packages and Iceberg jars in volumes, HMS jars bind-mounted from the host, the DB password in `SERVICE_OPTS`) with the standard layout, so Task 2 (Kubernetes) can pull images and mount secrets. Same versions, same job, same results.

Status: **plan revised 2026-10-21 after Bee's review; build and test by Bee (results in §8).**

## 0. Review points (Bee, 2026-10-21) → decisions

| # | Point | Decision | Checked how |
|---|---|---|---|
| 1 | Is `git switch main` + `make up` a real rollback? | **No.** `git switch` *carries* uncommitted edits and untracked files to `main`; the git-ignored `.env` and the deleted `jars/` are not restored by Git at all → main's compose would miss `CURL_IMAGE` etc. New rollback in §6: back up `.env` first, commit work on the branch, then switch | Git semantics |
| 2 | Schema init treats *every* `-info` failure as "no schema" | **Fixed.** Init only when the output says `relation "VERSION" does not exist`; schema 3.1.0 present → no-op; any other failure (connection, auth) or another schema version → print the error and **exit 1** | 4 branches tested with a fake `schematool` (exists / no schema / connection refused / wrong version) |
| 3 | Two Postgres JDBC drivers on the HMS classpath | **Fixed.** Dockerfile removes the image's `postgresql-9.4.1208.jre7.jar`; only `postgresql-42.7.4.jar` stays | Verify in 4.5 |
| 4 | Health checks | HMS: TCP check on 9083 (`bash </dev/tcp`), Spark starts only when HMS is **healthy**. Spark runner is `sleep infinity`, not a server, so a health check would prove nothing; instead `make smoke` checks the whole chain (Spark → Iceberg → HMS → Postgres) | Verify in 4.6 |
| 5 | `set -euo pipefail` (bash?) | `pipefail` is a bash feature (older `sh`/dash lack it) → used in `hms-schema-init`, which runs `bash -c`. `minio-init` stays `/bin/sh` with a retry loop | `bash -n` on the unescaped script |
| 6 | Pin by digest | **All images by digest** (`tag@sha256:…`, tag kept for readability). Spark, Hive, busybox digests = what is already on disk (no re-download). `postgres:16` moved upstream (now 16.15, `ca0bd484…`) → pin **your local** digest (saves 640 MB) | Registry API vs local image IDs |
| 7 | Log of schematool | Printed (password lines filtered): `-info` result, last lines of `-initSchema`, final version | In the script |
| 8 | `# syntax=docker/dockerfile:…` needed? | Not needed on this laptop (built-in frontend passed `docker build --check`), **but recommended**: it pins the Dockerfile parser, so CI (GitHub Actions, Task 2) understands `ADD --checksum/--chmod` identically. Pinned `docker/dockerfile:1.27.1@sha256:4edf…` (2026-09-30). Cost: one small frontend image pulled at build | Docker Hub tags |
| 9 | Password in `SERVICE_OPTS` (docker inspect, ps, logs) | **Fixed for HMS and Postgres.** JDBC settings move to a `hive-site.xml` mounted as a **Compose secret** at `/opt/hive/conf/hive-site.xml`; Postgres uses `POSTGRES_PASSWORD_FILE`. Both files are rendered from `.env` by `make secrets` into git-ignored `infra/compose/secrets/` (mode 600). MinIO root password / S3 keys in env = phase 2 (§7) | Renderer tested (XML escaping, mode 600) |

## 1. Principle

| What | Where it belongs | Lab version | Standard version |
|---|---|---|---|
| Programs + dependencies | **Image** (immutable, versioned) | `pydeps`, `ivy-cache` volumes; `jars/` bind mounts | 2 thin images |
| Configuration | **Outside** the image (mount; K8s ConfigMap) | mounted | mounted |
| Credentials | **Secret files** (Compose secrets; K8s Secrets) | env vars, JVM flags | files for HMS/Postgres; env for MinIO/S3 until phase 2 |
| Data and state | **Volumes** (K8s PVC) | volumes | volumes |

## 2. Options evaluated (Bee, 2026-10-20)

| Option | Verdict | Why |
|---|---|---|
| A. CI (GitHub Actions) → GHCR | **Task 2** | Standard flow; reuses these Dockerfiles unchanged |
| B. `FROM apache/spark:…` + pip (+ jars) | **Now** | ~0.35 GB new layers; keep 3.5.9. Images cost **disk**, not RAM (RAM = JVM heap + job) |
| C. `tabulario/spark-iceberg` | **No** | Newest tag `3.5.5_1.8.1` (2025-03-11, 2.28 GB, Jupyter): versions ≠ our pins; demo image, 19 months without a release |
| D. `--packages` | **Dev only** | Needs Maven at runtime; jars stored twice (213 MB) |
| E. Shared base image | **Later** | When several job images exist |

## 3. Target layout

| Item | After |
|---|---|
| `docker/spark/Dockerfile` | `FROM apache/spark:3.5.9…@sha256:f3d6…` + 2 Iceberg jars (SHA-256) + pinned pip deps |
| `docker/hive/Dockerfile` | `FROM apache/hive:3.1.3@sha256:d102…` + `hadoop-aws` 3.1.0 + SDK 1.11.271 + `postgresql-42.7.4` (SHA-256); old 9.4 driver removed |
| Image tags | `vnpt-lake-spark:3.5.9-iceberg1.11.0-r1`, `vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1` |
| Secrets | `infra/compose/render-secrets.py` → `infra/compose/secrets/{hive-site.xml,pg_password}` (git-ignored, 600) |
| Removed | `jars-init`, `spark-init`, volumes `pydeps`/`ivy-cache`, `fetch-jars.sh`, `jars/`, `spark.jars.*` lines, JDBC flags in `SERVICE_OPTS` |
| New make targets | `make build`, `make secrets` (run by `make up`), `make smoke` |
| Side effect | E20/E21 workarounds disappear (no bind-mounted jar files) |

## 4. Steps (tonight)

Start Docker Desktop first. If WSL says `docker could not be found`: `exit`, then `wsl` again. After a reboot, `hive-metastore` may show `Exited (127)` (E23: bind mounts not ready when Docker Desktop auto-restarted it) → run `make up` once.

Pre-checks done by Claude on 2026-10-21 (throwaway containers): HMS reads JDBC **only** from a mounted `hive-site.xml` with the 9.4 driver removed → `Metastore schema version: 3.1.0` ✅; Compose secret mounted at the absolute path `/opt/hive/conf/hive-site.xml`, `-rw------- hive` ✅; secret absent from `compose config` ✅; `bash </dev/tcp` check syntax ✅ (success path verified in 4.6).

### 4.0 Backups, branch, baseline
```bash
cd ~/vnpt-lakehouse
install -m 600 infra/compose/.env ~/vnpt-env-backup-lab.env      # .env is git-ignored: Git can't restore it
git switch -c standard-build
docker system df
for i in apache/spark:3.5.9-scala2.12-java17-python3-ubuntu apache/hive:3.1.3 busybox:1.36 postgres:16; do docker image inspect $i --format "$i {{index .RepoDigests 0}}"; done
```
Expect digests: spark `f3d6eaa8…`, hive `d102ba29…`, busybox `73aaf090…` (match the pins below). Note the **postgres** digest (expected `1a6ab3f5…`), used in 4.2.

### 4.1 Dockerfiles and requirements
```bash
mkdir -p docker/spark docker/hive
git mv infra/compose/spark/requirements.txt docker/spark/requirements.txt

cat > docker/spark/Dockerfile <<'EOF'
# syntax=docker/dockerfile:1.27.1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# Thin Spark runtime image: official Spark + job dependencies. No configuration, no credentials, no data.
ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu@sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5
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
# syntax=docker/dockerfile:1.27.1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# Thin Hive Metastore image: official Hive 3.1.3 + S3A and JDBC jars. No configuration, no credentials.
ARG HIVE_IMAGE=apache/hive:3.1.3@sha256:d102ba29ad07e93c303894896203a80b903c0001d80221f1cb9fea92dcac06e4
FROM ${HIVE_IMAGE}
USER root
# hadoop-aws MUST equal the image's hadoop-common 3.1.0 (E17); SDK = the version hadoop 3.1.0 was built with
ADD --chmod=644 --checksum=sha256:a18508b9348af095ea41301e439354dbd449e304ac44c6885b2b4fe78de88126 \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar /opt/hive/lib/
ADD --chmod=644 --checksum=sha256:faf78ac4880f56cf52791d84ec1068ce7c66acc4295d580a726104b734c01fcd \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/
# Exactly one JDBC driver: 42.7.4 (SCRAM-SHA-256 for Postgres 16); remove the image's 9.4 driver (E21)
ADD --chmod=644 --checksum=sha256:188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/
RUN rm -f /opt/hive/lib/postgresql-9.4.1208.jre7.jar
USER hive
EOF
docker build --check docker/spark && docker build --check docker/hive
```
Expect `Check complete, no warnings found.` twice (the first run pulls the small `docker/dockerfile` frontend).

### 4.2 `.env` and `.env.example`
```bash
PG_DIGEST=$(docker image inspect postgres:16 --format '{{index .RepoDigests 0}}'); echo "$PG_DIGEST"
for f in infra/compose/.env infra/compose/.env.example; do
  sed -i '/^\(CURL_IMAGE\|HADOOP_AWS_VERSION\|AWS_SDK_BUNDLE_VERSION\|PG_JDBC_VERSION\|ICEBERG_VERSION\|SPARK_IMAGE\|HIVE_IMAGE\)=/d' "$f"
  cat >> "$f" <<EOF

# Base images (pinned by digest; versions + jar checksums live in docker/*/Dockerfile)
SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu@sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5
HIVE_IMAGE=apache/hive:3.1.3@sha256:d102ba29ad07e93c303894896203a80b903c0001d80221f1cb9fea92dcac06e4
POSTGRES_IMAGE=postgres:16@${PG_DIGEST#*@}
BUSYBOX_IMAGE=busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662
# Thin runtime images built by `make build`
SPARK_RUNTIME_IMAGE=vnpt-lake-spark:3.5.9-iceberg1.11.0-r1
HMS_RUNTIME_IMAGE=vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1
EOF
done
grep -v -i -E "password|secret" infra/compose/.env
```
Note the heredoc here is **unquoted** (`<<EOF`) so `${PG_DIGEST#*@}` is filled in. Then open both files in VS Code and delete comments that mention the removed variables.

### 4.3 Secret renderer
```bash
cat > infra/compose/render-secrets.py <<'EOF'
"""Render credential files from infra/compose/.env (git-ignored) into infra/compose/secrets/ (git-ignored, mode 600).

Keeps the Postgres password out of HMS's SERVICE_OPTS, so it no longer appears in
docker inspect, ps (JVM arguments) or the container log. Run: make secrets
"""
import os
import xml.sax.saxutils as xs

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "secrets")


def read_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def write_private(name, text):
    path = os.path.join(OUT, name)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.chmod(path, 0o600)


def main():
    env = read_env(os.path.join(HERE, ".env"))
    os.makedirs(OUT, mode=0o700, exist_ok=True)
    props = {
        "javax.jdo.option.ConnectionDriverName": "org.postgresql.Driver",
        "javax.jdo.option.ConnectionURL": "jdbc:postgresql://postgres:5432/metastore",
        "javax.jdo.option.ConnectionUserName": env["PG_USER"],
        "javax.jdo.option.ConnectionPassword": env["PG_PASSWORD"],
    }
    body = "".join(
        f"  <property><name>{k}</name><value>{xs.escape(v)}</value></property>\n" for k, v in props.items()
    )
    write_private("hive-site.xml",
                  '<?xml version="1.0"?>\n<!-- generated by render-secrets.py from .env: do not commit -->\n'
                  f"<configuration>\n{body}</configuration>\n")
    write_private("pg_password", env["PG_PASSWORD"])
    print("secrets rendered (mode 600):", ", ".join(sorted(os.listdir(OUT))))


if __name__ == "__main__":
    main()
EOF
echo 'infra/compose/secrets/' >> .gitignore
python3 -I infra/compose/render-secrets.py && ls -l infra/compose/secrets && git check-ignore -v infra/compose/secrets/hive-site.xml
```
Expect two files `-rw-------`, and the check-ignore line. Don't `cat` them.

The files are owned by uid 1000. Inside the containers, `hive` is also uid 1000, and Postgres reads its file as root, so mode 600 still works.

### 4.4 Compose file (whole file via heredoc; YAML and the init script were checked on 2026-10-21)
```bash
cat > infra/compose/docker-compose.yml <<'EOF'
# Task 1 (standard build): MinIO + Postgres + Hive Metastore + Spark runner
# Dependencies: two thin images (docker/spark, docker/hive). Configuration: mounted. Credentials: Compose secrets
# rendered from .env by `make secrets` (never in env vars, JVM args or logs for HMS/Postgres). Volumes: data only.
# Usage (from repo root):  make build  (once per version change), then  make up,  make smoke,  make bronze
name: vnpt-lake

services:
  minio-perms:           # one-shot: give the MinIO data volume to uid 65532 (Chainguard non-root user)
    image: ${BUSYBOX_IMAGE}
    command: ["sh", "-c", "chown -R 65532:65532 /data && echo perms ok"]
    volumes: [minio-data:/data]

  minio:
    image: ${MINIO_IMAGE}
    command: ["server", "/data", "--console-address", ":9001"]
    depends_on:
      minio-perms: { condition: service_completed_successfully }
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}   # phase 2: move to a secret file (see 08_standard_build.md §7)
    ports: ["9000:9000", "9001:9001"]
    volumes: [minio-data:/data]
    mem_limit: 512m
    restart: unless-stopped
    # no healthcheck: the image has no shell/curl; minio-init waits for the API instead

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
    image: ${POSTGRES_IMAGE}
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD_FILE: /run/secrets/pg_password   # official image reads the password from the file
      POSTGRES_DB: metastore
    secrets: [pg_password]
    volumes: [pg-data:/var/lib/postgresql/data]
    mem_limit: 384m
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER} -d metastore"]
      interval: 5s
      retries: 20
    restart: unless-stopped

  hms-schema-init:       # one-shot: create the HMS schema ONLY when Postgres has none; any other error stops the stack
    image: ${HMS_RUNTIME_IMAGE}
    pull_policy: never   # built locally by `make build`; never look for it on Docker Hub
    depends_on:
      postgres: { condition: service_healthy }
    entrypoint: ["bash", "-c"]
    command:             # $$ = literal $ for bash (compose would interpolate a single $)
      - |
        set -euo pipefail
        ST=/opt/hive/bin/schematool
        if out=$$($$ST -dbType postgres -info 2>&1); then
          if grep -q "Metastore schema version:[[:space:]]*3.1.0" <<<"$$out"; then
            echo "HMS schema 3.1.0 exists, nothing to do"
          else
            echo "ERROR: unexpected schema version:" >&2; grep -i "version" <<<"$$out" >&2 || true; exit 1
          fi
        elif grep -q 'relation "VERSION" does not exist' <<<"$$out"; then
          echo "No HMS schema yet -> schematool -initSchema"
          $$ST -dbType postgres -initSchema 2>&1 | grep -v -i password | tail -n 5
          $$ST -dbType postgres -info 2>&1 | grep "Metastore schema version"
        else
          echo "ERROR: schematool -info failed for another reason (NOT 'no schema'); refusing to init:" >&2
          grep -v -i password <<<"$$out" | tail -n 15 >&2
          exit 1
        fi
    environment:
      HADOOP_CLIENT_OPTS: "-Xmx768m"   # JDBC settings come from the hive-site.xml secret, not from JVM flags
    secrets:
      - { source: hive_site, target: /opt/hive/conf/hive-site.xml }
    mem_limit: 1g         # -Xmx768m = 75% of 1g

  hive-metastore:        # thin image: apache/hive:3.1.3 + S3A/JDBC jars (docker/hive); core-site.xml mounted; JDBC creds = secret
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
      IS_RESUME: "true"    # entrypoint never inits; hms-schema-init does it (E07, E19)
      SERVICE_OPTS: "-Xmx768m"   # no credentials here any more (the entrypoint prints SERVICE_OPTS with set -x)
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}         # phase 2: credentials file instead of env (§7)
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
    secrets:
      - { source: hive_site, target: /opt/hive/conf/hive-site.xml }
    volumes:
      - ./hive/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro
    ports: ["9083:9083"]
    healthcheck:         # Thrift port accepting connections (bash /dev/tcp; no extra tools in the image)
      test: ["CMD", "bash", "-c", "</dev/tcp/127.0.0.1/9083"]
      interval: 5s
      timeout: 3s
      retries: 30
      start_period: 20s
    mem_limit: 1g
    restart: unless-stopped

  spark:                 # thin image: apache/spark:3.5.9 + Iceberg jars + pip deps (docker/spark); runner, idle until `make bronze`
    image: ${SPARK_RUNTIME_IMAGE}
    build:
      context: ../../docker/spark
      args: { SPARK_IMAGE: "${SPARK_IMAGE}" }
    depends_on:
      hive-metastore: { condition: service_healthy }   # start only when the catalog can accept connections
    command: ["sleep", "infinity"]    # not a server: readiness is proven end-to-end by `make smoke`
    environment:
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}         # phase 2: AWS_SHARED_CREDENTIALS_FILE secret (§7)
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
      AWS_REGION: us-east-1
    volumes:
      - ./spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
      - ../../jobs:/opt/jobs:ro
      - ${RAW_DATA_DIR}:/data/raw:ro          # Excel files, read-only
    ports: ["4040:4040"]                         # Spark UI while a job runs
    mem_limit: 3g

secrets:                 # files rendered by `make secrets` from .env into infra/compose/secrets/ (git-ignored, mode 600)
  hive_site:
    file: ./secrets/hive-site.xml
  pg_password:
    file: ./secrets/pg_password

volumes:                 # data only
  minio-data:
  pg-data:
EOF
DC="docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env"
$DC config --quiet && echo compose-ok
```

### 4.5 Spark config, Makefile, removed files, build
```bash
sed -i '/Iceberg jars resolved from Maven Central/d; /^spark.jars.packages/d; /^spark.jars.ivy/d' infra/compose/spark/spark-defaults.conf
tail -3 infra/compose/spark/spark-defaults.conf

sed -i '/run --rm jars-init/d; s/^up:  .*/up: secrets    ## render secrets, then start MinIO, Postgres, Hive Metastore, Spark runner/' Makefile
printf 'build:         ## build the 2 thin runtime images (once per version change)\n\t$(COMPOSE) build\nsecrets:       ## render credential files from .env into infra/compose/secrets/ (mode 600)\n\tpython3 -I infra/compose/render-secrets.py\nsmoke:         ## end-to-end check: Spark -> Iceberg -> HMS -> Postgres\n\t$(COMPOSE) exec -T spark /opt/spark/bin/spark-sql --master local[1] -e "SHOW NAMESPACES IN lake" < /dev/null 2>/dev/null | grep -v "^Time taken"\n' >> Makefile
grep -n -A1 -E "^(up|build|secrets|smoke):" Makefile

git rm -q infra/compose/fetch-jars.sh infra/compose/jars/.gitkeep
sed -i '/^infra\/compose\/jars\/\*$/d; /^!infra\/compose\/jars\/.gitkeep$/d' .gitignore
rm -rf infra/compose/jars

time make build
docker images | grep vnpt-lake
docker history vnpt-lake-spark:3.5.9-iceberg1.11.0-r1 --format "{{.Size}}\t{{.CreatedBy}}" | head -5
docker run --rm vnpt-lake-spark:3.5.9-iceberg1.11.0-r1 sh -c 'python3 -c "import pandas,openpyxl,pyarrow,numpy;print(pandas.__version__,openpyxl.__version__,pyarrow.__version__,numpy.__version__)"; ls /opt/spark/jars | grep iceberg'
docker run --rm --entrypoint sh vnpt-lake-hms:3.1.3-hadoopaws3.1.0-r1 -c 'ls /opt/hive/lib | grep -E "hadoop-aws|aws-java|postgres"'
docker builder prune -f
git add -A && git commit -q -m "WIP standard build" && git log --oneline -1   # committed on the branch, so a rollback is clean
```
Expect:
- `2.2.3 3.1.5 17.0.0 2.2.6` and both Iceberg jars
- HMS: `hadoop-aws-3.1.0.jar`, `aws-java-sdk-bundle-1.11.271.jar`, `postgresql-42.7.4.jar` and `postgresql-metadata-storage-0.12.0.jar` (an unrelated module), and **no** `postgresql-9.4…`
- `.env` and the secrets are **not** in the commit (both git-ignored)

### 4.6 From-zero test in a separate project (demo data untouched)
```bash
make down                                   # stop the demo stack, keep its volumes
DCS="docker compose -p vnpt-lake-std -f infra/compose/docker-compose.yml --env-file infra/compose/.env"
time $DCS up -d
$DCS ps -a --format "table {{.Service}}\t{{.Status}}"            # hive-metastore must show (healthy)
$DCS logs hms-schema-init                                         # expect: No HMS schema yet -> ... / Metastore schema version: 3.1.0
$DCS exec -T spark /opt/spark/bin/spark-sql --master local[1] -e "SHOW NAMESPACES IN lake" < /dev/null 2>/dev/null | grep -v "^Time taken"   # smoke
$DCS exec -T spark /opt/spark/bin/spark-submit --master local[2] /opt/jobs/bronze_ingest.py --ingest-date 2026-10-16 < /dev/null 2>&1 | grep -E "^\[bronze\]|Exception|resolving"
$DCS exec -T spark /opt/spark/bin/spark-sql --master local[1] -e "
SELECT count(*), count(DISTINCT ID), count(DISTINCT SENDER_ID) FROM lake.bronze.chatlog;
SELECT count(*) FROM lake.bronze.bot_predict;
SELECT operation, summary['total-records'] FROM lake.bronze.chatlog.snapshots;" < /dev/null 2>/dev/null | grep -v "^Time taken"

# Secret checks (counts only; every line must print 0)
C=vnpt-lake-std-hive-metastore-1
docker inspect $C --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -c ConnectionPassword
docker exec $C sh -c 'cat /proc/[0-9]*/cmdline 2>/dev/null | tr "\0" "\n" | grep -c ConnectionPassword'
docker logs $C 2>&1 | grep -c ConnectionPassword
docker inspect vnpt-lake-std-postgres-1 --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -c "^POSTGRES_PASSWORD="

$DCS down -v                                # remove the test stack and its volumes
```
Expect:
- `hive-metastore` shows **(healthy)**
- the schema-init log shows the "No HMS schema yet" branch, then `3.1.0`
- the smoke test prints `bronze`, or `default`
- **no** `resolving dependencies` line (no Ivy any more)
- `21224 21224 89`, `10729`, one `append 21224`
- four `0` lines from the secret checks

### 4.7 Switch the demo stack (keeps the 7 snapshots)
```bash
make up                                     # renders secrets, recreates postgres/hms/spark from the new images
$DC up -d --remove-orphans                  # removes the old jars-init/spark-init containers
$DC logs hms-schema-init                    # expect: HMS schema 3.1.0 exists, nothing to do
make smoke
docker volume rm vnpt-lake_pydeps vnpt-lake_ivy-cache
docker image rm curlimages/curl:8.22.0
$DC exec -T spark /opt/spark/bin/spark-sql --master local[1] -e "
SELECT count(*) FROM lake.bronze.chatlog;
SELECT count(*) FROM lake.bronze.chatlog.snapshots;
SELECT count(*) FROM lake.bronze.chatlog VERSION AS OF 7805204974073997753;" < /dev/null 2>/dev/null | grep -v "^Time taken"
docker system df
```
Expect: `21224`, `7` snapshots, and `21224` from time travel to the first snapshot. Postgres keeps its existing password: `POSTGRES_PASSWORD_FILE` is only read when the database is first created, and the file holds the same value as before.

### 4.8 Merge and push
```bash
git add -A && git status --short            # no .env, no secrets/, no jars
git commit --amend -m "Task 1: standard build — thin images, config mounted, credentials as secret files, data-only volumes"
git switch main && git merge --ff-only standard-build && git push origin main
```

## 5. Phase-2 secrets (not tonight)
The MinIO root password and the S3 keys are still environment variables, so `docker inspect` shows them on `minio`, `minio-init`, `hive-metastore` and `spark`. Candidate mechanisms, **to verify before use**:

| Who | Mechanism |
|---|---|
| MinIO | `MINIO_ROOT_USER_FILE` / `MINIO_ROOT_PASSWORD_FILE` pointing to secret files |
| Spark (Iceberg S3FileIO, AWS SDK v2) | `AWS_SHARED_CREDENTIALS_FILE=/run/secrets/aws_credentials` (default credentials chain) |
| HMS S3A (AWS SDK v1) | `fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider` + `AWS_CREDENTIAL_PROFILES_FILE` |
| Kubernetes (Task 2) | The same files from K8s Secrets (mounted), never env |

## 6. Rollback (any time before 4.8)
```bash
docker compose -p vnpt-lake-std -f infra/compose/docker-compose.yml --env-file infra/compose/.env down -v 2>/dev/null   # test stack, if it exists
make down
git add -A && git commit -q -m "WIP" || true           # keep the branch work; leaves a clean tree
git switch main                                        # main's compose, Makefile, fetch-jars.sh, .gitkeep come back
install -m 600 ~/vnpt-env-backup-lab.env infra/compose/.env   # restore the lab .env (Git can't)
make up                                                # main's make up re-downloads the jars (jars/ was deleted)
make bronze DATE=2026-10-16 2>&1 | grep "^\[bronze\]"  # optional check
```
Why it works: the branch holds every change. `main` gets back its tracked files, the `.env` backup restores the git-ignored config, and the old volumes (`pydeps`, `ivy-cache`) are only deleted in 4.7, after the new stack is proven.

## 7. Notes
- Tags vs digests: the tag stays for humans (`postgres:16`); the digest makes it immutable. After pushing the thin images to GHCR (Task 2), reference them by digest too.
- `docker builder prune -f` after builds keeps the build cache from growing (the 6 GB problem of 10-16).

## 8. Results (fill in after the build)

| Check | Before (lab) | After (standard) |
|---|---|---|
| `make build` time | — | ___ |
| Image sizes | spark 1.86 GB, hive 1.55 GB (official) | spark ___ , hms ___ |
| New layers (`docker history`) | — | spark ___ MB, hms ___ MB |
| HMS JDBC drivers on classpath | 2 (9.4 + 42.7.4) | ___ |
| Volumes | 4 (data 64 MB + deps 483 MB) | 2 (data ___ MB) |
| `docker system df` images / volumes / build cache | 4.98 GB / 0.54 GB / 0 | ___ / ___ / ___ |
| `make up` from zero (test project) | 49.5 s (incl. ~200 MB downloads) | ___ s |
| HMS health | — (no healthcheck) | ___ |
| Schema-init branch taken (test / demo) | — | ___ / ___ |
| Password in inspect / ps / logs (HMS), Postgres env | 1 / 3 / 4, 1 | ___ |
| First Spark run | Ivy resolves 2 artifacts (109 MB) | ___ |
| Bronze counts / snapshots (test) | 21,224 / 10,729 / 1 append | ___ |
| Demo stack after switch | — | ___ (21,224, 7 snapshots, time travel) |
| Errors met | — | ___ (log in `05_errors_and_fixes.md`) |
