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
