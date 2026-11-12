# Task 5 — Process log

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

### 2026-11-12 — 5.4 done, E82 (MySQLdb) prepared
5.4 (Bee): PR #121 merged; `superset-up` → `connection ok: StarRocks Gold -> starrocks://bi_ro:XXXXXXXXXX@…`, `query ok as 'bi_ro'@'%' | fct_user_message = 10613`, `read-only ok: CREATE TABLE -> Access denied`. SQL Lab: a query on a wrong column (`message_cnt`: Claude's guess, the real columns are `understood_cnt` / `not_understood_cnt`) returned StarRocks' analysis error; the corrected query → `No module named 'MySQLdb'` (E82). Lesson for 5.5: dataset metrics generated from / tested against the dbt model columns.
Prepared (Claude, branch `task5/e82-mysqldb-shim`): MySQLdb shim in `superset_config.py`, unit test, CI check of `get_datatype` inside the image.

### 2026-11-12 — 5.3b done (E81), 5.4 prepared: StarRocks connection
5.3b (Bee): PR #120 merged; `make superset-up` → rolled out, `admin ok: role Admin, password = Secret`. Measured: web 192 Mi / 229m right after start, 0 restarts; node requests **6,116 Mi (84 %)** = D22 budget, limits 9,948 Mi (136 %); only warning `flask_limiter … in-memory storage` (expected without Redis, D24; correct with 1 replica).
Prepared (Claude, branch `task5/5.4-connect`): Job superset-init registers "StarRocks Gold" (`starrocks://bi_ro:…@starrocks-fe-service…:9030/default_catalog.gold`, `get_or_create_db`, SQL Lab on, DML/CTAS/CVAS/upload/async off) and checks through the stored connection: `count(*) fct_user_message` = 10,613, `current_user()`, `CREATE TABLE` → Access denied (E71 rule). Predicted (my local run): ruff, pytest, `bash -n` of the Job script.

### 2026-11-12 — 5.3a merged (PR #118), 5.3b prepared: superset-up
CI evidence (Bee): `uid=1000(superset)`, `config loaded, db h:5432/d` (our file applied inside the image), Superset kustomization = 1 ConfigMap + 1 Deployment + 1 Service, image `@sha256:a0307416f264…`.
Prepared (Claude, branch `task5/5.3b-up`): web init container `migrate` (`superset db upgrade; superset init`; 1 replica + Recreate = no concurrent migrations); Job `superset-init` in the Superset kustomization (waits for web `/health`, `fab create-admin` + `fab reset-password` from Secret superset-init-credentials, then verifies role Admin + `check_password_hash`; output filtered for "password"); Make `superset-up` (refuses unless the scheduler is at 0, D22; delete Job → `apply -k` → rollout → Job result), `superset-down`, `superset-ui`, `demo-task5`; `mode-gold`/`mode-query`/`mode-pipeline` scale Superset to 0. Predicted (my local run): ruff, pytest (42), `bash -n` of the Job script, fake-SHELL expansion of the 6 targets + `bash -n` pass.

### 2026-11-12 — 5.3a rework: own manifests (D27)
CI on PR #118 (Bee): chart deprecated + worker always rendered + init script with admin defaults (E80). Read: Superset docs (Helm chart deprecated, use the operator), operator releases v0.1.0 (06-13) … v0.3.0 (09-30). Decided (Bee): **own manifests** (D27).
Prepared (Claude, same branch): `infra/k8s/superset/{superset.yaml, superset_config.py, kustomization.yaml}` (Deployment Recreate, Service :8088, init container wait-for-postgres with our image, non-root 1000, no SA token, probes `/health`, 1 Gi / 1.5 Gi; config: SECRET_KEY + DB URI from env with URL-quoting, small pool, SQL Lab limits, no Redis/Celery); chart values, Make targets and the render checker removed; `tests/unit/test_superset_manifests.py` (5 tests); CI: kustomize render of `infra/k8s/superset` with digest check, config loaded inside the image (`from superset import config`). Predicted (my local run): ruff, pytest, YAML pass; kustomize render and config load run first in CI.

### 2026-11-12 — 5.2 done, 5.3a prepared: chart render check
5.2 (Bee, PR merged + `make k8s-up`): secrets generated once, Job `superset-db-init` → `login ok: superset on superset, owner superset`; `\l superset` owner superset, UTF8; Secrets in `superset`: ghcr-pull, superset-env (6 keys), superset-init-credentials (2).
Read (chart superset 0.22.8, `values.yaml` + `_helpers.tpl`): `database.*` / `cache.*` replace `supersetNode.connections`; `cache.enabled: false` drops Redis, cache and Celery config; the generated `superset_config.py` builds the DB URI from env `DB_USER/DB_PASS/DB_HOST/DB_PORT/DB_NAME` (= our Secret superset-env); `secretEnv.create: false` + `envFromSecret`; defaults to avoid: `init.adminUser` password `admin`, `runAsUser: 0`, init container `apache/superset:dockerize` (moving tag, waits for Redis too). Release asset digest sha256:58075998… (GitHub API, published 2026-09-09).
Prepared (Claude, branch `task5/5.3a-render`): `infra/helm/superset/values-local.yaml`, `make superset-chart` / `superset-render`, `tools/superset_render_check.py` + 4 unit tests (images = images.env digest, 1 Deployment, no StatefulSet/Job/worker/beat/websocket, no chart env Secret, no DB_PASS/SECRET_KEY/admin defaults in Secrets, no root, web reads superset-env, config without Redis/Celery and with env DB_PASS), CI job `superset-render` (download + checksum + render + check) and `id` in `superset-image`. Predicted (my local run): ruff, pytest, YAML, fake-SHELL expansion + `bash -n` (5 lines) pass; the render itself runs first in CI.

### 2026-11-12 — 5.1 done, 5.2 prepared: metadata DB + Secrets
5.1 (Bee): PR #115 merged after E79 (3 commits), images run 43170076273 → `vnpt-lake-superset@sha256:a0307416…` (SQLAlchemy 1.4.54 kept, 157 → 162 packages), bump PR #116.
Prepared (Claude, branch `task5/5.2-metadata-db`): render-secrets generates `SUPERSET_SECRET_KEY` (base64 of 42 random bytes; also encrypts stored connection passwords, never rotate without `superset re-encrypt-secrets`), `SUPERSET_ADMIN_PASSWORD`, `SUPERSET_DB_PASSWORD`; Secrets `superset-db-credentials` (lake), `superset-env` (SECRET_KEY + DB_* for every Superset pod), `superset-init-credentials` (admin + bi_ro, init Job only), `ghcr-pull` in the new namespace `superset`; Job `superset-db-init` in lake (postgres:16 same digest, `\gexec` create-if-missing + `ALTER ROLE … PASSWORD`, login check as `superset` over TCP), waited for and logged by `k8s-up`; pg-backup also dumps `superset`. Predicted (my local run): ruff, pytest (render test extended), YAML parse, `bash -n` of the Job script pass.

### 2026-11-12 — 5.1 prepared: Superset image
Read (Superset 6.1.0 `Dockerfile`): venv `/app/.venv` on PATH, packages via `uv`, final user `superset`, WORKDIR `/app`, the lean stage installs `requirements/base.txt` and no `postgres` extra.
Prepared (Claude, branch `task5/5.1-image`): `docker/superset/` (FROM the 6.1.0 digest; `uv pip freeze --exclude-editable` of the base → constraints; `uv pip install -r requirements.txt -c constraints`; `uv pip check`; starrocks 1.3.4 + psycopg2-binary 2.9.13); CI job `superset-image` (build, `create_engine('starrocks://…')` resolves the dialect, imports, a diff of base vs our `uv pip freeze` that FAILS if any Superset package changed or was removed, `superset version`); Dockerfile check; images.yml job `superset`; ghcr-prune knows the package. Predicted: lint + tests pass, YAML parses; the build itself runs first in CI.

### 2026-11-12 — 5.0 plan v2 + disk clean-up
Did (Bee): version checks (Superset 6.1.0 digest, chart 0.22.8, starrocks 1.3.4, psycopg2-binary 2.9.13); C: 42 GB free → node images not in images.env removed (15, `crictl rmi`, after checking no pod used them) → `minikube stop` → diskpart compaction (first attempt: `detach vidsk` typo left docker_data.vhdx attached read-only to Windows → Docker Desktop "disk not found"; detached, compacted) → C: **52 GB free** (`dir` still shows the logical size of the sparse vhdx) → `minikube start && make k8s-up` (Airflow rev 27; the demo profile was reset: scheduler running again, asset DAGs still paused).
Decided (Bee): keep GHCR intact until the final presentation (ghcr-prune not run: no laptop disk gain).
Prepared (Claude, branch `task5/5.0-plan-v2`): `03_plan.md` v2 (R2 review: bi_ro, measured RAM, serving = demo-task4 + Superset, decisions D21–D26, steps 5.0–5.8 with an automated on-screen reconcile), `02_config.md` §1–2 with the checked pins and measured budget, `01_knowledge.md` references.

### 2026-11-06 — plan written
Did (Claude): input plan reviewed (`03_plan.md` §R, 9 points); `docs/task5/01–07` created; proposed decisions D19–D22.
Next: Task 4 `gold_uc5` green first; version checks in `02_config.md` §1.
