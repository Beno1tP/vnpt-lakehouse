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
