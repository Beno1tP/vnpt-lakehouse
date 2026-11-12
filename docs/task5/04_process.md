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

### 2026-11-12 — 5.0 plan v2 + disk clean-up
Did (Bee): version checks (Superset 6.1.0 digest, chart 0.22.8, starrocks 1.3.4, psycopg2-binary 2.9.13); C: 42 GB free → node images not in images.env removed (15, `crictl rmi`, after checking no pod used them) → `minikube stop` → diskpart compaction (first attempt: `detach vidsk` typo left docker_data.vhdx attached read-only to Windows → Docker Desktop "disk not found"; detached, compacted) → C: **52 GB free** (`dir` still shows the logical size of the sparse vhdx) → `minikube start && make k8s-up` (Airflow rev 27; the demo profile was reset: scheduler running again, asset DAGs still paused).
Decided (Bee): keep GHCR intact until the final presentation (ghcr-prune not run: no laptop disk gain).
Prepared (Claude, branch `task5/5.0-plan-v2`): `03_plan.md` v2 (R2 review: bi_ro, measured RAM, serving = demo-task4 + Superset, decisions D21–D26, steps 5.0–5.8 with an automated on-screen reconcile), `02_config.md` §1–2 with the checked pins and measured budget, `01_knowledge.md` references.

### 2026-11-06 — plan written
Did (Claude): input plan reviewed (`03_plan.md` §R, 9 points); `docs/task5/01–07` created; proposed decisions D19–D22.
Next: Task 4 `gold_uc5` green first; version checks in `02_config.md` §1.
