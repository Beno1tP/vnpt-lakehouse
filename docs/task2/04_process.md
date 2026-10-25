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
