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

### 2026-10-25 — 2.0a Plan reviewed
Did: plan from Claude desktop reviewed in Claude Code; versions checked upstream; docs `01`–`07` created.
Observed: Spark Operator v2.2.1 = Spark 3.5.5 base, v2.3.0+ = Spark 4.0; operator webhook applies volumes/env/ConfigMaps (must stay on); Airflow chart 1.22.0 ships 3.2.2 (no 3.3.x chart yet); minikube v1.39.0 defaults to K8s v1.37.0; data has 18 days in a 24-day window.
Learned: see `01_knowledge.md` [review] notes.
Next: decisions D1–D3 (`02_config.md`), then 2.0 Prep.
