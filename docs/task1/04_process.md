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

### 2026-10-15 — 1.0 Kick-off
Did: confirmed with the leader that tasks 1–5 are standalone mini-projects on the same data. Chose Docker Compose for Task 1. Prepared knowledge/config/plan docs and starter config.
Observed: data profile → chatlog 21,224 rows / predict 10,729 rows / 89 senders / 18 days (see `06_validation.md`).
Learned: why Task 1 needs a catalog (Hive Metastore) even without Trino yet: atomic pointer swap; Hadoop catalog unsafe on S3.
Next: 1.0 prerequisites, 1.1 data copy, 1.2 `make up`.
