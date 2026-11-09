# Task 3 — Errors & fixes

IDs continue from Task 2 (E25–E62 in `docs/task2/05_errors_and_fixes.md`, including the Task 2 freeze), so Task 3 starts at **E63**. Expected errors per symptom: `03_plan.md` §E.

| ID | Symptom | Root cause | Fix | Theory | Status |
|---|---|---|---|---|---|
| E63 | Overnight `tools/backfill_twice.sh` (2026-11-09 16:13–17:15 UTC = 23:13–00:15 ICT): `[backfill] DIFFERENT per-day lines between the passes` although both passes had 0 failed runs (*seen*) | Claude's script compared the whole `make dag-report` output; it also contains Airflow log lines with timestamps (`AWS Connection … retrieved`, `ProvidersManager.hooks is deprecated`) and `[report] N log files read` (283 → 397 grows every pass). The diff had only those 3 lines; no data line differed | Compare only lines starting with `[landing]`/`[bronze]`/`[validate]`/`[silver]`/`[validate_silver]`: 155 vs 155, identical (Bee, from the saved evidence). Script fixed to do that | An equality check is only as good as what it compares: strip volatile fields (timestamps, counters, log noise) before diffing, and print what was compared. Read a diff before believing its verdict | fixed |
