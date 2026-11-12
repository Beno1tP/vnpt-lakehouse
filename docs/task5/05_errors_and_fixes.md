# Task 5 — Errors & fixes

IDs continue after the last Task 4 error (`docs/task4/05_errors_and_fixes.md`). Expected errors per symptom: `03_plan.md` §E.

| ID | Symptom | Root cause | Fix | Theory | Status |
|---|---|---|---|---|---|
| E79 | PR #115 CI `superset-image` (2026-11-12 04:57 UTC = 11:57 ICT): the image built (dependency resolution with Superset's pins and `uv pip check` passed: D26 holds) and the dialect check passed, then `error: Failed to initialize cache at superset_home/.cache/uv … Permission denied`, followed by `FAIL: a Superset package was changed or removed` (*seen*) | Claude's Dockerfile ran `uv pip freeze` / `uv pip check` as root during the build: uv created its cache in `$HOME` = `/app/superset_home` owned by root, so as user `superset` uv cannot open it. The freeze in our image therefore failed, `ours.txt` was empty, and the diff reported every base package as removed: the check misreported its own failure | `UV_NO_CACHE=1` in the build and the CI freezes; CI first asserts both freezes returned > 50 packages and that nothing in `/app/superset_home` is owned by anyone but `superset` | Build steps running as root leave root-owned files in the runtime user's home; tools with caches need them disabled or redirected. A comparison check must first prove both inputs are valid, or an empty input looks like a large difference | fix in PR |
