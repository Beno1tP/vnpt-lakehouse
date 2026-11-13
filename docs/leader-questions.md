# Questions for the leader (collected 2026-11-13, all tasks)

Each question says what we did meanwhile, so nothing is blocked; an answer may change a definition, a default or a design.

| # | Task | Question | What we did meanwhile | Where it is documented |
|---|---|---|---|---|
| 1 | 4 | **Where should Gold live?** The lab doc's architecture diagram puts "dbt Gold" inside "ICEBERG (MinIO S3)" and says Gold serves StarRocks, Trino and Superset; Bước 4 and the tech-stack row say Gold tables **on StarRocks** ("Gold layer, bitmap aggregation") | Gold = StarRocks internal tables (Bước 4); the BITMAP exact-users column only exists there (Iceberg has no BITMAP type). Alternative costed: dbt writes Gold into Iceberg + a StarRocks MV for the bitmap | `docs/task4/slides.md` 15b–15c |
| 2 | 3 | Silver placement: a Spark job in the daily DAG, before Trino (D6)? | Spark job `silver_transform.py` in `chatbot_bronze` | `docs/task3/03_plan.md` D6 |
| 3 | 3 | Source time zone of `CREATED_TIME` (assumed Asia/Ho_Chi_Minh) | stored as the wall clock without a zone (`TIMESTAMP_NTZ`, D8) + `event_date` | `docs/task3/01_knowledge.md` A1 |
| 4 | 3 | `NLU_THRESHOLD` semantics: per row (0.8 everywhere in this data) or a fixed business rule? | the row's value (D9) | `docs/task3/03_plan.md` D9 |
| 5 | 3 | Trino's role in production: ad hoc exploration only, or shared with users? | read-only, single node, scaled 0 ↔ 1 for checks | `docs/task3/README.md` |
| 6 | 4/5 | Names of the scenarios behind STEP 406665 / 406666 | inferred labels "start (/home)" / "free text (NLU)" (seed `scenario_labels`) | `docs/metrics.md` |
| 7 | 4/5 | UC7 "vs 30 days earlier" with only 24 days of data: absolute change, % shown as "No data" (M2)? | change = users(D) − users(D−30); % NULL on a zero base, shown as "No data" with a description | `docs/metrics.md` M2 |
| 8 | 4/5 | UC8 averages over the 24 calendar days, empty days = 0 (M3)? | yes (3.71 new / 4.67 active per day) | `docs/metrics.md` M3 |
| 9 | 4 | The 116 predictions without a chat row: a 4th traffic class `unmatched` (M1)? | yes; the filter can exclude them | `docs/metrics.md` M1 |
| 10 | 5 | Default dashboard view: Traffic = `real` (hide `/home` and internal testers) or all traffic? | default `real` (58 % not understood vs 1.3 % overall) | `docs/task5/03_plan.md` §G |
| 11 | 5 | Who views the dashboard (roles); is row-level security expected? | read-only role `gold_viewer` + user `checker`; no RLS | `docs/task5/README.md` |
| 12 | all | From-zero acceptance of the whole stack after the presentation: required, and on which machine? | deferred to after the presentation (decided 11-10) | `docs/task3/README.md` |
