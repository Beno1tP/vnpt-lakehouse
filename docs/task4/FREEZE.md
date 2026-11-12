# Task 4 — Freeze record (task4_v1 / git tag task4-v1)

Frozen 2026-11-12. Everything needed to say exactly what Task 4's Gold was built from, and to rebuild it. Counts and
identifiers only (no row contents). Evidence copies live outside Git under `~/data/vnpt-lab/evidence/`.

## 1. Data: Iceberg tags (input of Gold)

`make freeze TASK=4` at 02:28:03 UTC (09:28 ICT), job `catalog-022803`, retain 365 days, rows verified = expected
(`tests/expected/bronze_daily.json`, `silver.json`). Read again through StarRocks (`make starrocks-tag-check TASK=4`,
5 × PASS) and through Spark (`freeze`).

| Table | `task4_v1` snapshot | Rows | `task3_v1` (11-10) | `task2_v1` (11-09) |
|---|---|---|---|---|
| bronze.chatlog | **4851909217252046539** | 21,224 | 7862409111351472734 | 4460393597214113888 |
| bronze.bot_predict | **7994949553350528317** | 10,729 | 6213223788355037792 | 3649899175837714746 |
| silver.chat_message | **4324401880006836136** | 21,224 | 3502470701764024213 | — |
| silver.nlu_prediction | **6940182659003592517** | 10,729 (`step` filled) | 1150847673300657609 (`step` NULL) | — |
| silver.user_message | **9106066062539116072** | 10,613 | 4444370878485053444 | — |

Why every snapshot moved since `task3_v1`: 4.0a added `step` to Silver (schema evolution + 18-day backfill, 11-10) and
07-24 was rerun end to end on 11-11 (4.5 test), rewriting that day's Bronze and Silver partitions.

**Tags survive maintenance** (drill on a scratch table, 11-12, `06_validation.md` §7): `expire_snapshots` with
`older_than = now, retain_last = 1` removed the untagged snapshot and kept the tagged one (rows 2 at the tag, 3 now).
Real tables were not used for the drill (Task 1 docs rely on untagged Bronze history).

## 2. Code and images (deployed by digest, `infra/k8s/images.env` at the git tag)

| Image | Digest | Built from |
|---|---|---|
| vnpt-lake-spark (base) | `sha256:aee0deb990471c57e37936f10aa6f91880ef7cff3713e09a4cc974d5bb0cad5c` | tag 3.5.9-iceberg1.11.0-r1 |
| vnpt-lake-hms (base) | `sha256:9446f7d1e0ab211f1fe32b0f8def318699e6afa40a5e1fcebfa3d07d134b91ff` | tag 3.1.3-hadoopaws3.1.0-r1 |
| vnpt-lake-spark-job | `sha256:196034f1092f7a2713c78c8c003096b2940806e60c0affb56c50abf69fb4aa42` | c13f38e |
| vnpt-lake-airflow | `sha256:506daaed57c74eed8dc97666e23e0f2b5335729c265ed73b14bcec83b24b80b8` | c832a1e |
| vnpt-lake-dbt | `sha256:a7dc01a29a4d1346af06a7e3416ec5f5f6ffbc42e94a6afcc111a8ae6c9075f6` | 5e4269e |

Third-party, pinned: StarRocks FE `starrocks/fe-ubuntu:3.5.21@sha256:cbc2460aa325c69c143c6497c3ae27822328aec3c74fb17525bb387a719d9fc6`,
BE `starrocks/be-ubuntu:3.5.21@sha256:6ed9e75bfb7fd6b48947b8b75f5811a49abe3b793595e4cd4239a6dba7fcbe9f`, operator
`starrocks/operator:v1.11.8` (tag); Trino `trinodb/trino:483@sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534`;
Spark operator `2.2.1@sha256:865ff4da56869cc817706a31e44e4d039619ca3e17caea49ea6ae3a0c51f948a`; MinIO
`cgr.dev/chainguard/minio@sha256:2136d593beacf4461187035eca949fe49abd589d5079735025c711ca58fee0c8`, client
`cgr.dev/chainguard/minio-client@sha256:1a1b84b36947e93b2d0eb9373e1c886eadfdcf223c91f47bcaa3756d3437785c`; Postgres
`postgres:16@sha256:1a6ab3f5345eb6dbe04a1349529caabdb0ab09293a09590fad07b2246bfa4b54`; dbt image base
`python:3.11-slim@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535` with dbt-core 1.12.6 +
dbt-starrocks 1.12.2.

Charts (downloaded once, checksum-verified by make): kube-starrocks 1.11.8 SHA-256 `c05804d0dc38719557c68e23713437e38b82be97db73799e59e1d5bc113c2286`;
trino 1.44.0 SHA-256 `ce3c2f6525d896513c5e20cac19a1260bf108f1d17f65188be45ccea9fb36caa`; spark-operator 2.2.1
SHA-256 `1c47c850c8d6ecc7eda4ede03e7932aa59f23fa6d186a10da3c8359df3671cf1`; airflow 1.22.0 SHA-512
`59a6b157…1a71c70` (full value in the Makefile).

## 3. Gold (StarRocks internal tables): reproducible, not snapshotted

StarRocks internal tables have no Iceberg-style tags; a `BACKUP SNAPSHOT` would need a repository on MinIO (bucket +
user + policy) for tables rebuilt in about 10 s. Gold is frozen as **inputs + code**: the tags above + the dbt image
digest. Proven 11-12 (07 §5, E77):

| Run | Result |
|---|---|
| `dbt compile --select fct_prediction --vars '{silver_version: task4_v1}'` | `FOR VERSION AS OF 'task4_v1'` twice in the table model's own SQL (staging inlined as ephemeral) |
| `dbt build --vars '{silver_version: task4_v1}'` | PASS=53 ERROR=0 (all reconcile tests vs pandas) = Gold rebuilt from the tag |
| `dbt build --vars '{silver_version: task3_v1}'` (negative) | Silver gate `FAIL 3` (no `step` at that tag) + errors: the run really read the tag |
| `dbt build` (current Silver) | PASS=56 ERROR=0; `make gold-check` 22/22 |

Limits (stated, not hidden): a StarRocks **view** cannot pin a tag (the clause is dropped from its stored definition,
E77); in a tag rebuild the Silver gate does not hold back the marts (dbt's test edges disappear with ephemeral
staging) → `make dbt-tag-rebuild` runs the gate first (E78); time travel reaches only tagged or not-yet-expired
snapshots.

Gold numbers at the freeze (`make gold-check`, counts + measure sums): dim_date 24 · dim_intent 3 · dim_sender 89
(max key 89) · fct_prediction 10,729 (real 219) · fct_user_message 10,613 · gold_uc4 36 rows / 10,729 msgs (step
406666: 235) · gold_uc5 36 / understood 10,588 · not 141 · gold_uc6 57 / 10,729 (business 10,588) · gold_uc7 48 /
89 users on 07-25 · gold_uc8 48 / new 89 · active 112 · bitmap union 89.

## 4. Runs and evidence

| What | Where / id |
|---|---|
| Asset-driven Gold build (Silver 07-24 → trino_reconcile → gold_build) | `asset_triggered__2026-11-11T02:55:58…_V89QJ9d3` success (attempt 2 after E76) |
| Manual Gold build after E76 | `manual__2026-11-11T03:23:54…` success, dbt_build attempt 1 PASS=56 |
| Evidence copy | `~/data/vnpt-lab/evidence/task4-20261112/` (180 MB: 11 state files, S3 648/648 files, 284 Spark event logs) |
| dbt docs (lineage) | `~/data/vnpt-lab/evidence/task4-dbt-docs-20261111T0339Z/lineage.html` (+ manifest, catalog) |
| Errors | E67–E78 in `05_errors_and_fixes.md` |

## 5. Rebuild from this freeze (procedure)

1. `git checkout task4-v1` (images.env digests = §2) → `make k8s-up` → `make mode-gold`.
2. `make starrocks-tag-check TASK=4` (5 × PASS).
3. `make dbt-tag-rebuild TAG=task4_v1` (gate, then build from the tag) → `make gold-check` (22/22 = §3).
4. `make dbt CMD="build"` to return Gold to current Silver.
