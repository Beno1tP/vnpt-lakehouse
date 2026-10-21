# Task 1 — Validation (expected numbers)

**Automated since 2026-10-21:** `make validate DATE=…` / `make acceptance DATE=…` run the checks in `tests/sql/bronze_checks.sql` against `tests/expected/bronze.json` (the numbers below) and exit non-zero on any FAIL (`10_operations.md` §3). The manual results below are kept as history.

Computed with pandas from the two lab files on 2026-10-15. Every Iceberg table you write must match these.

## Bronze (raw, strings)
| Check | chatlog (`bot_dvc_hcm.xlsx`) | bot_predict (`bot_ddvc_hcm_bot_predict.xlsx`) |
|---|---|---|
| Rows | **21,224** | **10,729** |
| Source columns | 15 (+4 metadata) | 17 (+4 metadata) |
| Duplicate `ID` | 0 | 0 |
| Distinct `SENDER_ID` | 89 | 89 |
| Completely empty columns | ADD_INFO, SESSION_ID, LOGSTT_ID, AUDIO_URL, PAGE_ID | UPDATED_INTENT, STATUS_DELETE, STATUS_CONFIRM, INTENT_MAP_CLICK_BUTTON |

```sql
SELECT count(*), count(DISTINCT ID), count(DISTINCT SENDER_ID) FROM lake.bronze.chatlog WHERE _ingest_date = DATE '<date>';
SELECT count(*) FROM lake.bronze.bot_predict WHERE _ingest_date = DATE '<date>';
SELECT snapshot_id, operation, summary['added-records'] FROM lake.bronze.chatlog.snapshots;  -- rerun → 'overwrite'
```

## Distributions (for the Silver stretch goal and later tasks)
| Check | Expected |
|---|---|
| chatlog `SOURCE` | client 10,613 · bot 10,611 |
| chatlog `INPUT_CHANNEL` | livechat 21,192 · platform 32 |
| Client messages with `TEXT = '/home'` | 10,378 of 10,613 (~97.8%) |
| `CREATED_TIME` parse failures | 0 (chatlog and predict) |
| Time range (chatlog) | 2024-07-02 15:39 → 2024-07-25 11:01, **18 distinct days** |
| Rows on 07-24 / 07-25 | 12,970 / 7,552 (≈97% of all rows) |
| predict `ID_CHATLOG` found in chatlog `ID` | 10,613 matched · **116 unmatched** |

Parsing note: Python's `%f` takes at most 6 fraction digits, so the 9-digit Oracle fraction was cut to 6 before parsing (`%d-%b-%y %I.%M.%S.%f %p`). In Spark use `to_timestamp(col, 'dd-MMM-yy hh.mm.ss.SSSSSSSSS a')` and confirm 0 nulls.

## Idempotency checks
- [x] Rerun with the same `DATE` → row counts unchanged; latest snapshot operation = `overwrite`.
- [x] Run with a new `DATE` → a second partition with the same counts (shows partitioning works).
- [x] Time travel to the first snapshot returns the first load.

## Results — Bronze, 2026-10-16 (Spark 3.5.9 / Iceberg 1.11.0 / HMS 3.1.3)
| Check | Expected | Actual |
|---|---|---|
| chatlog rows / distinct ID / distinct SENDER_ID / non-null in the 5 empty cols | 21,224 / 21,224 / 89 / 0 | ✅ same |
| predict rows / distinct ID / distinct SENDER_ID / non-null in the 4 empty cols | 10,729 / 10,729 / 89 / 0 | ✅ same |
| Columns (source + 4 metadata) | 19 / 21 | ✅ 19 / 21 (`DESCRIBE`) |
| `SOURCE` client / bot; client `/home` | 10,613 / 10,611; 10,378 | ✅ same |
| `INPUT_CHANNEL` livechat / platform | 21,192 / 32 | ✅ same |
| predict without chat row | 116 | ✅ 116 |
| New fact | — | bot rows with `TEXT='/home'`: 10,377 |
| `final_intent` (rule in CLAUDE.md) | — | xin_chao 10,588 · fallback 107 · bot_khong_hieu 34; `UPDATED_INTENT` 0 non-null |

Snapshots of `lake.bronze.chatlog`:
| committed_at | operation | added | deleted | total | what |
|---|---|---|---|---|---|
| 16:29:04 | append | 21,224 | NULL | 21,224 | first load (`create()`), DATE=10-16 |
| 16:42:08 | overwrite | 21,224 | 21,224 | 21,224 | rerun DATE=10-16 → partition replaced |
| 17:04:26 | overwrite | 21,224 | NULL | 42,448 | DATE=10-17 → new partition, 10-16 untouched |

Time travel: `VERSION AS OF <snapshot 1>` and `TIMESTAMP AS OF '2026-10-16 16:35:00'` both return the first batch (`_batch_id` differs from the current one), 21,224 rows. `files` = 1 vs `all_data_files` = 2 before the 10-17 load → overwritten files stay on MinIO until `expire_snapshots`.

## Failure drills (2026-10-18)
| Drill | How | Result |
|---|---|---|
| A. Catalog down | `$DC stop hive-metastore` → `make bronze` | Fails fast: `RuntimeMetaException: Failed to connect to Hive Metastore` ← `UnknownHostException: hive-metastore` (stopped container leaves Docker DNS); `make` exit **2**; no new snapshot |
| B. Spark killed between the two table commits | job in background (`exec -T … < /dev/null`, `PYTHONUNBUFFERED=1`), `docker compose kill spark` the moment `[bronze] bronze.chatlog` is printed | Job exit **137** (SIGKILL). `chatlog` = new batch `72ec327f…` (21,224), `bot_predict` = old batch `d435c42b…` (10,729): each table complete, tables out of step. No orphan files (predict: 4 files on MinIO = 4 in `all_data_files`) |
| Recovery | `make bronze DATE=2026-10-16` | Both tables on batch `5717899a…`, 21,224 / 10,729 |

Conclusions: Iceberg commits are **atomic per table**; there is **no cross-table transaction**, so a crash between commits leaves tables on different batches until the (idempotent) rerun. Silver should check matching `_batch_id`/`_ingest_date` before joining. The window between the two commits is ~2.7 s, so the kill had to be triggered by the log line.
