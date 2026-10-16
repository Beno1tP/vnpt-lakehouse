# Task 1 — Validation (expected numbers)

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
- [ ] Rerun with the same `DATE` → row counts unchanged; latest snapshot operation = `overwrite`.
- [ ] Run with a new `DATE` → a second partition with the same counts (shows partitioning works).
- [ ] Time travel to the first snapshot returns the first load.
