# Metrics — UC4–UC8 (Task 4 step 4.0b)

One row per use case: the question from the lab doc ("Yêu cầu dashboard"), the exact formula on Silver, the grain of the Gold table, filters and NULL rules. Gold tables (`gold_uc*`, Task 4) store **counts at their grain**; ratios and averages over a chosen period are computed at query time from those counts (a ratio of sums, never a sum of ratios). Expected values: `tools/expected_gold.py` (pandas on the Excel files) → `dbt/seeds/expected_gold_uc*.csv`.

Status: **accepted 2026-11-10** (Bee: M1–M4 as proposed). `traffic_class` of the 10,729 predicted messages: home 10,378 · real 219 · unmatched 116 · internal 16 (predicted by Claude's local run of `expected_gold.py`; Bee's run is the evidence).

## Common definitions

| Term | Definition (Silver columns) |
|---|---|
| Period ("trong kỳ") | default = the whole dataset, 2024-07-02 → 2024-07-25 (Bee 11-10 for UC4); the dashboard may narrow it with a date filter on `event_date` |
| Day | `event_date` = the date as written in `CREATED_TIME` (Vietnam local, Task 3 D8) |
| Predicted message (base of UC4–UC6) | one row of `silver.nlu_prediction`: **10,729** rows, incl. the 116 without a chat row (Bee 11-10: same base as `final_intent`) |
| Understood / not understood | not understood = `final_intent IN ('fallback', 'bot_khong_hieu')`; understood = every other `final_intent` (lab doc UC5) |
| Scenario ("kịch bản", UC4) | `step` (predict `STEP`, added to Silver in 4.0a): `406665` = start scenario (`/home` → greeting), `406666` = free-text scenario (NLU). Labels inferred from the data (`docs/task4/04_process.md` 11-10), not from a catalogue: leader question |
| User ("người chat", UC7/UC8) | a distinct `sender_id` of a **client** message in `silver.chat_message` (`is_client`) |
| First chat of a user | `min(event_date)` of the user's client messages over **all** data (not only the selected period) |
| `traffic_class` | message level, first match wins: `internal` if the sender looks like an e-mail address; `unmatched` if a predict row has no chat row (M1); `home` if the client text is `/home`; else `real` |
| `user_class` | user level (M4): `internal` if the sender looks like an e-mail address, else `external` |

## Use cases

| UC | Lab doc question | Formula | Gold grain | Known totals (whole period) |
|---|---|---|---|---|
| **UC4** Scenarios | messages per scenario in the period; share of each scenario in all messages; most used scenarios | `count(*)` of predicted messages per `step`; share = count / total of the period; rank by count | `gold_uc4`: `event_date × step × traffic_class` → `message_cnt` | 406665: 10,494 (97.81 %) · 406666: 235 (2.19 %) |
| **UC5** Understood / not understood | per day: understood and not-understood messages; how the not-understood rate moves over time | per `event_date`: `understood_cnt`, `not_understood_cnt`; rate = not_understood / (understood + not_understood) | `gold_uc5`: `event_date × traffic_class` → `understood_cnt`, `not_understood_cnt` | understood 10,588 · not understood 141 (107 fallback + 34 bot_khong_hieu) · rate 1.31 % |
| **UC6** Intents | intents with the most messages in the period, at least top 12; business intents separated from fallback and bot_khong_hieu | `count(*)` per `final_intent`, ranked; `intent_group` = `fallback` / `bot_khong_hieu` / `business` (every other intent) | `gold_uc6`: `event_date × final_intent × intent_group × traffic_class` → `message_cnt` | xin_chao 10,588 · fallback 107 · bot_khong_hieu 34 (3 intents in the data; generic top-N, leader 10-23) |
| **UC7** Total users | distinct users up to now; increase or decrease vs 30 days earlier; a user counted once even if active on several days | users as of day D = distinct users with first chat ≤ D; change = users(D) − users(D − 30 days): see M2 | `gold_uc7`: `event_date` (every calendar day of the period) → `cumulative_users`, plus the `sender_key` bitmap per day for exact unions | 89 users on 07-25 (87 external + 2 internal) = Task 1's 89 distinct senders; change vs D−30 = +89, % NULL |
| **UC8** New / active users | per day: new users (first chat that day) and active users (chatted that day); average of both per day in the period | new(D) = users whose first chat = D; active(D) = distinct users with a client message on D; averages = sum / number of days: see M3 | `gold_uc8`: `event_date × user_class` → `new_users`, `active_users` (+ bitmap) | 24 calendar days: avg new 3.71/day (89 / 24), avg active 4.67/day |

## Decisions (accepted 2026-11-10)

| ID | Question | Proposed | Alternative | Why |
|---|---|---|---|---|
| M1 | `traffic_class` of the 116 predict rows without a chat row (UC4–UC6) | `unmatched` (a 4th value) | count them as `real` | their text is unknown, so `home` vs `real` cannot be decided; a 4th value keeps every row and lets the filter exclude them (flag, don't delete) |
| M2 | UC7 "vs 30 days earlier" with only 24 days of data | `users(D) − users(D − 30)`; users before 2024-07-02 = 0 (the lake starts there), so the change = users(D) and the % change = NULL (division by 0) | window anchored to the last day with a `window_days` parameter | the lab doc's wording, computed honestly; NULL instead of an invented percentage |
| M3 | UC8 average "per day in the period": which days count | every **calendar** day of the period (24), days without messages count as 0 | only the 18 days with data | the source is one export for the whole period, split by day: a day without rows had no messages (0 is a fact, not missing data) |
| M4 | Traffic filter for users (UC7/UC8) | `user_class` = `internal` (sender looks like an e-mail) / `external`; a user who only pressed `/home` is still a user | "real users" = users with at least one non-`/home` message | "người chat" = a distinct sender; the lab doc does not exclude `/home`; ~98 % of client messages are `/home`, a message-level filter would leave almost no users |

## Leader questions (copy to the Task 4 plan §G)

1. Scenario names for `STEP` 406665 / 406666 (catalogue?), or are the inferred labels OK?
2. UC7 with 24 days of data: show the absolute change and leave the % empty (M2)?
3. UC8 average over calendar days, zero days included (M3)?
