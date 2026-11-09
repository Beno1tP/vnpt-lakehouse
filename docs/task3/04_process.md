# Task 3 — Process log

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

### 2026-11-09 — 3.0 expected Silver numbers
Decided (Bee): D7–D12 accepted; D13 schedule stays UTC (show ICT); D14 test Silver on Minikube one day by hand (no Compose loop).
Did: Claude wrote `tools/expected_silver.py` (pandas on the Excel files: parse `CREATED_TIME` to a naive wall clock, flags, `final_intent`, `has_chat_row`, `user_message` left join; fails unless totals = Task 1) + 5 unit tests; checked the 2 internal senders = SENDER_IDs containing `@` (counts only, no IDs). Bee ran it.
Observed: `PASS: totals equal the Task 1 numbers`; 18 days; new: internal sender rows 32 (16 client), user_message 10,613 with 10,613 predictions; 07-25 holds all 116 unmatched predict rows.
Next: 3.1 Silver job (`DESCRIBE lake.bronze.bot_predict` not needed: columns read from the Excel header: `INTENT_CONFIDENCE`, `NLU_THRESHOLD`, `UPDATED_INTENT`, `ID_CHATLOG`).

### 2026-11-06 — plan written
Did (Claude): input plan reviewed against the repo (`03_plan.md` §R, 14 points); `docs/task3/01–07` created. Proposed decisions D7–D12 for Bee to confirm.
Next: before step 3.0: `minikube start` (E40/E53), `kubectl get pods -A`, `kubectl describe node minikube | grep -A8 "Allocated resources"`, `minikube ssh -- df -h /var`, `df -h /mnt/c`; `DESCRIBE lake.bronze.bot_predict` (confidence and threshold column names).
