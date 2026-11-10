"""Task 3 step 3.0: expected Silver numbers, computed with pandas straight from the two Excel files (an engine
independent of Spark and Trino, plan R7). Writes tests/expected/silver.json (totals) and silver_daily.json (per
event_date). Counts only: no row contents, no sender IDs.

Rules (docs/task3/01_knowledge.md A1, decisions D7-D9):
- event day = the date as written in CREATED_TIME (Vietnam local, no zone): `02-JUL-24 03.40.09.895455000 PM`
- is_client: SOURCE = 'client'; is_home: TEXT = '/home'; is_internal_sender: SENDER_ID looks like an e-mail address
- final_intent: INTENT_NAME = 'fallback' -> fallback; INTENT_CONFIDENCE < the row's NLU_THRESHOLD -> bot_khong_hieu;
  else COALESCE(UPDATED_INTENT, INTENT_NAME)
- step: predict STEP as delivered (the bot scenario; Task 4 step 4.0a, UC4), counted per value
- nlu_prediction.has_chat_row: ID_CHATLOG found in chatlog ID (116 are not); user_message = client chat rows LEFT JOIN
  their prediction
Fails (exit 1) unless the totals equal the Task 1 numbers in docs/task1/06_validation.md.

    .venv/bin/python tools/expected_silver.py [--src ~/data/vnpt-lab] [--out tests/expected]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

import pandas as pd

SHEET = "Export Worksheet"
CHAT, PREDICT = "bot_dvc_hcm.xlsx", "bot_ddvc_hcm_bot_predict.xlsx"
FRACTION_9 = re.compile(r"\.(\d{6})\d{3} ")  # keep microseconds, like Spark (nanosecond digits 7-9 dropped)
TASK1 = {  # docs/task1/06_validation.md: the totals this script must reproduce
    "chat_rows": 21224,
    "client_rows": 10613,
    "client_home_rows": 10378,
    "predict_rows": 10729,
    "predict_without_chat_row": 116,
    "final_intent": {"xin_chao": 10588, "fallback": 107, "bot_khong_hieu": 34},
    "step": {"406665": 10494, "406666": 235},  # profiled 2026-11-10 (docs/task4/04_process.md)
}


def parse_created_time(values: pd.Series) -> pd.Series:
    """Oracle text without zone -> naive datetime (wall clock as written). Raises on any value that does not parse."""
    trimmed = values.str.replace(FRACTION_9, lambda m: f".{m.group(1)} ", regex=True)
    return pd.to_datetime(trimmed, format="%d-%b-%y %I.%M.%S.%f %p", errors="raise")


def is_email_like(sender_ids: pd.Series) -> pd.Series:
    return sender_ids.fillna("").str.contains("@", regex=False)


def final_intent(intent: pd.Series, confidence: pd.Series, threshold: pd.Series, updated: pd.Series) -> pd.Series:
    conf, thr = confidence.astype(float), threshold.astype(float)
    if conf.isna().any() or thr.isna().any():
        raise ValueError("INTENT_CONFIDENCE or NLU_THRESHOLD has empty values: decide the rule before computing")
    chosen = updated.where(updated.notna() & (updated != ""), intent)
    out = chosen.where(conf >= thr, "bot_khong_hieu")
    return out.where(intent != "fallback", "fallback")


def load(src: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    chat = pd.read_excel(os.path.join(src, CHAT), sheet_name=SHEET, dtype=str).drop_duplicates("ID", keep="last")
    pred = pd.read_excel(os.path.join(src, PREDICT), sheet_name=SHEET, dtype=str).drop_duplicates("ID", keep="last")
    chat["event_date"] = parse_created_time(chat["CREATED_TIME"]).dt.date.astype(str)
    pred["event_date"] = parse_created_time(pred["CREATED_TIME"]).dt.date.astype(str)
    chat["is_client"] = chat["SOURCE"] == "client"
    chat["is_home"] = chat["TEXT"] == "/home"
    chat["is_internal_sender"] = is_email_like(chat["SENDER_ID"])
    pred["final_intent"] = final_intent(
        pred["INTENT_NAME"], pred["INTENT_CONFIDENCE"], pred["NLU_THRESHOLD"], pred["UPDATED_INTENT"]
    )
    pred["has_chat_row"] = pred["ID_CHATLOG"].isin(set(chat["ID"]))
    return chat, pred


def user_message(chat: pd.DataFrame, pred: pd.DataFrame) -> pd.DataFrame:
    if pred["ID_CHATLOG"].duplicated().any():
        raise ValueError("a chat ID has more than one prediction: user_message would multiply rows")
    um = chat[chat["is_client"]].merge(
        pred[["ID_CHATLOG", "final_intent"]], how="left", left_on="ID", right_on="ID_CHATLOG"
    )
    um["has_prediction"] = um["ID_CHATLOG"].notna()
    return um


def counts(chat: pd.DataFrame, pred: pd.DataFrame, um: pd.DataFrame) -> dict:
    return {
        "chat_rows": int(len(chat)),
        "client_rows": int(chat["is_client"].sum()),
        "client_home_rows": int((chat["is_client"] & chat["is_home"]).sum()),
        "internal_sender_rows": int(chat["is_internal_sender"].sum()),
        "internal_sender_client_rows": int((chat["is_internal_sender"] & chat["is_client"]).sum()),
        "predict_rows": int(len(pred)),
        "predict_without_chat_row": int((~pred["has_chat_row"]).sum()),
        "final_intent": {k: int(v) for k, v in sorted(pred["final_intent"].value_counts().items())},
        "step": {k: int(v) for k, v in sorted(pred["STEP"].value_counts().items())},
        "user_message_rows": int(len(um)),
        "user_message_with_prediction": int(um["has_prediction"].sum()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=os.path.expanduser("~/data/vnpt-lab"))
    ap.add_argument("--out", default="tests/expected")
    args = ap.parse_args()
    chat, pred = load(args.src)
    um = user_message(chat, pred)
    totals = counts(chat, pred, um)
    totals["event_dates"] = int(chat["event_date"].nunique())
    days = {}
    for day in sorted(set(chat["event_date"]) | set(pred["event_date"])):
        c, p = chat[chat["event_date"] == day], pred[pred["event_date"] == day]
        days[day] = counts(c, p, um[um["event_date"] == day])
    mismatch = {k: (totals[k], v) for k, v in TASK1.items() if totals[k] != v}
    meta = {
        "_comment": "Expected Silver counts (Task 3 step 3.0), pandas on the two Excel files; "
        "tools/expected_silver.py. Day = date as written in CREATED_TIME (Vietnam local). Counts only.",
        "rules": "is_client SOURCE='client'; is_home TEXT='/home'; is_internal_sender SENDER_ID contains '@'; "
        "final_intent fallback > (confidence < row NLU_THRESHOLD -> bot_khong_hieu) "
        "> COALESCE(UPDATED_INTENT, INTENT_NAME); step = predict STEP as delivered",
    }
    with open(os.path.join(args.out, "silver.json"), "w") as f:
        json.dump({**meta, "totals": totals}, f, indent=2)
        f.write("\n")
    with open(os.path.join(args.out, "silver_daily.json"), "w") as f:
        json.dump({**meta, "days": days}, f, indent=2)
        f.write("\n")
    print(f"[expected-silver] {len(days)} days; totals: {json.dumps(totals)}")
    if mismatch:
        sys.exit(f"[expected-silver] FAIL: differs from Task 1 (got, expected): {mismatch}")
    print("[expected-silver] PASS: totals equal the Task 1 numbers")


if __name__ == "__main__":
    main()
