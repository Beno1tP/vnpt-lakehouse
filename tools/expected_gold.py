"""Task 4 step 4.0b: expected Gold values for UC4-UC8, computed with pandas straight from the two Excel files (an engine
independent of Spark, Trino and StarRocks). Definitions: docs/metrics.md (decisions M1-M4). Writes one CSV per UC at
the grain of its Gold table into dbt/seeds/ (expected_gold_uc4.csv ... uc8.csv); dbt loads them as seeds and singular
tests compare every Gold row with them. Counts only: no row contents, no sender IDs.

Reuses tools/expected_silver.py (same parsing and rules as the Silver expectations), and fails (exit 1) unless the
totals equal the Silver numbers already reconciled in Task 3 (pandas = Spark = Trino).

    .venv/bin/python tools/expected_gold.py [--src ~/data/vnpt-lab] [--out dbt/seeds]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import expected_silver as es  # noqa: E402

PERIOD = (date(2024, 7, 2), date(2024, 7, 25))  # the whole dataset (metrics.md "Period")
NOT_UNDERSTOOD = ("fallback", "bot_khong_hieu")
COMPARE_DAYS = 30  # UC7: "so với 30 ngày trước" (M2)


def calendar_days(start: date = PERIOD[0], end: date = PERIOD[1]) -> list[str]:
    return [str(start + timedelta(days=i)) for i in range((end - start).days + 1)]


def traffic_class(internal: pd.Series, has_chat_row: pd.Series, is_home: pd.Series) -> pd.Series:
    """First match wins (metrics.md): internal > unmatched (no chat row, M1) > home > real."""
    out = pd.Series("real", index=internal.index)
    out = out.where(~is_home.astype("boolean").fillna(False).astype(bool), "home")
    out = out.where(has_chat_row.astype(bool), "unmatched")
    return out.where(~internal.astype(bool), "internal")


def intent_group(final_intent: pd.Series) -> pd.Series:
    return final_intent.where(final_intent.isin(NOT_UNDERSTOOD), "business")


def predicted_messages(chat: pd.DataFrame, pred: pd.DataFrame) -> pd.DataFrame:
    """One row per predict row (base of UC4-UC6) with its traffic_class."""
    text = chat.set_index("ID")["is_home"]
    p = pred.copy()
    p["is_home"] = p["ID_CHATLOG"].map(text)
    p["traffic_class"] = traffic_class(es.is_email_like(p["SENDER_ID"]), p["has_chat_row"], p["is_home"])
    p["intent_group"] = intent_group(p["final_intent"])
    return p


def users(chat: pd.DataFrame) -> pd.DataFrame:
    """One row per (user, active day) of client messages, with first chat and user_class (UC7/UC8, M4)."""
    c = chat[chat["is_client"]][["SENDER_ID", "event_date"]].drop_duplicates()
    first = c.groupby("SENDER_ID")["event_date"].min().rename("first_date")
    c = c.join(first, on="SENDER_ID")
    c["user_class"] = es.is_email_like(c["SENDER_ID"]).map({True: "internal", False: "external"})
    return c


def uc4(p: pd.DataFrame) -> pd.DataFrame:
    g = p.groupby(["event_date", "STEP", "traffic_class"]).size().rename("message_cnt").reset_index()
    return g.rename(columns={"STEP": "step"})


def uc5(p: pd.DataFrame) -> pd.DataFrame:
    p = p.assign(not_understood=p["final_intent"].isin(NOT_UNDERSTOOD))
    g = p.groupby(["event_date", "traffic_class"])["not_understood"].agg(["size", "sum"]).reset_index()
    g["not_understood_cnt"] = g["sum"].astype(int)
    g["understood_cnt"] = (g["size"] - g["sum"]).astype(int)
    return g[["event_date", "traffic_class", "understood_cnt", "not_understood_cnt"]]


def uc6(p: pd.DataFrame) -> pd.DataFrame:
    keys = ["event_date", "final_intent", "intent_group", "traffic_class"]
    return p.groupby(keys).size().rename("message_cnt").reset_index()


def uc7(u: pd.DataFrame) -> pd.DataFrame:
    """Cumulative distinct users per calendar day and user_class; change vs D-30 (users before the lake = 0, M2)."""
    first = u[["SENDER_ID", "first_date", "user_class"]].drop_duplicates()
    rows = []
    for day in calendar_days():
        before = str(date.fromisoformat(day) - timedelta(days=COMPARE_DAYS))
        for cls in ("external", "internal"):
            f = first[first["user_class"] == cls]
            now, then = int((f["first_date"] <= day).sum()), int((f["first_date"] <= before).sum())
            pct = "" if then == 0 else round(100.0 * (now - then) / then, 2)  # NULL in the seed, not inf (M2)
            rows.append([day, cls, now, then, now - then, pct])
    cols = ["event_date", "user_class", "cumulative_users", "users_30d_before", "change_abs", "change_pct"]
    return pd.DataFrame(rows, columns=cols)


def uc8(u: pd.DataFrame) -> pd.DataFrame:
    """New and active users per calendar day (days without messages = 0, M3) and user_class."""
    rows = []
    for day in calendar_days():
        for cls in ("external", "internal"):
            d = u[(u["user_class"] == cls)]
            active = int(d[d["event_date"] == day]["SENDER_ID"].nunique())
            new = int(d[d["first_date"] == day]["SENDER_ID"].nunique())
            rows.append([day, cls, new, active])
    return pd.DataFrame(rows, columns=["event_date", "user_class", "new_users", "active_users"])


def check(p: pd.DataFrame, t7: pd.DataFrame, t8: pd.DataFrame, silver_totals: dict) -> list[str]:
    """Gold totals must equal the Silver numbers reconciled in Task 3."""
    errs = []
    if len(p) != silver_totals["predict_rows"]:
        errs.append(f"predicted messages {len(p)} != predict_rows {silver_totals['predict_rows']}")
    got = {k: int(v) for k, v in p["final_intent"].value_counts().items()}
    if got != silver_totals["final_intent"]:
        errs.append(f"final_intent {got} != {silver_totals['final_intent']}")
    got = {k: int(v) for k, v in p["STEP"].value_counts().items()}
    if got != silver_totals["step"]:
        errs.append(f"step {got} != {silver_totals['step']}")
    if int((p["traffic_class"] == "unmatched").sum()) > silver_totals["predict_without_chat_row"]:
        errs.append("more unmatched rows than predict rows without a chat row")
    last = t7[t7["event_date"] == calendar_days()[-1]]["cumulative_users"].sum()
    if last != t8["new_users"].sum():
        errs.append(f"UC7 users on the last day {last} != sum of UC8 new users {t8['new_users'].sum()}")
    return errs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=os.path.expanduser("~/data/vnpt-lab"))
    ap.add_argument("--out", default="dbt/seeds")
    ap.add_argument("--silver", default="tests/expected/silver.json")
    args = ap.parse_args()
    chat, pred = es.load(args.src)
    p, u = predicted_messages(chat, pred), users(chat)
    tables = {"uc4": uc4(p), "uc5": uc5(p), "uc6": uc6(p), "uc7": uc7(u), "uc8": uc8(u)}
    os.makedirs(args.out, exist_ok=True)
    for name, df in tables.items():
        df.sort_values(list(df.columns[:-1])).to_csv(os.path.join(args.out, f"expected_gold_{name}.csv"), index=False)
    with open(args.silver) as f:
        errs = check(p, tables["uc7"], tables["uc8"], json.load(f)["totals"])
    t7_last = tables["uc7"][tables["uc7"]["event_date"] == calendar_days()[-1]]
    summary = {
        "uc4_step": {k: int(v) for k, v in p["STEP"].value_counts().items()},
        "traffic_class": {k: int(v) for k, v in p["traffic_class"].value_counts().items()},
        "uc5_not_understood": int(tables["uc5"]["not_understood_cnt"].sum()),
        "uc7_users_last_day": {r.user_class: int(r.cumulative_users) for r in t7_last.itertuples()},
        "uc8_days": len(calendar_days()),
        "uc8_avg_new_per_day": round(tables["uc8"].groupby("event_date")["new_users"].sum().mean(), 2),
        "uc8_avg_active_per_day": round(tables["uc8"].groupby("event_date")["active_users"].sum().mean(), 2),
        "rows": {k: len(v) for k, v in tables.items()},
    }
    print(f"[expected-gold] {json.dumps(summary)}")
    if errs:
        sys.exit(f"[expected-gold] FAIL: {errs}")
    print("[expected-gold] PASS: totals equal the Silver numbers (Task 3)")


if __name__ == "__main__":
    main()
