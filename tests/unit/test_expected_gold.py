"""Task 4 step 4.0b: the pure rules of tools/expected_gold.py (definitions in docs/metrics.md, decisions M1-M4)."""

import expected_gold as eg
import pandas as pd


def test_traffic_class_first_match_wins():
    internal = pd.Series([True, True, False, False, False])
    has_chat = pd.Series([True, False, False, True, True])
    is_home = pd.Series([True, None, None, True, False])
    got = list(eg.traffic_class(internal, has_chat, is_home))
    assert got == ["internal", "internal", "unmatched", "home", "real"]  # internal > unmatched (M1) > home > real


def test_intent_group_separates_fallback_and_bot_khong_hieu():
    got = list(eg.intent_group(pd.Series(["xin_chao", "fallback", "bot_khong_hieu", "hoi_dap"])))
    assert got == ["business", "fallback", "bot_khong_hieu", "business"]


def test_calendar_days_cover_the_whole_period():
    days = eg.calendar_days()
    assert days[0] == "2024-07-02" and days[-1] == "2024-07-25" and len(days) == 24  # M3: zero days included


def _users():
    return pd.DataFrame(
        {
            "SENDER_ID": ["a", "a", "b", "x@vnpt.vn"],
            "event_date": ["2024-07-02", "2024-07-24", "2024-07-24", "2024-07-03"],
            "is_client": [True, True, True, True],
        }
    )


def test_uc7_counts_a_user_once_and_has_no_percentage_without_a_base():
    t = eg.uc7(eg.users(_users()))
    last = t[t["event_date"] == "2024-07-25"].set_index("user_class")
    assert last.loc["external", "cumulative_users"] == 2  # 'a' active on 2 days counts once
    assert last.loc["internal", "cumulative_users"] == 1
    assert last.loc["external", "users_30d_before"] == 0 and last.loc["external", "change_pct"] == ""  # M2: NULL


def test_uc8_new_and_active_per_calendar_day():
    t = eg.uc8(eg.users(_users())).set_index(["event_date", "user_class"])
    assert t.loc[("2024-07-24", "external"), "active_users"] == 2
    assert t.loc[("2024-07-24", "external"), "new_users"] == 1  # 'a' was new on 07-02, 'b' on 07-24
    assert t.loc[("2024-07-10", "external"), "active_users"] == 0  # a day without messages is a 0 row (M3)
    assert len(t) == 24 * 2
