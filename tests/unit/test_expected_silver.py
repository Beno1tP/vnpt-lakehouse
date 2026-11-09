import expected_silver as es
import pandas as pd
import pytest


def test_parse_created_time_keeps_wall_clock_and_microseconds():
    got = es.parse_created_time(pd.Series(["02-JUL-24 03.40.09.895455123 PM", "24-JUL-24 12.05.00.000000000 AM"]))
    assert str(got[0]) == "2024-07-02 15:40:09.895455"  # 9 digits -> microseconds, PM -> 15 h
    assert str(got[1]) == "2024-07-24 00:05:00"  # 12 AM = midnight: the day stays 07-24
    assert got[0].tzinfo is None  # no zone: Vietnam local as written


def test_parse_created_time_rejects_garbage():
    with pytest.raises(ValueError):
        es.parse_created_time(pd.Series(["2024-07-02 15:40"]))


def test_final_intent_rule_order():
    intent = pd.Series(["fallback", "xin_chao", "xin_chao", "xin_chao", "xin_chao"])
    conf = pd.Series(["0.10", "0.79", "0.80", "0.95", "0.95"])
    thr = pd.Series(["0.8"] * 5)
    upd = pd.Series([None, None, None, None, "hoi_dap"])
    got = list(es.final_intent(intent, conf, thr, upd))
    assert got == ["fallback", "bot_khong_hieu", "xin_chao", "xin_chao", "hoi_dap"]  # at threshold = understood


def test_final_intent_refuses_empty_confidence():
    with pytest.raises(ValueError):
        es.final_intent(pd.Series(["x"]), pd.Series([None]), pd.Series(["0.8"]), pd.Series([None]))


def test_email_like_sender():
    assert list(es.is_email_like(pd.Series(["a@b.vn", "123456", None]))) == [True, False, False]
