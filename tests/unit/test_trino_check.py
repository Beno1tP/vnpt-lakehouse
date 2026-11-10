import trino_check as tc

ROWS = [
    "2024-07-09,chat_rows,86",
    "2024-07-09,client_rows,43",
    "2024-07-09,final_intent.fallback,18",
    "2024-07-09,final_intent.xin_chao,19",
    "2024-07-25,chat_rows,7552",
    "2024-07-25,final_intent.xin_chao,3892",
    "",
]


def test_parse_long_rows_into_days():
    days = tc.parse(ROWS)
    assert days["2024-07-09"]["chat_rows"] == 86
    assert days["2024-07-09"]["final_intent"] == {"fallback": 18, "xin_chao": 19}


def test_totals_sum_days_and_intents():
    t = tc.totals(tc.parse(ROWS))
    assert t["chat_rows"] == 86 + 7552 and t["event_dates"] == 2
    assert t["final_intent"] == {"fallback": 18, "xin_chao": 19 + 3892}


def test_compare_flags_missing_and_unexpected_keys():
    report: list[bool] = []
    tc.compare({"a": 1, "extra": 2}, {"a": 1, "b": 3}, "d", report)
    assert report.count(False) == 2  # b missing in Trino, extra not expected
