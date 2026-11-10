import pytest
import trino_check as tc

ROWS = [
    "2024-07-09,chat_rows,86",
    "2024-07-09,client_rows,43",
    "2024-07-09,final_intent.fallback,18",
    "2024-07-09,final_intent.xin_chao,19",
    "2024-07-25,chat_rows,7552",
    "2024-07-25,final_intent.xin_chao,3892",
    "2024-07-25,step.406665,3892",
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
    assert t["step"] == {"406665": 3892}  # nested like final_intent (Task 4 4.0a)
    assert tc.parse(ROWS)["2024-07-09"]["step"] == {}  # a day without step rows still has the key


def test_compare_flags_missing_and_unexpected_keys():
    report: list[bool] = []
    tc.compare({"a": 1, "extra": 2}, {"a": 1, "b": 3}, "d", report)
    assert report.count(False) == 2  # b missing in Trino, extra not expected


def test_reconcile_counts_all_days_and_totals():
    import lake_reconcile as lr

    got = lr.parse(
        [["2024-07-09", "chat_rows", "86"], ["2024-07-09", "final_intent.fallback", 18], ["2024-07-09", "step.1", 18]]
    )
    want_days = {"2024-07-09": {"chat_rows": 86, "final_intent": {"fallback": 18}, "step": {"1": 18}}}
    want_total = {"chat_rows": 86, "final_intent": {"fallback": 18}, "step": {"1": 18}, "event_dates": 1}
    lines: list[str] = []
    assert lr.reconcile(got, want_days, want_total, out=lines.append) == (7, 0)  # 3 day keys + 4 total keys
    assert lines[-1].startswith("[trino-check] 7/7 values equal")


def test_trino_query_follows_next_uri_and_raises_on_error(monkeypatch):
    import io
    import json as js

    import lake_reconcile as lr

    pages = iter(
        [
            {"nextUri": "http://t/2", "data": [["2024-07-09", "chat_rows", 86]]},
            {"data": [["2024-07-25", "chat_rows", 7552]]},
        ]
    )

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(lr.urllib.request, "urlopen", lambda *a, **k: Resp(js.dumps(next(pages)).encode()))
    assert lr.trino_query("SELECT 1;") == [["2024-07-09", "chat_rows", 86], ["2024-07-25", "chat_rows", 7552]]

    pages = iter([{"error": {"errorName": "PERMISSION_DENIED", "message": "Access Denied"}}])
    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        lr.trino_query("ANALYZE x")
