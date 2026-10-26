import asyncio
from types import SimpleNamespace

import lake_alerts as la


def test_failure_text_is_metadata_only():
    ti = SimpleNamespace(dag_id="chatbot_bronze", task_id="bronze", try_number=3, max_tries=2)
    text = la.failure_text({"ti": ti, "ds": "2024-07-24", "run_id": "backfill__2024-07-24T00:00:00+00:00",
                            "exception": RuntimeError("row 42: Nguyen Van A said hi")})
    assert "chatbot_bronze.bronze FAILED for day 2024-07-24 (attempt 3/3, final)" in text
    assert "Nguyen" not in text and "row 42" not in text           # exception text never leaves the cluster


def test_deadline_text_accepts_dicts():
    text = la.deadline_text({"dag_run": {"dag_id": "chatbot_bronze", "logical_date": "2024-07-24",
                                         "dag_run_id": "r1", "run_type": "backfill"},
                             "deadline": {"deadline_time": "2026-10-26T05:00:00Z"}})
    assert text.startswith("⏰ chatbot_bronze run for 2024-07-24") and "2026-10-26T05:00:00Z" in text
    assert text.endswith("run: r1")                                   # 3.3.2 key is dag_run_id (E44)


def test_texts_for_a_manual_run_without_logical_date():
    d = la.deadline_text({"dag_run": {"dag_id": "alert_smoke", "logical_date": None, "dag_run_id": "manual__x",
                                      "run_type": "manual"}})
    assert "none (manual run without a logical date)" in d and d.endswith("run: manual__x")
    f = la.failure_text({"ti": None, "run_id": "manual__x"})
    assert "for day none (run without a logical date)" in f


def test_send_without_credentials_is_a_no_op(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    assert la.send_telegram("x") is False


def test_send_swallows_network_errors(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "c")

    def boom(*a, **k):
        raise OSError("no network")
    monkeypatch.setattr(la.urllib.request, "urlopen", boom)
    assert la.send_telegram("x") is False                           # never raises into the task


def test_deadline_alert_is_async_and_uses_context(monkeypatch):
    sent = []
    monkeypatch.setattr(la, "send_telegram", lambda text: sent.append(text) or True)
    asyncio.run(la.deadline_alert(context={"dag_run": {"dag_id": "d", "logical_date": "x", "dag_run_id": "r"}}))
    assert sent and sent[0].startswith("⏰ d run for x")
