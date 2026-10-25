import json
from datetime import date, timedelta
from pathlib import Path

import pytest

import validate_bronze as vb

ROOT = Path(__file__).resolve().parents[2]
DAILY = json.loads((ROOT / "tests/expected/bronze_daily.json").read_text())
FULL = json.loads((ROOT / "tests/expected/bronze.json").read_text())


def test_load_checks_names_and_sql():
    checks = vb.load_checks(str(ROOT / "tests/sql/bronze_checks.sql"))
    assert len(checks) == 15
    assert all(name.split(".", 1)[0] in ("chatlog", "bot_predict", "both") for name in checks)
    assert all(sql and not sql.rstrip().endswith(";") for sql in checks.values())
    assert all("${ingest_date}" in sql for sql in checks.values())


def test_every_check_has_a_full_expected_value():
    for name in vb.load_checks(str(ROOT / "tests/sql/bronze_checks.sql")):
        table, key = name.split(".", 1)
        assert key in FULL[table], name


def test_daily_totals_match_task1_and_full_file():
    days = DAILY["days"].values()
    assert sum(d["chatlog_rows"] for d in days) == DAILY["totals"]["chatlog_rows"] == FULL["chatlog"]["rows"]
    predict = DAILY["totals"]["bot_predict_rows"]
    assert sum(d["bot_predict_rows"] for d in days) == predict == FULL["bot_predict"]["rows"]
    assert sum(d["unmatched_to_chat"] for d in days) == FULL["bot_predict"]["unmatched_to_chat"]
    assert DAILY["columns"] == {t: FULL[t]["columns"] for t in ("chatlog", "bot_predict")}


def test_daily_range_is_data_days_plus_empty_days():
    start, end = (date.fromisoformat(DAILY["range"][k]) for k in ("from", "to"))
    every_day = {(start + timedelta(n)).isoformat() for n in range((end - start).days + 1)}
    assert set(DAILY["days"]) | set(DAILY["empty_days"]) == every_day
    assert not set(DAILY["days"]) & set(DAILY["empty_days"])


def test_expected_for_day_shape():
    e = vb.expected_for_day(DAILY, "2024-07-25")
    assert e["chatlog"]["rows"] == e["chatlog"]["distinct_id"] == 7552
    assert e["bot_predict"]["unmatched_to_chat"] == 116
    assert e["both"]["distinct_batch_ids"] == 1


def test_expected_for_empty_day_exits():
    with pytest.raises(SystemExit):
        vb.expected_for_day(DAILY, "2024-07-07")


def test_rules_file_parses_and_targets_one_partition():
    rules = vb.load_checks(str(ROOT / "tests/sql/bronze_rules.sql"), marker="rule")
    assert len(rules) == 13
    assert {n.split(".", 1)[0] for n in rules} == {"chatlog", "bot_predict", "both"}
    assert all("${ingest_date}" in sql for sql in rules.values())
    assert not vb.load_checks(str(ROOT / "tests/sql/bronze_rules.sql"))        # no '-- check:' blocks mixed in
