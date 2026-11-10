"""Task 3 step 3.1: the Silver SQL builders (no Spark needed). The numbers themselves are checked on Minikube against
tests/expected/silver_daily.json (pandas, tools/expected_silver.py)."""

import pytest
import silver_transform as st


@pytest.mark.parametrize("bad", ["2024-7-24", "2024-02-30", "2024-07-24' OR '1'='1", "", "24-JUL-24"])
def test_check_day_rejects_bad_dates(bad):
    with pytest.raises(ValueError):
        st.check_day(bad)


def test_batch_check_reads_both_tables_of_the_day():
    sql = st.batch_check_sql("2024-07-24")
    assert "count(DISTINCT _batch_id)" in sql
    assert sql.count("_ingest_date = DATE '2024-07-24'") == 2
    assert "lake.bronze.chatlog" in sql and "lake.bronze.bot_predict" in sql


def test_chat_message_parses_without_zone_and_dedups():
    sql = st.chat_message_sql("2024-07-24")
    assert "to_timestamp_ntz(CREATED_TIME, 'dd-MMM-yy hh.mm.ss.SSSSSSSSS a')" in sql  # D8: no session-zone shift
    assert "to_timestamp(" not in sql
    assert "row_number() OVER (PARTITION BY ID ORDER BY _ingested_at DESC)" in sql and "_rn = 1" in sql
    assert "SENDER_ID LIKE '%@%'" in sql  # internal sender = e-mail-like ID, same rule as expected_silver.py
    assert "_ingest_date = DATE '2024-07-24'" in sql


def test_final_intent_rule_order_and_row_threshold():
    sql = st.nlu_prediction_sql("2024-07-24")
    fallback = sql.index("WHEN p.INTENT_NAME = 'fallback' THEN 'fallback'")
    below = sql.index("< CAST(p.NLU_THRESHOLD AS DOUBLE) THEN 'bot_khong_hieu'")  # D9: the row's threshold
    default = sql.index("ELSE coalesce(nullif(p.UPDATED_INTENT, ''), p.INTENT_NAME)")
    assert fallback < below < default
    # has_chat_row over ALL Bronze days, as a join: Spark rejects IN-subqueries in the SELECT list
    assert "LEFT JOIN (SELECT DISTINCT ID AS chat_ref FROM lake.bronze.chatlog) c ON c.chat_ref = p.ID_CHATLOG" in sql
    assert " IN (SELECT" not in sql
    assert "p.STEP AS step" in sql  # bot scenario, as delivered (Task 4 4.0a, UC4)


def test_add_columns_is_idempotent_and_only_for_known_tables():
    old = ["id", "chat_id", "final_intent", "event_date"]
    want = ["ALTER TABLE lake.silver.nlu_prediction ADD COLUMN step string"]
    assert st.add_columns_sql("nlu_prediction", old) == want
    assert st.add_columns_sql("nlu_prediction", [*old, "STEP"]) == []  # already added: nothing to do (case-insensitive)
    assert st.add_columns_sql("chat_message", old) == []  # no new column there
    with pytest.raises(ValueError):
        st.add_columns_sql("x; DROP TABLE y", old)


def test_user_message_is_a_left_join_of_client_messages():
    sql = st.USER_MESSAGE_SQL
    assert "LEFT JOIN nlu_prediction p ON p.chat_id = c.id" in sql
    assert "WHERE c.is_client" in sql
    assert "has_prediction" in sql
    assert "p.step" in sql


def test_summary_has_the_keys_of_the_expected_file():
    import json
    from pathlib import Path

    day = json.loads((Path(__file__).resolve().parents[1] / "expected/silver_daily.json").read_text())["days"]
    keys = set(next(iter(day.values())))
    sql_keys = {line.split(" AS ")[-1].strip(",") for line in st.SUMMARY_SQL.splitlines() if " AS " in line}
    assert sql_keys | {"final_intent", "step"} == keys  # printed summary is comparable line by line


def test_silver_rules_file_parses_and_expects_zero_violations():
    from pathlib import Path

    from validate_bronze import load_checks

    rules = load_checks(str(Path(__file__).resolve().parents[1] / "sql/silver_rules.sql"), marker="rule")
    assert len(rules) == 14
    assert {"nlu_prediction.step_not_null", "user_message.step_iff_prediction"} <= set(rules)
    assert {"chat_message.event_ts_not_null", "nlu_prediction.final_intent_follows_rule"} <= set(rules)
    assert all("${ingest_date}" in sql or "count(DISTINCT id)" in sql for sql in rules.values())


def test_validator_counts_match_the_expected_keys():
    import json
    from pathlib import Path

    import validate_silver as vs

    day = json.loads((Path(__file__).resolve().parents[1] / "expected/silver_daily.json").read_text())["days"]
    keys = set(next(iter(day.values()))) - {"final_intent", "step"}
    sql_keys = {ln.strip().split(" AS ")[-1].strip(",") for ln in vs.COUNTS_SQL.splitlines() if " AS " in ln}
    sql_keys |= {ln.strip().strip(",").split()[-1] for ln in vs.COUNTS_SQL.splitlines() if ln.strip().startswith("AS ")}
    assert keys <= sql_keys
