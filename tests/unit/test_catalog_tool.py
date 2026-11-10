import pytest

import catalog_tool as ct

GOOD = "s3a://lakehouse/warehouse/bronze.db/chatlog/metadata/00012-d3c54239-1463-40d5-b3fd-6cdac238915c.metadata.json"


def test_register_sql_for_a_valid_metadata_file():
    assert ct.register_sql("bronze.chatlog", GOOD) == (
        f"CALL lake.system.register_table(table => 'bronze.chatlog', metadata_file => '{GOOD}')"
    )


@pytest.mark.parametrize(
    "table, metadata",
    [
        ("bronze.other", GOOD),  # unknown table
        ("bronze.bot_predict", GOOD),  # metadata of another table
        ("bronze.chatlog", GOOD.replace(".metadata.json", ".json")),  # not a metadata file
        ("bronze.chatlog", GOOD + "'); DROP TABLE x; --"),  # SQL injection attempt
        ("bronze.chatlog", "s3a://landing/chatlog/x.metadata.json"),  # outside the warehouse
    ],
)
def test_register_sql_rejects_bad_input(table, metadata):
    with pytest.raises(ValueError):
        ct.register_sql(table, metadata)


def test_tag_sql_keeps_an_existing_tag():
    assert ct.tag_sql("bronze.chatlog", "task2_v1", 365) == (
        "ALTER TABLE lake.bronze.chatlog CREATE TAG IF NOT EXISTS task2_v1 RETAIN 365 DAYS"
    )


@pytest.mark.parametrize(
    "table, tag, days",
    [
        ("bronze.chatlog", "task1_v1", 365),  # Task 1 has no freeze tag
        ("bronze.chatlog", "task2_v1; DROP TABLE x", 365),  # SQL injection attempt
        ("bronze.chatlog", "latest", 365),  # not a task tag
        ("bronze.chatlog", "task2_v1", 0),  # retention must be positive
        ("bronze.other", "task2_v1", 365),  # unknown table
    ],
)
def test_tag_sql_rejects_bad_input(table, tag, days):
    with pytest.raises(ValueError):
        ct.tag_sql(table, tag, days)


def test_expected_rows_from_the_expected_files():
    expected = {
        "bronze_daily.json": {"totals": {"chatlog_rows": 21224, "bot_predict_rows": 10729}},
        "silver.json": {"totals": {"chat_rows": 21224, "predict_rows": 10729, "user_message_rows": 10613}},
    }
    assert ct.expected_rows("bronze.chatlog", expected) == 21224
    assert ct.expected_rows("bronze.bot_predict", expected) == 10729
    assert ct.expected_rows("silver.user_message", expected) == 10613  # Task 3 freeze


def test_silver_tables_can_be_tagged_but_not_registered():
    assert "silver.chat_message" in ct.tag_sql("silver.chat_message", "task3_v1", 365)
    with pytest.raises(ValueError):  # register stays Bronze-only (metadata path pattern)
        ct.register_sql("silver.chat_message", GOOD)


def test_trino_tag_check_uses_the_same_expectations():
    import trino_tag_check as ttc

    assert ttc.EXPECTED_KEY == ct.EXPECTED_KEY  # Spark and Trino verify a freeze against the same numbers
