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
