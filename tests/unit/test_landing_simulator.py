import pandas as pd

import landing_simulator as ls


def test_event_day_oracle_text_any_case():
    s = pd.Series(["02-JUL-24 03.40.09.895455000 PM", "25-Jul-24 11.01.00.000000000 AM"])
    assert ls.event_day(s).tolist() == ["2024-07-02", "2024-07-25"]


def test_event_day_unparseable_is_nan():
    assert ls.event_day(pd.Series(["not a time"])).isna().all()


def test_sources_match_bronze_job_files():
    import bronze_ingest as bi
    assert {f for f, _ in ls.SOURCES.values()} == {f for f, _ in bi.SOURCES.values()}
