import bronze_ingest as bi


def test_source_path_landing_zone():
    assert (bi.source_path("s3://landing/", "bronze.chatlog", "bot_dvc_hcm.xlsx", "2024-07-24")
            == "s3://landing/chatlog/dt=2024-07-24/bot_dvc_hcm.xlsx")


def test_source_path_task1_folder_ignores_date():
    assert (bi.source_path("/data/raw", "bronze.bot_predict", "bot_ddvc_hcm_bot_predict.xlsx", "2026-10-16")
            == "/data/raw/bot_ddvc_hcm_bot_predict.xlsx")


def test_sources_cover_both_tables():
    assert set(bi.SOURCES) == {"bronze.chatlog", "bronze.bot_predict"}
