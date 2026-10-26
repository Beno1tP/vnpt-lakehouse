"""Lab-doc pytest topics for the Bronze job: pandas -> Spark types (everything text, empty -> None),
S3 reads (s3:// landing path through pyarrow), table_exists (create vs. idempotent overwrite)."""

import io

import pandas as pd

import bronze_ingest as bi


def _xlsx_bytes(df: pd.DataFrame, sheet: str = "Export Worksheet") -> bytes:
    buf = io.BytesIO()
    df.to_excel(buf, sheet_name=sheet, index=False, engine="openpyxl")
    return buf.getvalue()


SAMPLE = pd.DataFrame({"ID": [1, 2], "CREATED_TIME": ["02-JUL-24 03.40.09.895455000 PM", None], "SCORE": [0.9, None]})


def test_read_excel_keeps_everything_as_text_and_empty_as_none(tmp_path):
    path = tmp_path / "x.xlsx"
    path.write_bytes(_xlsx_bytes(SAMPLE))
    pdf = bi.read_excel_as_strings(str(path), "Export Worksheet")
    assert pdf["ID"].tolist() == ["1", "2"]  # numbers stay text in Bronze
    assert pdf["SCORE"].tolist()[0] == "0.9" and pdf["SCORE"].tolist()[1] is None
    assert pdf["CREATED_TIME"].tolist()[1] is None  # NaN -> None = Spark null


def test_read_excel_from_s3_landing(monkeypatch):
    data = _xlsx_bytes(SAMPLE)
    opened = []

    class FakeFile(io.BytesIO):
        def __enter__(self):
            return self

    class FakeS3:
        def __init__(self, **kwargs):
            assert kwargs["endpoint_override"] == "minio.lake.svc.cluster.local:9000" and kwargs["scheme"] == "http"

        def open_input_file(self, path):
            opened.append(path)
            return FakeFile(data)

    import pyarrow.fs as pafs

    monkeypatch.setattr(pafs, "S3FileSystem", FakeS3)
    monkeypatch.setenv("S3_ENDPOINT", "minio.lake.svc.cluster.local:9000")
    pdf = bi.read_excel_as_strings("s3://landing/chatlog/dt=2024-07-24/bot_dvc_hcm.xlsx", "Export Worksheet")
    assert opened == ["landing/chatlog/dt=2024-07-24/bot_dvc_hcm.xlsx"]  # bucket/key without the scheme
    assert len(pdf) == 2


class _Writer:
    def __init__(self, log):
        self.log = log

    def overwritePartitions(self):
        self.log.append("overwritePartitions")

    def using(self, fmt):
        self.log.append(f"using {fmt}")
        return self

    def partitionedBy(self, col):
        self.log.append(f"partitionedBy {col}")
        return self

    def tableProperty(self, k, v):
        self.log.append(f"{k}={v}")
        return self

    def create(self):
        self.log.append("create")


class _Spark:
    def __init__(self, exists):
        self.catalog = type("C", (), {"tableExists": staticmethod(lambda name: exists)})()


class _DF:
    def __init__(self, log):
        self.log = log

    def writeTo(self, name):
        self.log.append(f"writeTo {name}")
        return _Writer(self.log)


def test_table_exists_means_idempotent_partition_overwrite():
    log = []
    bi.write_bronze(_Spark(exists=True), "bronze.chatlog", _DF(log))
    assert log == ["writeTo lake.bronze.chatlog", "overwritePartitions"]


def test_first_load_creates_iceberg_v2_partitioned_by_ingest_date():
    log = []
    bi.write_bronze(_Spark(exists=False), "bronze.chatlog", _DF(log))
    assert log == [
        "writeTo lake.bronze.chatlog",
        "using iceberg",
        "partitionedBy col(_ingest_date)",
        "format-version=2",
        "create",
    ]
