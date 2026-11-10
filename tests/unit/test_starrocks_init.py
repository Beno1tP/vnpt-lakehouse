"""Task 4 step 4.2: the SQL of scripts/starrocks_init.py (no StarRocks needed)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import starrocks_init as si  # noqa: E402

ENV = {
    "STARROCKS_DBT_PASSWORD": "abc123",
    "STARROCKS_BI_PASSWORD": "def456",
    "S3_ACCESS_KEY": "starrocks",
    "S3_SECRET_KEY": "s3cr3t",
}


def test_catalog_reads_iceberg_via_hms_and_minio_path_style():
    sql = si.catalog_sql("starrocks", "s3cr3t")
    assert sql.startswith("CREATE EXTERNAL CATALOG IF NOT EXISTS lake_iceberg")
    assert '"iceberg.catalog.type" = "hive"' in sql and "hive-metastore.lake.svc.cluster.local:9083" in sql
    assert '"aws.s3.enable_path_style_access" = "true"' in sql and '"aws.s3.access_key" = "starrocks"' in sql


def test_setup_is_idempotent_and_least_privilege():
    stmts = si.setup_statements(ENV)
    assert all("IF NOT EXISTS" in s or not s.startswith("CREATE") for s in stmts)  # rerun = no error
    bi = [s for s in stmts if "'bi_ro'" in s and s.startswith("GRANT")]
    assert bi and all(s.startswith("GRANT SELECT") and "DATABASE gold" in s for s in bi)  # bi_ro: read gold only
    assert "GRANT USAGE ON CATALOG lake_iceberg TO USER 'dbt_rw'@'%'" in stmts


def test_passwords_must_be_alphanumeric_and_are_masked():
    with pytest.raises(ValueError):
        si.setup_statements({**ENV, "STARROCKS_DBT_PASSWORD": "x'; DROP USER root; --"})
    line = si.masked("ALTER USER 'dbt_rw'@'%' IDENTIFIED BY 'abc123'", ["abc123", ""])
    assert "abc123" not in line and "***" in line
