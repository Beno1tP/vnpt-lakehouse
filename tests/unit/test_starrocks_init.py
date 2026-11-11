"""Task 4: infra/k8s/starrocks/init.sql + the runner scripts/starrocks_init.py (no StarRocks needed)."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import starrocks_init as si  # noqa: E402

ENV = {
    "STARROCKS_DBT_PASSWORD": "abc123",
    "STARROCKS_BI_PASSWORD": "def456",
    "S3_ACCESS_KEY": "starrocks",
    "S3_SECRET_KEY": "s3cr3t",
}
STATEMENTS = si.split_statements((ROOT / "infra/k8s/starrocks/init.sql").read_text())


def test_init_sql_splits_into_the_setup_statements():
    assert len(STATEMENTS) == 16  # catalog, database, 4 user, 3 gold + 1 usage, 2 SET CATALOG, 2 catalog, 2 bi_ro
    assert STATEMENTS[0].startswith("CREATE EXTERNAL CATALOG IF NOT EXISTS lake_iceberg")
    assert (
        '"iceberg.catalog.type" = "hive"' in STATEMENTS[0]
        and '"aws.s3.enable_path_style_access" = "true"' in STATEMENTS[0]
    )
    assert all(not s.endswith(";") and "--" not in s for s in STATEMENTS)


def test_setup_is_idempotent_and_least_privilege():
    assert all("IF NOT EXISTS" in s for s in STATEMENTS if s.startswith("CREATE"))  # rerun = no error
    bi = [s for s in STATEMENTS if "'bi_ro'" in s and s.startswith("GRANT")]
    assert bi and all(s.startswith("GRANT SELECT") and "DATABASE gold" in s for s in bi)  # bi_ro: read gold only
    catalog = STATEMENTS[
        STATEMENTS.index("SET CATALOG lake_iceberg") + 1 : STATEMENTS.index("SET CATALOG default_catalog")
    ]
    assert catalog and all(s.split()[1] in ("SELECT", "REFRESH") for s in catalog)  # dbt_rw: no write on Iceberg


def test_placeholders_are_filled_validated_and_masked():
    rendered = [si.render(s, ENV) for s in STATEMENTS]
    assert not any("${" in s for s in rendered) and '"aws.s3.secret_key" = "s3cr3t"' in rendered[0]
    with pytest.raises(ValueError):
        si.render(
            "IDENTIFIED BY '${STARROCKS_DBT_PASSWORD}'", {**ENV, "STARROCKS_DBT_PASSWORD": "x'; DROP USER root; --"}
        )
    with pytest.raises(KeyError):
        si.render("${NOT_SET}", ENV)
    assert si.masked("ALTER USER 'dbt_rw'@'%' IDENTIFIED BY 'abc123'", ["abc123", ""]).endswith("'***'")
