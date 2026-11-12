import importlib.util
import zipfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
spec_ = importlib.util.spec_from_file_location("sa", ROOT / "tools/superset_assets.py")
sa = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(sa)
CHECKS = yaml.safe_load((ROOT / "infra/k8s/superset/dashboard_checks.yaml").read_text())
ASSETS = ROOT / "infra/k8s/superset/assets"


def fake_export(tmp_path, uri="starrocks://bi_ro:XXXXXXXXXX@fe:9030/default_catalog.gold", extra=None, uuid=None):
    top = "dashboard_export_20261113T000000"
    files = {
        "metadata.yaml": {"version": "1.0.0", "type": "Dashboard"},
        "databases/StarRocks_Gold.yaml": {"database_name": "StarRocks Gold", "sqlalchemy_uri": uri},
        "datasets/StarRocks_Gold/gold_uc5.yaml": {
            "table_name": "gold_uc5",
            "uuid": uuid or str(sa.sd.dataset_uuid("gold_uc5")),
        },
        "dashboards/VNPT_chatbot.yaml": {"dashboard_title": CHECKS["dashboard"]},
    }
    for i, name in enumerate(CHECKS["charts"]):
        files[f"charts/c{i}.yaml"] = {"slice_name": name}
    files.update(extra or {})
    z = tmp_path / "export.zip"
    with zipfile.ZipFile(z, "w") as zf:
        for path, doc in files.items():
            zf.writestr(f"{top}/{path}", doc if isinstance(doc, str) else yaml.safe_dump(doc, allow_unicode=True))
    return z


def test_unpack_strips_top_folder_and_checks_pass(tmp_path):
    dest = tmp_path / "assets"
    (dest / "charts").mkdir(parents=True)
    (dest / "charts/deleted_chart.yaml").write_text("slice_name: gone")
    (dest / "README.md").write_text("keep me")
    files = sa.unpack(fake_export(tmp_path), dest)
    assert "metadata.yaml" in files and not (dest / "charts/deleted_chart.yaml").exists()  # replaced, not merged
    assert (dest / "README.md").read_text() == "keep me"  # 5.8a: the first export deleted the README
    assert sa.check(dest) == []


def test_check_finds_secrets_uuid_and_chart_drift(tmp_path):
    dest = tmp_path / "a"
    sa.unpack(fake_export(tmp_path, uri="starrocks://bi_ro:s3cret@fe:9030/x"), dest)
    assert any("clear-text password" in e for e in sa.check(dest))
    sa.unpack(fake_export(tmp_path, extra={"databases/x.yaml": "database_name: y\npassword: hunter2\n"}), dest)
    errs = sa.check(dest)
    assert any("'password' has a value" in e for e in errs) and any("expected 1 database" in e for e in errs)
    sa.unpack(fake_export(tmp_path, uuid="00000000-0000-0000-0000-000000000000"), dest)
    assert any("fixed uuid5" in e for e in sa.check(dest))
    sa.unpack(fake_export(tmp_path, extra={"charts/new.yaml": {"slice_name": "unreviewed"}}), dest)
    assert any("only in export ['unreviewed']" in e for e in sa.check(dest))


def test_unpack_refuses_unsafe_paths(tmp_path):
    z = tmp_path / "evil.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("top/../../etc/x.yaml", "a: 1")
    with pytest.raises(ValueError):
        sa.unpack(z, tmp_path / "d")


@pytest.mark.skipif(not list(ASSETS.rglob("*.yaml")), reason="no export committed yet (5.8a: make superset-export)")
def test_committed_assets_pass_the_check():
    assert sa.check(ASSETS) == []
