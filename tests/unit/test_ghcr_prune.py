"""Step 2.7: GHCR prune never deletes a deployed digest and keeps the newest N."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("ghcr_prune", ROOT / ".github/scripts/ghcr_prune.py")
gp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gp)


def _v(n, day):
    return {"id": str(n), "name": f"sha256:{n:064x}", "created_at": f"2026-10-{day:02d}T00:00:00Z", "tags": ""}


def test_keeps_deployed_and_newest():
    versions = [_v(1, 20), _v(2, 21), _v(3, 22), _v(4, 23), _v(5, 24)]
    keep, delete = gp.plan(versions, {_v(1, 20)["name"]}, keep_n=2)
    assert {v["id"] for v in keep} == {"5", "4", "1"}  # 2 newest + the deployed old one
    assert {v["id"] for v in delete} == {"3", "2"}


def test_current_images_env_digests_are_found():
    digests = gp.deployed_digests((ROOT / "infra/k8s/images.env").read_text())
    assert len(digests) == 6  # + vnpt-lake-dbt (Task 4), vnpt-lake-superset (Task 5)
