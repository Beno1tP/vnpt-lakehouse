"""Step 2.7: delete old GHCR versions of our 4 images (dry run unless --confirm).

Keeps every digest in infra/k8s/images.env (deployed) and the newest --keep versions per package (rollback).
Images are pushed with provenance: false (single manifests), so no version is a child of another one.
Needs `gh auth refresh -h github.com -s read:packages,delete:packages`; runs `gh api` as the logged-in user.

    python3 .github/scripts/ghcr_prune.py [--keep 3] [--confirm]
"""

from __future__ import annotations

import argparse
import re
import subprocess
import urllib.request
from pathlib import Path

PACKAGES = ["vnpt-lake-spark", "vnpt-lake-hms", "vnpt-lake-spark-job", "vnpt-lake-airflow", "vnpt-lake-dbt"]
ROOT = Path(__file__).resolve().parents[2]


def deployed_digests(images_env: str) -> set[str]:
    return set(re.findall(r"@(sha256:[0-9a-f]{64})", images_env))


def plan(versions: list[dict], keep_digests: set[str], keep_n: int) -> tuple[list[dict], list[dict]]:
    """(keep, delete): versions sorted newest first; keep deployed digests and the newest keep_n."""
    newest = sorted(versions, key=lambda v: v["created_at"], reverse=True)
    keep = [v for i, v in enumerate(newest) if i < keep_n or v["name"] in keep_digests]
    return keep, [v for v in newest if v not in keep]


def gh_versions(pkg: str) -> list[dict]:
    jq = '.[] | [.id, .name, .created_at, ((.metadata.container.tags // []) | join(","))] | @tsv'
    out = subprocess.run(
        ["gh", "api", "--paginate", f"/user/packages/container/{pkg}/versions", "--jq", jq],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    rows = [line.split("\t") for line in out.splitlines() if line]
    return [{"id": r[0], "name": r[1], "created_at": r[2], "tags": r[3] if len(r) > 3 else ""} for r in rows]


def delete_version(pkg: str, version_id: str) -> None:
    """DELETE with an empty body via urllib: the old gh call was rejected with
    422 "For 'links/0/schema', {} is not a null" (E50). Token from `gh auth token` (gh >= 2.17), never printed."""
    token = subprocess.run(["gh", "auth", "token"], check=True, capture_output=True, text=True).stdout.strip()
    req = urllib.request.Request(
        f"https://api.github.com/user/packages/container/{pkg}/versions/{version_id}",
        method="DELETE",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        assert resp.status == 204, resp.status


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", type=int, default=3, help="newest versions kept per package (rollback)")
    ap.add_argument("--confirm", action="store_true", help="really delete (default: dry run)")
    args = ap.parse_args()
    keep_digests = deployed_digests((ROOT / "infra/k8s/images.env").read_text())
    total = 0
    for pkg in PACKAGES:
        keep, delete = plan(gh_versions(pkg), keep_digests, args.keep)
        print(f"[ghcr] {pkg}: {len(keep)} kept, {len(delete)} to delete")
        for v in keep:
            why = "deployed" if v["name"] in keep_digests else "newest"
            print(f"  keep   {v['name'][:19]}… {v['created_at']} tags={v['tags'] or '-'} ({why})")
        for v in delete:
            print(f"  delete {v['name'][:19]}… {v['created_at']} tags={v['tags'] or '-'}")
            if args.confirm:
                delete_version(pkg, v["id"])
        total += len(delete)
    print(f"[ghcr] {'deleted' if args.confirm else 'would delete'} {total} versions")


if __name__ == "__main__":
    main()
