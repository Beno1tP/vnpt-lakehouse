"""Task 5 step 5.8a: unpack a Superset dashboard export (ZIP from `superset export-dashboards`) into
infra/k8s/superset/assets/ and check it, so the dashboard lives in Git as reviewable YAML.

    python -I tools/superset_assets.py unpack <export.zip> infra/k8s/superset/assets   # make superset-export
    python -I tools/superset_assets.py check infra/k8s/superset/assets                 # + unit test on the commit

(Not a top-level folder named `superset/`: it made ruff treat `superset` as first-party and could shadow the real
package for scripts run from the repo root: E84.)

unpack: refuses unsafe paths (absolute, ..), strips the export's top folder (dashboard_export_<timestamp>/),
replaces metadata.yaml + databases/ datasets/ charts/ dashboards/ (README.md and other files stay), then runs check.
check fails unless: no secret in any file (database URIs masked or without a password, no password/secret keys with
values); every dataset UUID = the fixed uuid5 of its table (superset_datasets); the chart names = the charts in
dashboard_checks.yaml; exactly one dashboard and one database.
"""

from __future__ import annotations

import importlib.util
import re
import shutil
import sys
import zipfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SUPERSET = ROOT / "infra/k8s/superset"
_spec = importlib.util.spec_from_file_location("superset_datasets", SUPERSET / "superset_datasets.py")
sd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sd)

EXPORT_PARTS = ("metadata.yaml", "databases", "datasets", "charts", "dashboards")
SECRET_KEYS = re.compile(
    r"^\s*(password|secret|secret_key|encrypted_extra|server_cert|access_token)\s*:\s*(.+)$", re.I | re.M
)
URI_WITH_PASSWORD = re.compile(r"://[^:/@\s]+:([^@\s]+)@")


def unpack(zip_path: Path, dest: Path) -> list[str]:
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if not n.endswith("/")]
        for n in names:
            if n.startswith("/") or ".." in Path(n).parts:
                raise ValueError(f"unsafe path in export: {n}")
        tops = {n.split("/", 1)[0] for n in names}
        if len(tops) != 1 or not all("/" in n for n in names):
            raise ValueError(f"expected one top folder in the export, got {sorted(tops)}")
        for part in EXPORT_PARTS:  # replace only what an export writes (a deleted chart disappears, README stays)
            old = dest / part
            if old.is_dir():
                shutil.rmtree(old)
            elif old.exists():
                old.unlink()
        for n in names:
            target = dest / n.split("/", 1)[1]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(z.read(n))
    return sorted(n.split("/", 1)[1] for n in names)


def check(dest: Path) -> list[str]:
    errors: list[str] = []
    files = sorted(p for p in dest.rglob("*.yaml"))
    if not files:
        return [f"no YAML files in {dest}"]
    for p in files:
        text = p.read_text()
        for m in SECRET_KEYS.finditer(text):
            value = m.group(2).strip().strip("'\"")
            if value not in ("", "null", "{}", "XXXXXXXXXX"):
                errors.append(f"{p.relative_to(dest)}: key '{m.group(1)}' has a value")
        for m in URI_WITH_PASSWORD.finditer(text):
            if m.group(1) != "XXXXXXXXXX":
                errors.append(f"{p.relative_to(dest)}: URI with a clear-text password")
    docs = {p.relative_to(dest).as_posix(): yaml.safe_load(p.read_text()) for p in files}
    kinds = {
        k: [v for f, v in docs.items() if f.startswith(k + "/")]
        for k in ("databases", "datasets", "charts", "dashboards")
    }
    if len(kinds["databases"]) != 1 or len(kinds["dashboards"]) != 1:
        errors.append(f"expected 1 database + 1 dashboard, got {len(kinds['databases'])} + {len(kinds['dashboards'])}")
    for ds in kinds["datasets"]:
        if str(ds.get("uuid")) != str(sd.dataset_uuid(ds["table_name"])):
            errors.append(
                f"dataset {ds['table_name']}: uuid {ds.get('uuid')} != fixed uuid5 (created outside the Job?)"
            )
    checks = yaml.safe_load((SUPERSET / "dashboard_checks.yaml").read_text())
    names = {c["slice_name"] for c in kinds["charts"]}
    if names != set(checks["charts"]):
        errors.append(
            f"chart names differ from dashboard_checks.yaml: only in export {sorted(names - set(checks['charts']))}, "
            f"only in checks {sorted(set(checks['charts']) - names)}"
        )
    if kinds["dashboards"] and kinds["dashboards"][0].get("dashboard_title") != checks["dashboard"]:
        errors.append(f"dashboard title {kinds['dashboards'][0].get('dashboard_title')!r} != {checks['dashboard']!r}")
    return errors


def main(argv: list[str]) -> int:
    if len(argv) == 3 and argv[0] == "unpack":
        files = unpack(Path(argv[1]), Path(argv[2]))
        print(f"unpacked {len(files)} files into {argv[2]}")
        dest = Path(argv[2])
    elif len(argv) == 2 and argv[0] == "check":
        dest = Path(argv[1])
    else:
        print(__doc__)
        return 2
    errors = check(dest)
    for e in errors:
        print("FAIL:", e)
    counts = {k: len(list((dest / k).rglob("*.yaml"))) for k in ("databases", "datasets", "charts", "dashboards")}
    print(f"superset-assets: {counts} -> {'PASS' if not errors else f'FAIL ({len(errors)})'}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
