import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DIR = ROOT / "infra/k8s/superset"
spec_ = importlib.util.spec_from_file_location("drc", ROOT / "tools/drill_render_check.py")
drc = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(drc)


def containers(doc):
    pod = doc["spec"]["template"]["spec"]
    return [c["name"] for c in pod.get("initContainers", []) + pod["containers"]]


def test_overlay_patches_every_container_of_the_base():  # a new container in the base must get DB_NAME too
    base = {
        d["kind"]: d
        for f in ("superset.yaml", "superset-init-job.yaml")
        for d in yaml.safe_load_all((DIR / f).read_text())
    }
    kust = yaml.safe_load((ROOT / "infra/k8s/superset-drill/kustomization.yaml").read_text())
    assert kust["nameSuffix"] == "-drill" and kust["resources"] == ["../superset"]  # sibling of the base (E86)
    for p in kust["patches"]:
        patch = yaml.safe_load(p["patch"])
        patched = containers(patch)
        assert sorted(patched) == sorted(containers(base[p["target"]["kind"]])), p["target"]
        pod = patch["spec"]["template"]["spec"]
        for c in pod.get("initContainers", []) + pod["containers"]:
            assert {"name": "DB_NAME", "value": "superset_drill"} in c["env"]


def drill_doc(db="superset_drill", name="superset-drill", app="superset-drill"):
    return {"kind": "Deployment", "metadata": {"name": name},
            "spec": {"selector": {"matchLabels": {"app": app}}, "template": {"spec": {"containers": [
                {"name": "superset", "env": [{"name": "DB_NAME", "value": db}]}]}}}}  # fmt: skip


def test_render_check_catches_real_db_name_or_selector():
    assert drc.problems([drill_doc(), {"kind": "ConfigMap", "metadata": {"name": "superset-config-drill-abc"}}]) == []
    assert drc.problems([drill_doc(db=None)])  # container still on the real DB
    assert drc.problems([drill_doc(name="superset")])
    assert drc.problems([drill_doc(app="superset")])  # the real Service would select it
    assert drc.problems([])


def test_check_pod_is_parameterised_and_job_url_from_env():
    pod = (DIR / "superset-check-pod.yaml").read_text()
    assert "__NAME__" in pod and "__URL__" in pod and "__IMAGE__" in pod
    job = (DIR / "superset-init-job.yaml").read_text()
    assert 'os.environ["SUPERSET_URL"] + "/health"' in job and "metadata db: $DB_NAME" in job
