import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DIR = ROOT / "infra/k8s/superset"


def load_config(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    spec = importlib.util.spec_from_file_location("superset_config", DIR / "superset_config.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ENV = dict(
    SUPERSET_SECRET_KEY="k", DB_USER="superset", DB_PASS="p@ss/:x", DB_HOST="h", DB_PORT="5432", DB_NAME="superset"
)


def test_config_reads_env_and_quotes_password(monkeypatch):
    cfg = load_config(monkeypatch, **ENV)
    assert cfg.SECRET_KEY == "k"
    assert cfg.SQLALCHEMY_DATABASE_URI == "postgresql+psycopg2://superset:p%40ss%2F%3Ax@h:5432/superset"


def test_config_refuses_missing_secret_key(monkeypatch):
    monkeypatch.delenv("SUPERSET_SECRET_KEY", raising=False)
    with pytest.raises(KeyError):
        load_config(monkeypatch, **{k: v for k, v in ENV.items() if k != "SUPERSET_SECRET_KEY"})


def test_config_has_no_redis_or_celery():
    text = (DIR / "superset_config.py").read_text()
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    code = code.split('"""', 2)[2]  # skip the docstring
    assert "redis" not in code.lower() and "celery" not in code.lower()


def test_digest_equals_images_env():
    env = dict(
        line.split("=", 1)
        for line in (ROOT / "infra/k8s/images.env").read_text().splitlines()
        if line and not line.startswith("#")
    )
    repo, digest = env["SUPERSET_IMAGE_REF"].split("@")
    kust = yaml.safe_load((DIR / "kustomization.yaml").read_text())
    assert {i["name"]: i["digest"] for i in kust["images"]} == {repo: digest}


def test_web_pod_rules():
    docs = list(yaml.safe_load_all((DIR / "superset.yaml").read_text()))
    deps = [d for d in docs if d["kind"] == "Deployment"]
    assert len(deps) == 1 and {d["kind"] for d in docs} == {"Deployment", "Service"}  # no worker, no Job
    pod = deps[0]["spec"]["template"]["spec"]
    assert pod["securityContext"]["runAsNonRoot"] is True and pod["securityContext"]["runAsUser"] != 0
    assert pod["automountServiceAccountToken"] is False
    assert pod["imagePullSecrets"] == [{"name": "ghcr-pull"}]
    for c in pod["initContainers"] + pod["containers"]:
        assert c["image"] == "ghcr.io/beno1tp/vnpt-lake-superset"  # digest only via kustomize images:
        assert {"secretRef": {"name": "superset-env"}} in c["envFrom"]  # never the init credentials
        assert "limits" in c["resources"] and "memory" in c["resources"]["limits"]
    assert "superset-init-credentials" not in (DIR / "superset.yaml").read_text()
