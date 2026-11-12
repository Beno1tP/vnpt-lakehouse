import base64
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("src", ROOT / "tools/superset_render_check.py")
src = importlib.util.module_from_spec(spec)
spec.loader.exec_module(src)

REF = "ghcr.io/beno1tp/vnpt-lake-superset@sha256:" + "a" * 64
TAGGED = "ghcr.io/beno1tp/vnpt-lake-superset:4662349@sha256:" + "a" * 64
CONFIG = "import os\ndef env(k, d=None): return os.getenv(k, d)\nSQLALCHEMY_DATABASE_URI = f\"x{env('DB_PASS')}\"\n"


def good():
    return [
        {
            "kind": "Deployment",
            "metadata": {"name": "superset"},
            "spec": {
                "template": {
                    "spec": {
                        "securityContext": {"runAsUser": 1000},
                        "initContainers": [{"name": "wait-for-postgres", "image": TAGGED}],
                        "containers": [
                            {"name": "superset", "image": TAGGED, "envFrom": [{"secretRef": {"name": "superset-env"}}]}
                        ],
                    }
                }
            },
        },
        {
            "kind": "Secret",
            "metadata": {"name": "superset-config"},
            "data": {"superset_config.py": base64.b64encode(CONFIG.encode()).decode()},
        },
        {"kind": "Service", "metadata": {"name": "superset"}},
    ]


def test_strip_tag():
    assert src.strip_tag(TAGGED) == REF
    assert src.strip_tag(REF) == REF
    assert src.strip_tag("localhost:5000/x:1@sha256:b") == "localhost:5000/x@sha256:b"  # registry port kept


def test_good_render_passes():
    fails, info = src.check(good(), REF)
    assert fails == [], fails
    assert any("SQLALCHEMY_DATABASE_URI" in line for line in info)


def test_empty_input_fails():
    assert src.check([], REF)[0]


def test_each_violation_is_caught():
    docs = good()
    docs[0]["spec"]["template"]["spec"]["initContainers"][0]["image"] = "apache/superset:dockerize"
    assert any("wait-for-postgres" in f for f in src.check(docs, REF)[0])
    docs = good()
    docs[0]["spec"]["template"]["spec"]["securityContext"]["runAsUser"] = 0
    assert any("runAsUser 0" in f for f in src.check(docs, REF)[0])
    docs = good()
    docs[0]["spec"]["template"]["spec"]["containers"][0]["envFrom"] = []
    assert any("superset-env" in f for f in src.check(docs, REF)[0])
    docs = good() + [{"kind": "Secret", "metadata": {"name": "superset-env"}, "stringData": {"DB_PASS": "x"}}]
    fails = src.check(docs, REF)[0]
    assert any("secretEnv.create" in f for f in fails) and any("DB_PASS" in f for f in fails)
    docs = good() + [{"kind": "Job", "metadata": {"name": "superset-init-db"}, "spec": {}}]
    assert any("Job/superset-init-db" in f for f in src.check(docs, REF)[0])
    docs = good() + [{"kind": "StatefulSet", "metadata": {"name": "superset-redis-master"}, "spec": {}}]
    assert len(src.check(docs, REF)[0]) >= 2  # kind and name
    docs = good()
    docs[1]["data"]["superset_config.py"] = base64.b64encode((CONFIG + "REDIS_BASE_URL = 'x'\n").encode()).decode()
    assert any("Redis" in f for f in src.check(docs, REF)[0])
