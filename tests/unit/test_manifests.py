"""SparkApplication templates stay valid after `make sparkapp` fills the placeholders."""
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
FILL = {"__NAME__": "bronze-2024-07-02", "__DATE__": "2024-07-02", "__IMAGE__": "img:tag",
        "__EXTRA_ARGS__": ', "--check-idempotency"'}


@pytest.mark.parametrize("app", ["bronze", "validate"])
def test_sparkapp_template(app):
    text = (ROOT / f"infra/k8s/spark-jobs/{app}-sparkapp.yaml").read_text()
    for key, value in FILL.items():
        text = text.replace(key, value)
    assert "__" not in text.replace("__init__", "")
    doc = yaml.safe_load(text)
    spec = doc["spec"]
    assert doc["kind"] == "SparkApplication" and doc["metadata"]["namespace"] == "spark-jobs"
    assert spec["restartPolicy"]["type"] == "Never"                    # one retry layer: Airflow
    assert spec["driver"]["serviceAccount"] == "spark-operator-spark"
    for role in ("driver", "executor"):
        assert spec[role]["envFrom"] == [{"secretRef": {"name": "s3-credentials"}}]
    assert spec["sparkConf"]["spark.sql.catalog.lake.uri"].endswith(".lake.svc.cluster.local:9083")


def test_workflow_files_parse():
    for wf in (ROOT / ".github/workflows").glob("*.yml"):
        assert yaml.safe_load(wf.read_text())["jobs"], wf.name
