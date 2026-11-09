"""SparkApplication templates stay valid after `make sparkapp` fills the placeholders."""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
FILL = {"{{ ds }}": "2024-07-02", "{{ var.value.spark_job_image }}": "img@sha256:x"}  # what make/Airflow render


@pytest.mark.parametrize("app", ["bronze", "validate", "silver", "validate_silver"])
def test_sparkapp_template(app):
    text = (ROOT / f"dags/templates/{app}_sparkapp.yaml").read_text()
    for key, value in FILL.items():
        text = text.replace(key, value)
    assert "{{" not in text and "__" not in text  # only the two documented expressions
    doc = yaml.safe_load(text)
    spec = doc["spec"]
    assert doc["kind"] == "SparkApplication" and doc["metadata"]["namespace"] == "spark-jobs"
    name = app.replace("_", "-")  # K8s names allow no underscores; make sparkapp uses the same mapping
    assert doc["metadata"]["name"] == f"{name}-2024-07-02" and spec["imagePullSecrets"] == ["ghcr-pull"]
    assert spec["restartPolicy"]["type"] == "Never"  # one retry layer: Airflow
    assert spec["driver"]["serviceAccount"] == "spark-operator-spark"
    for role in ("driver", "executor"):
        assert spec[role]["envFrom"] == [{"secretRef": {"name": "s3-credentials"}}]
    assert spec["sparkConf"]["spark.sql.catalog.lake.uri"].endswith(".lake.svc.cluster.local:9083")


def test_workflow_files_parse():
    for wf in (ROOT / ".github/workflows").glob("*.yml"):
        assert yaml.safe_load(wf.read_text())["jobs"], wf.name


def _images_env():
    pairs = (
        line.split("=", 1)
        for line in (ROOT / "infra/k8s/images.env").read_text().splitlines()
        if line and not line.startswith("#")
    )
    return dict(pairs)


def test_kustomize_digests_equal_images_env():
    env = _images_env()
    kust = yaml.safe_load((ROOT / "infra/k8s/kustomization.yaml").read_text())
    pinned = {i["name"]: i["digest"] for i in kust["images"]}
    for key in ("SPARK_IMAGE_REF", "HMS_IMAGE_REF"):
        repo, digest = env[key].split("@")
        assert pinned[repo] == digest, key
    assert all(ref.startswith("ghcr.io/beno1tp/") and "@sha256:" in ref for ref in env.values())


def test_no_local_only_images_left():
    for path in (p for p in (ROOT / "infra/k8s").rglob("*.yaml") if p.name != "kustomization.yaml"):
        text = path.read_text()
        assert "imagePullPolicy: Never" not in text, path.name
        if "ghcr.io/beno1tp" in text or "__IMAGE__" in text:
            assert "ghcr-pull" in text, f"{path.name}: GHCR image without imagePullSecrets"


def test_validate_template_runs_rules_and_make_can_add_idempotency():
    text = (ROOT / "dags/templates/validate_sparkapp.yaml").read_text()
    assert '"/opt/tests/sql/bronze_rules.sql"]' in text  # the exact string the Makefile IDEMPOTENCY_SED edits
    assert "IDEMPOTENCY_SED" in (ROOT / "Makefile").read_text()


def test_airflow_may_only_manage_spark_jobs():
    docs = list(yaml.safe_load_all((ROOT / "infra/k8s/spark-jobs/rbac-airflow.yaml").read_text()))
    role = next(d for d in docs if d["kind"] == "Role")
    binding = next(d for d in docs if d["kind"] == "RoleBinding")
    assert role["metadata"]["namespace"] == "spark-jobs"  # a Role, not a ClusterRole
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": "airflow-scheduler", "namespace": "airflow"}]
    for rule in role["rules"]:
        assert "*" not in rule["verbs"] and "*" not in rule["resources"]
        assert "secrets" not in rule["resources"]


def test_backup_cronjob_dumps_both_databases_with_server_major_version():
    cj = yaml.safe_load((ROOT / "infra/k8s/lake/pg-backup-cronjob.yaml").read_text())
    pod = cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    dump = pod["initContainers"][0]
    server = yaml.safe_load((ROOT / "infra/k8s/lake/postgres.yaml").read_text().split("---")[1])
    assert dump["image"] == server["spec"]["template"]["spec"]["containers"][0]["image"]  # pg_dump == server major
    assert "for db in metastore airflow" in dump["args"][0]
    assert cj["spec"]["concurrencyPolicy"] == "Forbid"


def test_spark_operator_image_pinned_by_digest():
    values = yaml.safe_load((ROOT / "infra/helm/spark-operator/values-local.yaml").read_text())
    assert values["image"]["tag"].startswith("2.2.1@sha256:")  # chart 2.2.1 = app 2.2.1, Spark 3.5


@pytest.mark.parametrize(
    "path",
    [
        "dags/templates/bronze_sparkapp.yaml",
        "dags/templates/validate_sparkapp.yaml",
        "dags/templates/silver_sparkapp.yaml",
        "dags/templates/validate_silver_sparkapp.yaml",
        "dags/templates/maintenance_sparkapp.yaml",
        "infra/k8s/spark-jobs/catalog-sparkapp.yaml",
    ],
)
def test_sparkapp_writes_event_log(path):
    """Every Spark run is browsable later in the History Server (make spark-history): driver writes to the PVC."""
    text = (ROOT / path).read_text().replace("{{", "").replace("}}", "")
    spec = yaml.safe_load(text)["spec"]
    assert spec["sparkConf"]["spark.eventLog.enabled"] == "true"
    assert spec["sparkConf"]["spark.eventLog.dir"] == "file:///spark-events"
    assert {"name": "spark-events", "persistentVolumeClaim": {"claimName": "spark-events"}} in spec["volumes"]
    assert {"name": "spark-events", "mountPath": "/spark-events"} in spec["driver"]["volumeMounts"]
