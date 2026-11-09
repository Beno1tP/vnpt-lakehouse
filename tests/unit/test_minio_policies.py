"""Least privilege of the MinIO service users (step 2.1b, gap #9): what each policy may and may not touch."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def grants(policy: str) -> set[tuple[str, str]]:
    doc = json.loads((ROOT / f"infra/k8s/lake/minio-users/{policy}.json").read_text())
    return {
        (action, res)
        for st in doc["Statement"]
        if st["Effect"] == "Allow"
        for action in st["Action"]
        for res in st["Resource"]
    }


def test_spark_jobs_reads_landing_writes_lakehouse_only():
    g = grants("lake-spark-jobs")
    assert ("s3:GetObject", "arn:aws:s3:::landing/*") in g
    assert ("s3:PutObject", "arn:aws:s3:::lakehouse/*") in g
    assert not any(a in ("s3:PutObject", "s3:DeleteObject") and r.startswith("arn:aws:s3:::landing") for a, r in g)


def test_landing_writer_cannot_touch_lakehouse():
    assert not any("lakehouse" in r for _, r in grants("lake-landing-writer"))


def test_hms_cannot_touch_landing():
    assert not any("landing" in r for _, r in grants("lake-hms"))


def test_no_wildcards_or_admin():
    for policy in sorted(p.stem for p in (ROOT / "infra/k8s/lake/minio-users").glob("*.json")):  # every policy file
        for action, res in grants(policy):
            assert action != "s3:*" and not action.startswith("admin:"), policy
            # the only wildcard allowed: listing bucket NAMES (the console needs it), never objects
            assert res != "arn:aws:s3:::*" or action == "s3:ListAllMyBuckets", policy
            assert "CreateBucket" not in action and "DeleteBucket" not in action, policy


def test_airflow_landing_reader_is_read_only():
    g = grants("lake-landing-reader")
    assert {r for _, r in g} <= {"arn:aws:s3:::landing", "arn:aws:s3:::landing/*"}
    assert not any(a in ("s3:PutObject", "s3:DeleteObject") for a, _ in g)


def test_airflow_logs_user_only_touches_its_bucket():
    assert {r for _, r in grants("lake-airflow-logs")} <= {"arn:aws:s3:::airflow-logs", "arn:aws:s3:::airflow-logs/*"}


def test_backup_writer_cannot_delete_backups():
    g = grants("lake-backup")
    assert {r for _, r in g} <= {"arn:aws:s3:::backup", "arn:aws:s3:::backup/*"}
    assert ("s3:PutObject", "arn:aws:s3:::backup/*") in g
    assert not any(a == "s3:DeleteObject" for a, _ in g)  # a compromised job cannot wipe the backups


def test_demo_viewer_is_read_only_and_cannot_see_backups():
    g = grants("lake-demo-viewer")
    assert {a for a, _ in g} == {"s3:ListAllMyBuckets", "s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject"}
    assert not any("backup" in r for _, r in g)  # pg_dump files stay invisible in demos


def test_trino_reads_lakehouse_only():
    g = grants("lake-trino-read")
    assert {r for _, r in g} == {"arn:aws:s3:::lakehouse", "arn:aws:s3:::lakehouse/*"}
    assert {a for a, _ in g} == {"s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject"}  # D11: no write, no delete
