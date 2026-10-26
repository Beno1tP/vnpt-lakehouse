from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

import maintenance as mt

ROOT = Path(__file__).resolve().parents[2]


def test_timestamp_literal_is_explicit_utc():
    now = datetime(2026, 10, 26, 9, 0, 0, tzinfo=timezone.utc)
    assert mt.ts_literal(72, now) == "TIMESTAMP '2026-10-23 09:00:00 UTC'"


def test_only_bronze_tables():
    with pytest.raises(ValueError):
        mt.check_table("bronze.chatlog; DROP TABLE x")


def test_template_renders_for_make_and_keeps_safe_defaults():
    text = (ROOT / "dags/templates/maintenance_sparkapp.yaml").read_text()
    fill = {"{{ ds }}": "manual", "{{ var.value.spark_job_image }}": "img", "{{ params.snapshot_hours }}": "168",
            "{{ params.retain_last }}": "5", "{{ params.orphan_hours }}": "72", "{{ params.dry_run }}": "false"}
    for k, v in fill.items():
        text = text.replace(k, v)
    assert "{{" not in text
    doc = yaml.safe_load(text)
    assert doc["spec"]["restartPolicy"]["type"] == "Never"
    assert doc["spec"]["mainApplicationFile"].endswith("/maintenance.py")
