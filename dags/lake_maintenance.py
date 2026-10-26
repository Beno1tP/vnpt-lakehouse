"""Task 2 step 2.5c (gap #10): weekly Iceberg maintenance of the Bronze tables (jobs/maintenance.py).

Sunday 02:00 UTC, after the daily load (00:00) has finished; the orphan-file age threshold (72 h) protects files of a
write that would still be running. Retention: snapshots older than 7 days, but always the last 5 (= time travel window).
Params can be overridden for a manual run (e.g. dry_run=true).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sdk import AsyncCallback, CronDataIntervalTimetable, DeadlineAlert, DeadlineReference, dag

from lake_alerts import deadline_alert, task_failure_alert


@dag(
    schedule=CronDataIntervalTimetable("0 2 * * 0", timezone="UTC"),
    start_date=datetime(2024, 7, 2, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    deadline=DeadlineAlert(reference=DeadlineReference.DAGRUN_QUEUED_AT, interval=timedelta(hours=2),
                           callback=AsyncCallback(deadline_alert)),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(minutes=45),
                  "on_failure_callback": task_failure_alert},
    params={"snapshot_hours": 168, "retain_last": 5, "orphan_hours": 72, "dry_run": "false"},
    tags=["task2", "lake", "maintenance"],
)
def lake_maintenance():
    SparkKubernetesOperator(task_id="iceberg_maintenance", application_file="templates/maintenance_sparkapp.yaml",
                            namespace="spark-jobs", kubernetes_conn_id="kubernetes_in_cluster", get_logs=True,
                            delete_on_termination=True)


lake_maintenance()
