"""Task 3 step 3.6: Trino reconciliation orchestrated by Airflow (make trino-check, automated).

Scheduled on the asset LAKE_SILVER: runs after chatbot_bronze.validate_silver succeeded. Trino (2 Gi) and a Spark run
do not fit the node together (D12), so: wait until no Spark application is unfinished -> scale Trino 0 -> 1 ->
reconcile every Silver number per day in Trino (sql/trino/reconcile.sql) against the pandas expectations -> ALWAYS
scale Trino back to 0 (also after a failure). A mismatch fails the reconcile task -> Telegram alert.
If a Spark run starts meanwhile it waits Pending for ~1-2 min until Trino is down again (safe, not an OOM).
Files baked into the image under /opt/airflow/resources/ (docker/airflow/Dockerfile). Counts only.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task
from lake_assets import LAKE_SILVER

from lake_alerts import task_failure_alert

RESOURCES = Path("/opt/airflow/resources")


@dag(
    schedule=[LAKE_SILVER],
    start_date=datetime(2024, 7, 2, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=20),
        "on_failure_callback": task_failure_alert,
    },
    tags=["task3", "trino", "reconcile"],
)
def trino_reconcile():
    @task.sensor(poke_interval=60, timeout=3 * 3600, mode="reschedule", execution_timeout=timedelta(hours=3))
    def wait_no_spark() -> bool:
        import lake_reconcile as lr

        n = lr.unfinished_spark_apps()
        print(f"[trino-reconcile] unfinished SparkApplications: {n}")
        return n == 0

    @task
    def trino_up():
        import lake_reconcile as lr

        lr.scale_trino(1)
        print("[trino-reconcile] trino-coordinator ready")

    @task(retries=0)  # a wrong number is deterministic; infrastructure errors show up in trino_up
    def reconcile():
        import lake_reconcile as lr

        rows = lr.trino_query((RESOURCES / "sql/reconcile.sql").read_text())
        want_days = json.loads((RESOURCES / "expected/silver_daily.json").read_text())["days"]
        want_total = json.loads((RESOURCES / "expected/silver.json").read_text())["totals"]
        _, failed = lr.reconcile(lr.parse(rows), want_days, want_total)
        if failed:
            raise ValueError(f"{failed} Silver numbers differ between Trino and the expectations")

    @task(trigger_rule="all_done")  # always give the 2 Gi back, also after a failure (D12)
    def trino_down():
        import lake_reconcile as lr

        lr.scale_trino(0)
        print("[trino-reconcile] trino-coordinator scaled to 0")

    wait_no_spark() >> trino_up() >> reconcile() >> trino_down()


trino_reconcile()
