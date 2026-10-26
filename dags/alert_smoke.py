"""Step 2.6 smoke DAG: proves both alert paths without touching the data DAG. Manual trigger only.
- deadline: 30 s after the run is queued (the task sleeps 60 s) -> one "⏰" message from the triggerer
- failure: the task then fails on purpose, retries=0 -> one "❌" message from on_failure_callback
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from airflow.sdk import AsyncCallback, DeadlineAlert, DeadlineReference, dag, task

from lake_alerts import deadline_alert, task_failure_alert


@dag(
    schedule=None,
    start_date=datetime(2024, 7, 2, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["task2", "smoke"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=30),
        callback=AsyncCallback(deadline_alert),
    ),
)
def alert_smoke():
    @task(retries=0, on_failure_callback=task_failure_alert, execution_timeout=timedelta(minutes=5))
    def fail_on_purpose():
        time.sleep(60)
        raise RuntimeError("alert smoke test: intentional failure")

    fail_on_purpose()


alert_smoke()
