"""Task 3 step 3.6: Trino reconciliation orchestrated by Airflow (make trino-check, automated).

Scheduled on the asset LAKE_SILVER: runs after chatbot_bronze.validate_silver succeeded. Trino (2 Gi) and a Spark run
do not fit the node together (D12), and a check in the middle of a multi-day backfill compares a half-rewritten lake
(E67), so: wait until no Spark application is unfinished AND no chatbot_bronze run is running or queued (asset events
that arrive meanwhile are merged into the next run) -> scale Trino 0 -> 1 ->
reconcile every Silver number per day in Trino (sql/trino/reconcile.sql) against the pandas expectations -> ALWAYS
scale Trino back to 0 (also after a failure). A mismatch fails the reconcile task -> Telegram alert, and the leaf
`verdict` makes the RUN fail too (E67: with the all_done clean-up as the only leaf the run showed success).
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
    @task.short_circuit
    def not_gold_mode() -> bool:  # Task 4 4.5: the mode decides, DAGs never scale the other engine
        import lake_reconcile as lr

        fe, be = lr.starrocks_ready()
        if fe:
            print(
                f"[trino-reconcile] skipped: gold mode (StarRocks FE {fe}, BE {be} ready); Silver is reconciled by "
                "gold_build through StarRocks (dbt test assert_silver_counts). Rerun in pipeline mode if needed"
            )
        return fe == 0

    @task.sensor(poke_interval=60, timeout=3 * 3600, mode="reschedule", execution_timeout=timedelta(hours=3))
    def wait_pipeline_idle(ti=None) -> bool:
        import lake_reconcile as lr

        apps = lr.unfinished_spark_apps()
        runs = ti.get_dr_count(dag_id=lr.PIPELINE_DAG, states=["running", "queued"])  # a backfill queues its runs
        print(f"[trino-reconcile] unfinished SparkApplications: {apps}; {lr.PIPELINE_DAG} runs running/queued: {runs}")
        return apps == 0 and runs == 0

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

    @task
    def verdict():  # leaf: upstream_failed when reconcile or trino_down failed -> the run fails (E67)
        print("[trino-reconcile] reconcile passed and Trino is back to 0")

    checked = reconcile()
    down = trino_down()
    not_gold_mode() >> wait_pipeline_idle() >> trino_up() >> checked >> down
    [checked, down] >> verdict()


trino_reconcile()
