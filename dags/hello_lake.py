"""Step 2.3 smoke DAG: proves the DAG folder (ConfigMap), the dag-processor, the scheduler (LocalExecutor) and the
task logs work, and shows the run's data interval (Airflow 3 gotcha, 01_knowledge.md A3). No Spark here."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


@dag(
    schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 2, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["task2", "smoke"],
)
def hello_lake():
    @task
    def show_interval(data_interval_start=None, data_interval_end=None, ds=None):
        print(f"[hello] ds={ds} interval=[{data_interval_start}, {data_interval_end})")

    show_interval()


hello_lake()
