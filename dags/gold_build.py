"""Task 4 step 4.5: Gold in StarRocks built by dbt, orchestrated by Airflow (plan A6, D16).

Scheduled on the asset LAKE_SILVER (validate_silver passed). StarRocks and a Spark run do not fit the node together,
so the mode decides (D15): this DAG never scales an engine. It waits (reschedule mode, holds no worker slot) until
StarRocks is up = gold mode (`make mode-gold`); after 30 min it FAILS with a clear message instead of skipping, so a
forgotten mode switch is visible and the asset event is not silently consumed (rerun: clear the run after mode-gold).

dbt_build = `dbt build` in a one-off pod (namespace gold-jobs, user dbt_rw, image by digest from images.env): seeds,
models and their tests in graph order, so a failed test skips everything downstream (incl. the lab doc's
dbt_test_gold checks: reconcile_uc4..uc8, assert_bitmap_users) and the Silver gate assert_silver_counts runs first.
on-run-start refreshes the Iceberg metadata cache (E72). Retries: 1 for the pod (infrastructure); a second failure
is a data problem, see the test output in the task log. Emits LAKE_GOLD on success. Counts only in the logs.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag, task
from kubernetes.client import models as k8s
from lake_assets import LAKE_GOLD, LAKE_SILVER

from lake_alerts import task_failure_alert

STABLE_SECONDS = 120  # E76: FE and BE Ready for 2 min before dbt runs
DBT_RESOURCES = k8s.V1ResourceRequirements(  # request = limit: shares the gold-mode margin (02_config.md §4)
    requests={"memory": "512Mi", "cpu": "200m"}, limits={"memory": "512Mi", "cpu": "1"}
)


@dag(
    schedule=[LAKE_SILVER],
    start_date=datetime(2024, 7, 2, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": task_failure_alert,
    },
    tags=["task4", "gold", "dbt"],
)
def gold_build():
    @task.sensor(poke_interval=60, timeout=30 * 60, mode="reschedule", execution_timeout=timedelta(minutes=35))
    def starrocks_ready() -> bool:
        import lake_reconcile as lr

        fe, be = lr.starrocks_ready()
        stable = lr.starrocks_ready_seconds() if fe and be else 0.0
        ok = bool(fe and be) and stable >= STABLE_SECONDS  # E76: not the first Ready (BE blacklist window)
        print(
            f"[gold-build] StarRocks FE ready {fe}, BE ready {be}, both ready for {stable:.0f} s"
            + ("" if ok else f": waiting (make mode-gold; need {STABLE_SECONDS} s stable)")
        )
        return ok

    dbt_build = KubernetesPodOperator(
        task_id="dbt_build",
        namespace="gold-jobs",
        name="dbt-build",
        image="{{ var.value.dbt_image }}",
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference(name="ghcr-pull")],
        service_account_name="dbt",
        cmds=["dbt"],
        arguments=["build"],
        env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="starrocks-dbt-credentials"))],
        container_resources=DBT_RESOURCES,
        get_logs=True,
        startup_timeout_seconds=600,
        on_finish_action="delete_succeeded_pod",  # a failed pod stays for debugging
        retries=1,
        retry_delay=timedelta(minutes=2),  # E76 margin; a second failure = data, not infrastructure
        outlets=[LAKE_GOLD],
    )

    @task
    def verdict():  # leaf (E67): the run fails when dbt_build failed
        print("[gold-build] dbt build passed (models + tests); asset lake_gold updated")

    starrocks_ready() >> dbt_build >> verdict()


gold_build()
