"""Task 2 step 2.4: daily Bronze load of the chatbot logs, orchestrated on Kubernetes.

check_landing -> bronze (SparkApplication) -> validate (SparkApplication) -> notify
- One run = one day = `ds` (logical date). Schedule = CronDataIntervalTimetable at midnight UTC, so scheduled and
  backfill runs cover [D 00:00, D+1 00:00) with logical date D. Airflow 3 turns a bare timedelta into a
  DeltaTriggerTimetable (zero-length intervals, logical date = run time): E38. Manual runs: ds = --logical-date.
- A day without landing files is SKIPPED (decision D2): 6 of the 24 days 2024-07-02..25 have no data.
- One retry layer (A4): Airflow retries the load (transient failures); SparkApplications use restartPolicy Never;
  validation retries once, quickly: a wrong number is deterministic and fails again, but an infrastructure
  interruption (e.g. scheduler pod replaced mid-task, drill 3 / E41) must not fail the day.
- Safe to rerun/backfill only because bronze overwrites its own partition (Task 1 idempotency).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sdk import AsyncCallback, CronDataIntervalTimetable, DeadlineAlert, DeadlineReference, dag, task
from airflow.sdk.exceptions import AirflowSkipException  # Airflow 3 location (core alias is deprecated)

from lake_alerts import deadline_alert, task_failure_alert

LANDING = {"chatlog": "bot_dvc_hcm.xlsx", "bot_predict": "bot_ddvc_hcm_bot_predict.xlsx"}


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),   # real [D, D+1) intervals (E38)
    start_date=datetime(2024, 7, 2, tzinfo=timezone.utc),
    catchup=False,                                    # history only through an explicit backfill
    max_active_runs=1,                                # all runs write the same two tables
    dagrun_timeout=timedelta(hours=3),               # hard stop, after the 2 h deadline alert below
    # step 2.6: alert (Telegram) when a run is not finished 2 h after it was queued (replaces Airflow 2 SLAs)
    deadline=DeadlineAlert(reference=DeadlineReference.DAGRUN_QUEUED_AT, interval=timedelta(hours=2),
                           callback=AsyncCallback(deadline_alert)),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "execution_timeout": timedelta(minutes=20),
                  "on_failure_callback": task_failure_alert},   # one message after the last try fails
    tags=["task2", "lake", "bronze"],
)
def chatbot_bronze():
    @task(retries=0)
    def check_landing(ds=None, data_interval_start=None, data_interval_end=None):
        print(f"[landing] ds={ds} data_interval=[{data_interval_start}, {data_interval_end})")
        s3 = S3Hook(aws_conn_id="minio_landing")      # MinIO user airflow-landing: read landing only (2.1b)
        found = {t: s3.check_for_key(f"{t}/dt={ds}/{f}", bucket_name="landing") for t, f in LANDING.items()}
        print(f"[landing] {ds}: " + ", ".join(f"{t}={'yes' if ok else 'no'}" for t, ok in found.items()))
        if not any(found.values()):
            raise AirflowSkipException(f"no export for {ds}: run skipped (D2)")
        if not all(found.values()):
            raise ValueError(f"incomplete export for {ds}: {found}")   # one table without the other = bad export

    common = {"namespace": "spark-jobs", "kubernetes_conn_id": "kubernetes_in_cluster", "get_logs": True,
              "delete_on_termination": True}
    bronze = SparkKubernetesOperator(task_id="bronze", application_file="templates/bronze_sparkapp.yaml", **common)
    validate = SparkKubernetesOperator(task_id="validate", application_file="templates/validate_sparkapp.yaml",
                                       retries=1, retry_delay=timedelta(minutes=1), **common)

    @task(retries=0)
    def notify(ds=None):
        print(f"[notify] {ds}: bronze loaded and validated")  # success stays quiet; failures/deadlines alert

    check_landing() >> bronze >> validate >> notify()


chatbot_bronze()
