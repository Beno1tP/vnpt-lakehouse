# syntax=docker/dockerfile:1.27.1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# Task 2 Airflow image (D5): official Airflow 3.3.2 + our DAGs and plugins baked in. Built by CI only (images.yml),
# never on the laptop. Build context = dags/ only, plus the named context "plugins" (step 2.6 alert callbacks). No pip install: the constraint-tested providers in the base image are used
# (cncf.kubernetes 10.22.0, fab 3.9.0, postgres 7.0.2, standard 1.19.0; docs/task2/02_config.md).
ARG BASE_IMAGE=apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1
FROM ${BASE_IMAGE}
COPY --chown=airflow:0 . /opt/airflow/dags/
COPY --from=plugins --chown=airflow:0 . /opt/airflow/plugins/
# Task 3.6 (DAG trino_reconcile): the reconcile SQL + the pandas expectations, read-only resources
COPY --from=expected --chown=airflow:0 silver.json silver_daily.json /opt/airflow/resources/expected/
COPY --from=trino_sql --chown=airflow:0 reconcile.sql /opt/airflow/resources/sql/
