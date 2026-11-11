# syntax=docker/dockerfile:1
# Task 4 step 4.2: thin dbt image (D17): python slim + pinned dbt-core/dbt-starrocks + the dbt project + the init
# script. Built by CI (images.yml), stored in GHCR, deployed by digest (D5). Context = dbt/; extra build contexts:
# req = docker/dbt (requirements), scripts = scripts.
FROM python:3.11-slim@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/opt/dbt DBT_SEND_ANONYMOUS_USAGE_STATS=False \
    DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs
COPY --from=req requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && useradd --uid 1000 --create-home dbt
COPY --chown=1000:1000 . /opt/dbt
COPY --from=scripts --chown=1000:1000 starrocks_init.py /opt/scripts/starrocks_init.py
USER 1000
WORKDIR /opt/dbt
CMD ["dbt", "--version"]
