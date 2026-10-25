#!/bin/bash
# Runs once, only when the data directory is empty (official postgres image, /docker-entrypoint-initdb.d).
# Creates the airflow role + database next to the metastore DB. Password read from the mounted secret file.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname metastore \
     -v pw="$(cat /run/secrets/pg/AIRFLOW_DB_PASSWORD)" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'pw';
CREATE DATABASE airflow OWNER airflow;
SQL
echo "airflow role and database created"
