#!/bin/bash
# Runs once, only when the data directory is empty (official postgres image, /docker-entrypoint-initdb.d).
# Creates the airflow role + database next to the metastore DB. Password read from the mounted secret file.
set -euo pipefail
# separate assignment: set -e stops here if the file is unreadable (inside an argument the error was ignored: E27)
pw="$(cat /run/secrets/pg/AIRFLOW_DB_PASSWORD)"
[ -n "$pw" ] || { echo "ERROR: AIRFLOW_DB_PASSWORD is empty" >&2; exit 1; }
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname metastore -v pw="$pw" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'pw';
CREATE DATABASE airflow OWNER airflow;
SQL
echo "airflow role and database created"
