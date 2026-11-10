"""Task 4 step 4.2 (D17): idempotent StarRocks setup, run as the Job starrocks-init (make starrocks-init).

1. external catalog lake_iceberg -> Iceberg tables via HMS + MinIO (MinIO user starrocks: read lakehouse/ only)
2. database gold
3. users dbt_rw (all on gold, SELECT on the catalog) and bi_ro (SELECT on gold only); passwords reset every run
4. checks: dbt_rw reads lake_iceberg.silver.user_message (= silver.json), bi_ro cannot CREATE/INSERT

Credentials come from the environment (Secret starrocks-init-credentials); statements are printed with every secret
replaced by ***, results as counts only. Exit 1 on any failed statement or check.
"""

from __future__ import annotations

import os
import sys

HMS = "thrift://hive-metastore.lake.svc.cluster.local:9083"
S3 = "http://minio.lake.svc.cluster.local:9000"
EXPECTED_USER_MESSAGE = 10613  # tests/expected/silver.json totals.user_message_rows


def catalog_sql(access_key: str, secret_key: str) -> str:
    props = {
        "type": "iceberg",
        "iceberg.catalog.type": "hive",
        "hive.metastore.uris": HMS,
        "aws.s3.endpoint": S3,
        "aws.s3.enable_path_style_access": "true",
        "aws.s3.enable_ssl": "false",
        "aws.s3.region": "us-east-1",
        "aws.s3.access_key": access_key,
        "aws.s3.secret_key": secret_key,
    }
    body = ",\n  ".join(f'"{k}" = "{v}"' for k, v in props.items())
    return f"CREATE EXTERNAL CATALOG IF NOT EXISTS lake_iceberg PROPERTIES (\n  {body}\n)"


def setup_statements(env: dict) -> list[str]:
    dbt, bi = env["STARROCKS_DBT_PASSWORD"], env["STARROCKS_BI_PASSWORD"]
    for name, value in (("STARROCKS_DBT_PASSWORD", dbt), ("STARROCKS_BI_PASSWORD", bi)):
        if not value.isalnum():  # generated as hex; anything else could break out of the SQL string
            raise ValueError(f"{name} must be alphanumeric")
    return [
        catalog_sql(env["S3_ACCESS_KEY"], env["S3_SECRET_KEY"]),
        "CREATE DATABASE IF NOT EXISTS gold",
        f"CREATE USER IF NOT EXISTS 'dbt_rw'@'%' IDENTIFIED BY '{dbt}'",
        f"ALTER USER 'dbt_rw'@'%' IDENTIFIED BY '{dbt}'",
        f"CREATE USER IF NOT EXISTS 'bi_ro'@'%' IDENTIFIED BY '{bi}'",
        f"ALTER USER 'bi_ro'@'%' IDENTIFIED BY '{bi}'",
        "GRANT ALL ON DATABASE gold TO USER 'dbt_rw'@'%'",
        "GRANT ALL ON ALL TABLES IN DATABASE gold TO USER 'dbt_rw'@'%'",
        "GRANT ALL ON ALL VIEWS IN DATABASE gold TO USER 'dbt_rw'@'%'",
        "GRANT USAGE ON CATALOG lake_iceberg TO USER 'dbt_rw'@'%'",
        "SET CATALOG lake_iceberg",
        "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'dbt_rw'@'%'",
        "SET CATALOG default_catalog",
        "GRANT SELECT ON ALL TABLES IN DATABASE gold TO USER 'bi_ro'@'%'",
        "GRANT SELECT ON ALL VIEWS IN DATABASE gold TO USER 'bi_ro'@'%'",
    ]


def masked(sql: str, secrets: list[str]) -> str:
    for s in secrets:
        if s:
            sql = sql.replace(s, "***")
    return sql


def connect(host: str, user: str, password: str):
    import mysql.connector  # in the dbt image (dependency of dbt-starrocks)

    return mysql.connector.connect(host=host, port=9030, user=user, password=password, autocommit=True)


def main() -> None:
    env = os.environ
    host = env.get("STARROCKS_HOST", "starrocks-fe-service.starrocks.svc.cluster.local")
    secrets = [env.get(k, "") for k in ("STARROCKS_DBT_PASSWORD", "STARROCKS_BI_PASSWORD", "S3_SECRET_KEY")]
    failed = 0
    root = connect(host, "root", env["STARROCKS_ROOT_PASSWORD"])
    cur = root.cursor()
    for sql in setup_statements(env):
        try:
            cur.execute(sql)
            print(f"[starrocks-init] OK    {masked(sql, secrets).splitlines()[0]}", flush=True)
        except Exception as e:  # noqa: BLE001 - report every statement, then fail
            failed += 1
            print(f"[starrocks-init] FAIL  {masked(sql, secrets).splitlines()[0]}: {masked(str(e), secrets)}")
    root.close()

    dbt = connect(host, "dbt_rw", env["STARROCKS_DBT_PASSWORD"]).cursor()
    dbt.execute("REFRESH EXTERNAL TABLE lake_iceberg.silver.user_message")
    dbt.execute("SELECT count(*) FROM lake_iceberg.silver.user_message")
    n = dbt.fetchone()[0]
    ok = n == EXPECTED_USER_MESSAGE
    failed += not ok
    print(
        f"[starrocks-init] {'PASS' if ok else 'FAIL'}  dbt_rw reads lake_iceberg.silver.user_message: {n} rows "
        f"(expected {EXPECTED_USER_MESSAGE})"
    )

    bi = connect(host, "bi_ro", env["STARROCKS_BI_PASSWORD"]).cursor()
    for sql in (
        "CREATE TABLE gold.bi_ro_should_fail (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1",
        "INSERT INTO lake_iceberg.silver.user_message SELECT * FROM lake_iceberg.silver.user_message LIMIT 0",
    ):
        try:
            bi.execute(sql)
            failed += 1
            print(f"[starrocks-init] FAIL  bi_ro was allowed: {sql.split('(')[0].strip()}")
        except Exception as e:  # noqa: BLE001 - a denial is the expected outcome
            print(f"[starrocks-init] PASS  bi_ro denied: {sql.split(' ')[0]} … ({type(e).__name__})")
    print(f"[starrocks-init] {'done' if not failed else f'{failed} failures'}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
