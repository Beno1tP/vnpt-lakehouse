"""Task 4 (D17): runner of the idempotent StarRocks setup, as the Job starrocks-init (make starrocks-init).

The statements live in infra/k8s/starrocks/init.sql, mounted from the ConfigMap starrocks-init-sql at
STARROCKS_INIT_SQL (default /config/init.sql): changing a grant needs no image build. This runner
1. reads the file, splits it into statements, fills ${NAME} placeholders from the environment (Secret
   starrocks-init-credentials; values must be alphanumeric so they cannot break out of a SQL string),
2. runs them as root, printing each first line with every secret replaced by ***,
3. checks: dbt_rw refreshes + reads lake_iceberg.silver.user_message (= silver.json), bi_ro gets a privilege error
   on CREATE in gold and on INSERT into Iceberg.
Exit 1 on any failed statement or check. Counts only.
"""

from __future__ import annotations

import os
import re
import sys

EXPECTED_USER_MESSAGE = 10613  # tests/expected/silver.json totals.user_message_rows
PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")
SECRET_VARS = ("STARROCKS_DBT_PASSWORD", "STARROCKS_BI_PASSWORD", "S3_SECRET_KEY")


def split_statements(text: str) -> list[str]:
    """Statements end with ';' at the end of a line; '--' comment lines are dropped."""
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in re.split(r";[ \t]*(?:\n|$)", body) if s.strip()]


def render(sql: str, env: dict) -> str:
    def value(m: re.Match) -> str:
        name = m.group(1)
        if name not in env:
            raise KeyError(f"placeholder ${{{name}}} has no value in the environment")
        if not env[name].isalnum():  # generated as hex; anything else could break out of the SQL string
            raise ValueError(f"{name} must be alphanumeric")
        return env[name]

    return PLACEHOLDER.sub(value, sql)


def masked(sql: str, secrets: list[str]) -> str:
    for s in secrets:
        if s:
            sql = sql.replace(s, "***")
    return sql


def connect(host: str, user: str, password: str):
    import mysql.connector  # in the dbt image (dependency of dbt-starrocks)

    return mysql.connector.connect(host=host, port=9030, user=user, password=password, autocommit=True)


def main() -> None:
    env = dict(os.environ)
    host = env.get("STARROCKS_HOST", "starrocks-fe-service.starrocks.svc.cluster.local")
    secrets = [env.get(k, "") for k in SECRET_VARS]
    with open(env.get("STARROCKS_INIT_SQL", "/config/init.sql")) as f:
        statements = [render(s, env) for s in split_statements(f.read())]
    failed = 0
    # E71: keep every connection in a variable; a cursor alone (weak reference) loses its connection at once
    root_conn = connect(host, "root", env["STARROCKS_ROOT_PASSWORD"])
    cur = root_conn.cursor()
    for sql in statements:
        try:
            cur.execute(sql)
            print(f"[starrocks-init] OK    {masked(sql, secrets).splitlines()[0]}", flush=True)
        except Exception as e:  # noqa: BLE001 - report every statement, then fail
            failed += 1
            print(f"[starrocks-init] FAIL  {masked(sql, secrets).splitlines()[0]}: {masked(str(e), secrets)}")
    root_conn.close()

    try:
        dbt_conn = connect(host, "dbt_rw", env["STARROCKS_DBT_PASSWORD"])
        dbt = dbt_conn.cursor()
        dbt.execute("REFRESH EXTERNAL TABLE lake_iceberg.silver.user_message")
        dbt.execute("SELECT count(*) FROM lake_iceberg.silver.user_message")
        n = dbt.fetchone()[0]
        dbt_conn.close()
    except Exception as e:  # noqa: BLE001 - a check that cannot run is a FAIL line, not a traceback
        n = f"error {masked(str(e), secrets)[:200]}"
    ok = n == EXPECTED_USER_MESSAGE
    failed += not ok
    print(
        f"[starrocks-init] {'PASS' if ok else 'FAIL'}  dbt_rw reads lake_iceberg.silver.user_message: {n} rows "
        f"(expected {EXPECTED_USER_MESSAGE})"
    )

    bi_conn = connect(host, "bi_ro", env["STARROCKS_BI_PASSWORD"])
    bi = bi_conn.cursor()
    for sql in (
        "CREATE TABLE gold.bi_ro_should_fail (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1",
        "INSERT INTO lake_iceberg.silver.user_message SELECT * FROM lake_iceberg.silver.user_message LIMIT 0",
    ):
        verb = sql.split(" ")[0]
        try:
            bi.execute(sql)
            failed += 1
            print(f"[starrocks-init] FAIL  bi_ro was allowed: {verb}")
        except Exception as e:  # noqa: BLE001 - only a privilege error counts as the expected denial
            denied = "access denied" in str(e).lower()
            failed += not denied
            why = "privilege error" if denied else f"other error, not a denial: {masked(str(e), secrets)[:200]}"
            print(f"[starrocks-init] {'PASS' if denied else 'FAIL'}  bi_ro denied: {verb} ({why})")
    bi_conn.close()
    print(f"[starrocks-init] {'done' if not failed else f'{failed} failures'}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
