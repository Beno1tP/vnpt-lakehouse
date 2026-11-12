"""Task 5 step 5.5: create / update the Superset datasets and metrics from datasets.yaml (same folder) (run by the Job
superset-init inside the Superset image; idempotent).

For each dataset: SqlaTable on the "StarRocks Gold" connection with a fixed UUID (uuid5 of the table name, so an
exported dashboard bundle re-imports onto the same datasets), columns synced from StarRocks and compared with the
spec, event_date as the time column, metrics replaced by the spec's list, then every check runs the metric's SQL in
StarRocks and must equal the expected value (Task 4 gold-check numbers). Exit 1 on any difference.

The pure functions (spec validation, identifiers, uuid, comparison) are unit-tested without Superset.
"""

from __future__ import annotations

import os
import re
import sys
import uuid

import yaml

UUID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/Beno1tP/vnpt-lakehouse/superset/dataset")
SQL_WORDS = {"SUM", "COUNT", "DISTINCT", "NULLIF", "BITMAP_UNION_COUNT", "AND", "OR", "NOT", "NULL", "AS", "CASE",
             "WHEN", "THEN", "ELSE", "END", "MAX", "MIN", "AVG", "ROUND", "COALESCE"}  # fmt: skip


def dataset_uuid(table: str) -> uuid.UUID:
    return uuid.uuid5(UUID_NAMESPACE, table)


def database_uuid(name: str) -> uuid.UUID:
    """5.8b: fixed uuid of the connection, so an exported bundle re-imports onto it on a fresh Superset (the importer
    matches databases by uuid; a random one would collide on the database name)."""
    return uuid.uuid5(UUID_NAMESPACE, f"database/{name}")


def identifiers(expression: str) -> set[str]:
    """Column names used by a SQL expression: words that are not SQL keywords/functions, numbers or string literals."""
    code = re.sub(r"'[^']*'", "", expression)
    return {w for w in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", code) if w.upper() not in SQL_WORDS}


def validate(spec: dict) -> list[str]:
    errors = []
    for ds in spec["datasets"]:
        cols, names = set(ds["columns"]), [m["name"] for m in ds["metrics"]]
        if "event_date" not in cols:
            errors.append(f"{ds['table']}: no event_date column (time axis, 01_knowledge.md)")
        if len(names) != len(set(names)):
            errors.append(f"{ds['table']}: duplicate metric names {names}")
        for m in ds["metrics"]:
            unknown = identifiers(m["expression"]) - cols
            if unknown:
                errors.append(f"{ds['table']}.{m['name']}: unknown columns {sorted(unknown)}")
        for c in ds.get("checks", []):
            if c["metric"] not in names:
                errors.append(f"{ds['table']}: check on unknown metric {c['metric']}")
            unknown = identifiers(c.get("where", "")) - cols
            if unknown:
                errors.append(f"{ds['table']}: check where uses unknown columns {sorted(unknown)}")
    return errors


def matches(value, expect, tol: float = 1e-4) -> bool:
    if expect is None or value is None:
        return value is None and expect is None
    if isinstance(expect, int):  # counts: exact
        return float(value) == expect
    return abs(float(value) - float(expect)) <= tol  # ratios/averages: expected values have 6 decimals


def main(path: str) -> int:  # pragma: no cover - needs Superset + StarRocks (runs in the Job)
    with open(path) as f:
        spec = yaml.safe_load(f)
    errors = validate(spec)
    if errors:
        print("\n".join("FAIL: " + e for e in errors))
        return 1
    from sqlalchemy import text
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable, SqlMetric
        from superset.models.core import Database

        database = db.session.query(Database).filter_by(database_name=spec["database"]).one()
        fails = 0
        for ds in spec["datasets"]:
            table = (
                db.session.query(SqlaTable)
                .filter_by(database_id=database.id, schema=spec["schema"], table_name=ds["table"])
                .one_or_none()
            )
            if table is None:
                table = SqlaTable(table_name=ds["table"], schema=spec["schema"], database=database)
                table.uuid = dataset_uuid(ds["table"])
                db.session.add(table)
            table.catalog = spec["catalog"]
            table.description = ds["description"]
            table.fetch_metadata()
            got = {c.column_name for c in table.columns}
            if got != set(ds["columns"]):
                print(f"FAIL: {ds['table']} columns in StarRocks {sorted(got)} != spec {sorted(ds['columns'])}")
                fails += 1
            for c in table.columns:
                if c.column_name == "event_date":
                    c.is_dttm = True
            table.main_dttm_col = "event_date"
            wanted = {m["name"]: m for m in ds["metrics"]}
            table.metrics = [m for m in table.metrics if m.metric_name in wanted]
            existing = {m.metric_name: m for m in table.metrics}
            for name, m in wanted.items():
                metric = existing.get(name) or SqlMetric(metric_name=name)
                metric.verbose_name, metric.expression = m["label"], m["expression"]
                metric.d3format, metric.description = m.get("format"), m.get("description")
                if name not in existing:
                    table.metrics.append(metric)
            db.session.commit()
            print(f"dataset ok: {ds['table']} uuid {table.uuid} | {len(got)} columns | metrics {sorted(wanted)}")
            with database.get_sqla_engine() as engine, engine.connect() as conn:
                for c in ds.get("checks", []):
                    where = f" WHERE {c['where']}" if c.get("where") else ""
                    sql = f"SELECT {wanted[c['metric']]['expression']} FROM {spec['schema']}.{ds['table']}{where}"
                    value = conn.execute(text(sql)).scalar()
                    ok = matches(value, c["expect"])
                    fails += not ok
                    print(f"  {'PASS' if ok else 'FAIL'} {c['metric']}{where}: {value} (expected {c['expect']})")
        fails += setup_viewer(spec)
        print(f"datasets: {'PASS' if not fails else f'FAIL ({fails})'}")
        return 1 if fails else 0


VIEWER_ROLE, CHECKER = "gold_viewer", "checker"


def setup_viewer(spec: dict) -> int:  # pragma: no cover - runs in the Job (app context)
    """5.7: role gold_viewer = datasource_access on the spec's datasets only; user checker = Gamma + gold_viewer with
    the password from SUPERSET_CHECKER_PASSWORD (created or reset, then verified). Returns the number of failures."""
    from superset import db
    from superset.connectors.sqla.models import SqlaTable
    from superset.extensions import security_manager as sm
    from werkzeug.security import check_password_hash

    role = sm.find_role(VIEWER_ROLE) or sm.add_role(VIEWER_ROLE)
    names = [d["table"] for d in spec["datasets"]]
    tables = db.session.query(SqlaTable).filter(SqlaTable.table_name.in_(names), SqlaTable.schema == spec["schema"])
    perms = [sm.add_permission_view_menu("datasource_access", t.perm) for t in tables]
    role.permissions = perms  # exactly these: re-running removes anything added by hand
    pw = os.environ["SUPERSET_CHECKER_PASSWORD"]
    roles = [sm.find_role("Gamma"), role]
    user = sm.find_user(username=CHECKER)
    if user is None:
        user = sm.add_user(CHECKER, "Dashboard", "Checker", "checker@lab.local", roles, password=pw)
    else:
        user.roles = roles
        sm.reset_password(user.id, pw)
    db.session.commit()
    user = sm.find_user(username=CHECKER)
    ok = (
        user is not None
        and check_password_hash(user.password, pw)
        and sorted(r.name for r in user.roles) == ["Gamma", VIEWER_ROLE]
        and len(perms) == len(names)
    )
    print(f"viewer {'ok' if ok else 'FAIL'}: user {CHECKER}, roles Gamma + {VIEWER_ROLE} ({len(perms)} datasets)")
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1]))
