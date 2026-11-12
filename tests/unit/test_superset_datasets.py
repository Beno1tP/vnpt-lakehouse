import importlib.util
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
spec_ = importlib.util.spec_from_file_location("sd", ROOT / "infra/k8s/superset/superset_datasets.py")
sd = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(sd)
SPEC = yaml.safe_load((ROOT / "infra/k8s/superset/datasets.yaml").read_text())


def model_sql(table):
    return (ROOT / f"dbt/models/gold/{table}.sql").read_text()


def output_columns(sql):
    """Output columns of the model's last SELECT (alias after `as`, else the bare column after a table prefix)."""
    body = sql[sql.lower().rindex("select") + len("select") :]
    body = re.split(r"\bfrom\b", body, flags=re.I)[0]
    cols, depth, cur = [], 0, ""
    for ch in body:  # split on top-level commas only
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            cols.append(cur)
            cur = ""
        else:
            cur += ch
    cols.append(cur)
    out = set()
    for c in cols:
        c = c.strip()
        m = re.search(r"\bas\s+(\w+)\s*$", c, flags=re.I)
        out.add(m.group(1) if m else c.split(".")[-1])
    return out


def test_spec_is_valid():
    assert sd.validate(SPEC) == []


def test_spec_columns_equal_the_dbt_model_columns():  # the message_cnt mistake (5.4) fails here
    assert {d["table"] for d in SPEC["datasets"]} == {f"gold_uc{i}" for i in range(4, 9)}
    for ds in SPEC["datasets"]:
        assert set(ds["columns"]) == output_columns(model_sql(ds["table"])), ds["table"]


def test_validate_catches_unknown_column():
    bad = {"datasets": [{"table": "gold_uc5", "columns": ["event_date", "understood_cnt"],
                         "metrics": [{"name": "m", "expression": "SUM(message_cnt)"}], "checks": []}]}  # fmt: skip
    assert any("message_cnt" in e for e in sd.validate(bad))


def test_identifiers_ignore_functions_numbers_and_literals():
    assert sd.identifiers("SUM(a) * 1.0 / NULLIF(SUM(a + b), 0)") == {"a", "b"}
    assert sd.identifiers("traffic_class = 'real' AND event_date = '2024-07-25'") == {"traffic_class", "event_date"}
    assert sd.identifiers("BITMAP_UNION_COUNT(users)") == {"users"}


def test_no_sum_of_distinct_or_cumulative_over_days():
    for ds in SPEC["datasets"]:
        for m in ds["metrics"]:
            assert "SUM(active_users)" not in m["expression"] or "COUNT(DISTINCT event_date)" in m["expression"]
    uc8 = next(d for d in SPEC["datasets"] if d["table"] == "gold_uc8")
    assert next(m for m in uc8["metrics"] if m["name"] == "active_users")["expression"] == "BITMAP_UNION_COUNT(users)"


def test_uuid_is_stable_and_per_table():
    assert sd.dataset_uuid("gold_uc5") == sd.dataset_uuid("gold_uc5") != sd.dataset_uuid("gold_uc6")


def test_matches():
    assert sd.matches(10729, 10729) and sd.matches(0.0131421, 0.013142) and not sd.matches(10728, 10729)
    assert sd.matches(None, None) and not sd.matches(0, None) and not sd.matches(None, 0)
