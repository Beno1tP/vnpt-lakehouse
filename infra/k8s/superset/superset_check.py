"""Task 5 step 5.7: check every chart of the dashboard through Superset's REST API as the read-only user `checker`.

Runs in the one-off pod superset-check (make superset-check). For the dashboard in dashboard_checks.yaml:
  1. log in as checker (Gamma + gold_viewer): the dashboard must be visible to a non-admin (published, dataset access)
  2. read the native filters from the dashboard's json_metadata: target column, default value, charts in scope;
     they must equal the spec (Traffic -> traffic_class default real, Users -> user_class no default) and every chart
     must be in exactly the filter the spec names
  3. every chart on the dashboard must be in the spec and vice versa
  4. for each chart and each filter value: the chart's own saved query_context, post-processing removed, plus the
     filter as the dashboard applies it -> POST /api/v1/chart/data -> pick the value -> compare, unrounded
Prints PASS/FAIL per check (aggregates only, no row contents beyond the expected keys), exit 1 on any FAIL.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import os
import sys

import yaml

BASE = os.environ.get("SUPERSET_URL", "http://superset.superset.svc.cluster.local:8088")


# ---------- pure helpers (unit-tested) ----------
def norm(v):
    """Comparable value: epoch-ms -> ISO date, numbers -> float, rest unchanged."""
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, (int, float)):
        if abs(v) > 1e11:  # epoch milliseconds (dates come back as timestamps)
            return dt.datetime.fromtimestamp(v / 1000, dt.timezone.utc).date().isoformat()
        return float(v)
    if isinstance(v, str) and len(v) >= 10 and v[4] == "-" and v[7] == "-":
        return v[:10]
    return v


def same(got, expect, tol=1e-5):
    if isinstance(expect, dict):
        return isinstance(got, dict) and set(got) == set(expect) and all(same(got[k], expect[k]) for k in expect)
    got, expect = norm(got), norm(expect)
    if expect is None or got is None:
        return got is None and expect is None
    if isinstance(expect, float) and isinstance(got, float):
        return abs(got - expect) <= tol
    return got == expect


def time_key(rows, metrics):
    keys = [k for k in rows[0] if k not in metrics]
    if len(keys) != 1:
        raise ValueError(f"cannot tell the time column among {keys}")
    return keys[0]


def pick(rows, spec):
    if "value" in spec:
        if len(rows) != 1:
            raise ValueError(f"expected 1 row, got {len(rows)}")
        return rows[0][spec["value"]]
    if "sum" in spec:
        return sum(r[spec["sum"]] or 0 for r in rows)
    if "sums" in spec:
        return {m: sum(r[m] or 0 for r in rows) for m in spec["sums"]}
    if "max" in spec:
        return max(r[spec["max"]] for r in rows if r[spec["max"]] is not None)
    if "last" in spec:
        if not rows:
            return None
        tk = time_key(rows, {spec["last"]})
        return max(rows, key=lambda r: r[tk])[spec["last"]]
    if "by" in spec:
        out: dict = {}
        for r in rows:
            k = r[spec["by"]["column"]]
            out[k] = out.get(k, 0) + (r[spec["by"]["metric"]] or 0)
        return out
    raise ValueError(f"unknown picker {spec}")


def native_filters(json_metadata: dict) -> dict:
    """name -> {column, default, charts} from the dashboard's native_filter_configuration."""
    out = {}
    for f in json_metadata.get("native_filter_configuration", []):
        if f.get("type") == "DIVIDER":
            continue
        targets = f.get("targets") or [{}]
        default = ((f.get("defaultDataMask") or {}).get("filterState") or {}).get("value")
        out[f["name"]] = {
            "column": ((targets[0].get("column") or {}).get("name")),
            "default": default,
            "charts": set(f.get("chartsInScope") or []),
        }
    return out


def with_filter(query_context: dict, column: str | None, value: str) -> dict:
    qc = copy.deepcopy(query_context)
    qc["result_format"], qc["result_type"] = "json", "full"
    qc.pop("form_data", None)
    for q in qc["queries"]:
        q["post_processing"] = []  # raw aggregated rows: pivots / contribution / rolling are display-only
        q["row_limit"] = max(q.get("row_limit") or 0, 10000)
        if column and value:
            q.setdefault("filters", []).append({"col": column, "op": "IN", "val": [value]})
    return qc


def structure_problems(spec: dict, filters: dict, charts: dict) -> list[str]:
    """charts: name -> id. Returns FAIL messages for filters/scoping/chart lists."""
    probs = []
    for name, want in spec["filters"].items():
        got = filters.get(name)
        if got is None:
            probs.append(f"filter {name}: missing on the dashboard (have {sorted(filters)})")
            continue
        if got["column"] != want["column"]:
            probs.append(f"filter {name}: column {got['column']} != {want['column']}")
        if (got["default"] or None) != want["default"]:
            probs.append(f"filter {name}: default {got['default']} != {want['default']}")
    for extra in sorted(set(charts) - set(spec["charts"])):
        probs.append(f"chart '{extra}' is on the dashboard but not in dashboard_checks.yaml (unchecked)")
    for missing in sorted(set(spec["charts"]) - set(charts)):
        probs.append(f"chart '{missing}' expected but not on the dashboard")
    for name, c in spec["charts"].items():
        if name not in charts:
            continue
        cid = charts[name]
        for fname, f in filters.items():
            if fname not in spec["filters"]:
                continue
            inside = cid in f["charts"]
            if inside != (c["filter"] == fname):
                probs.append(f"chart '{name}': {'in' if inside else 'not in'} scope of {fname} (spec: {c['filter']})")
    return probs


# ---------- API (runs in the pod) ----------
def main(path: str) -> int:  # pragma: no cover - needs a running Superset
    import requests

    with open(path) as f:
        spec = yaml.safe_load(f)
    s = requests.Session()
    login = {
        "username": os.environ["SUPERSET_CHECKER_USER"],
        "password": os.environ["SUPERSET_CHECKER_PASSWORD"],
        "provider": "db",
        "refresh": False,
    }
    r = s.post(f"{BASE}/api/v1/security/login", json=login, timeout=30)
    r.raise_for_status()
    s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    s.headers["X-CSRFToken"] = s.get(f"{BASE}/api/v1/security/csrf_token/", timeout=30).json()["result"]
    s.headers["Referer"] = BASE
    print(f"logged in as {os.environ['SUPERSET_CHECKER_USER']}")
    dashboards = s.get(f"{BASE}/api/v1/dashboard/", params={"q": "(page_size:100)"}, timeout=30).json()["result"]
    match = [d for d in dashboards if d["dashboard_title"] == spec["dashboard"]]
    if len(match) != 1:
        print(f"FAIL: dashboard '{spec['dashboard']}' visible to checker: {len(match)} (published? dataset access?)")
        return 1
    dash = s.get(f"{BASE}/api/v1/dashboard/{match[0]['id']}", timeout=30).json()["result"]
    filters = native_filters(json.loads(dash.get("json_metadata") or "{}"))
    charts = {
        c["slice_name"]: c["id"]
        for c in s.get(f"{BASE}/api/v1/dashboard/{dash['id']}/charts", timeout=30).json()["result"]
    }
    fails = 0
    for p in structure_problems(spec, filters, charts):
        print("FAIL:", p)
        fails += 1
    print(f"dashboard '{spec['dashboard']}' id {dash['id']}: {len(charts)} charts, filters {sorted(filters)}")
    for name, c in spec["charts"].items():
        if name not in charts:
            continue
        chart = s.get(f"{BASE}/api/v1/chart/{charts[name]}", timeout=30).json()["result"]
        if not chart.get("query_context"):
            print(f"FAIL {name}: no saved query_context (open the chart, Save it again)")
            fails += 1
            continue
        qc = json.loads(chart["query_context"])
        column = spec["filters"][c["filter"]]["column"] if c["filter"] else None
        for value, expect in c["expect"].items():
            r = s.post(f"{BASE}/api/v1/chart/data", json=with_filter(qc, column, value), timeout=120)
            label = f"{name} [{c['filter'] or 'no filter'}={value or 'cleared'}]"
            if r.status_code != 200:
                print(f"FAIL {label}: HTTP {r.status_code} {r.text[:200]}")
                fails += 1
                continue
            try:
                got = pick(r.json()["result"][0]["data"], c["pick"])
            except (KeyError, ValueError) as e:
                print(f"FAIL {label}: cannot pick {c['pick']}: {e}")
                fails += 1
                continue
            ok = same(got, expect)
            fails += not ok
            print(f"{'PASS' if ok else 'FAIL'} {label}: {got} (expected {expect})")
    print(f"superset-check: {'PASS' if not fails else f'FAIL ({fails})'}")
    return 1 if fails else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1]))
