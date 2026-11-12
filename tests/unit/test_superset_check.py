import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DIR = ROOT / "infra/k8s/superset"
spec_ = importlib.util.spec_from_file_location("sc", DIR / "superset_check.py")
sc = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(sc)
CHECKS = yaml.safe_load((DIR / "dashboard_checks.yaml").read_text())
DATASETS = yaml.safe_load((DIR / "datasets.yaml").read_text())


def test_every_expected_metric_exists_in_the_dataset_spec():
    metrics = {m["name"] for d in DATASETS["datasets"] for m in d["metrics"]}
    for name, c in CHECKS["charts"].items():
        p = c["pick"]
        used = p.get("sums") or [p.get("value") or p.get("sum") or p.get("last") or p.get("max") or p["by"]["metric"]]
        for m in used:
            assert m in metrics or m == "MAX(event_date)", (name, m)
        assert c["filter"] in (None, *CHECKS["filters"]), name
        assert set(c["expect"]) <= {"", "real", "external", "internal"}, name


def test_pickers():
    rows = [
        {"event_date": 1, "m": 2, "d": "a"},
        {"event_date": 3, "m": None, "d": "b"},
        {"event_date": 2, "m": 5, "d": "a"},
    ]
    assert sc.pick(rows, {"sum": "m"}) == 7
    assert sc.pick(rows, {"sums": ["m"]}) == {"m": 7}
    days = [{"event_date": 1, "m": 2}, {"event_date": 3, "m": None}, {"event_date": 2, "m": 5}]
    assert sc.pick(days, {"last": "m"}) is None  # latest day is NULL (UC7 change %)
    assert sc.pick(days[:1] + days[2:], {"last": "m"}) == 5
    assert sc.pick(rows, {"max": "m"}) == 5
    assert sc.pick(rows, {"by": {"column": "d", "metric": "m"}}) == {"a": 7, "b": 0}
    assert sc.pick([{"v": 1}], {"value": "v"}) == 1


def test_same_unrounded_and_dates():
    assert sc.same(3.625, 3.625) and not sc.same(3.63, 3.625)  # 87/24: compare the value, not the display
    assert sc.same(0.0131420, 0.013142) and sc.same(10729, 10729) and not sc.same(10728, 10729)
    assert sc.same(1721865600000, "2024-07-25") and sc.same("2024-07-25T00:00:00", "2024-07-25")
    assert sc.same(None, None) and not sc.same(0, None)
    assert sc.same({"a": 1, "b": 2}, {"a": 1, "b": 2}) and not sc.same({"a": 1}, {"a": 1, "b": 0})


def test_with_filter_adds_filter_and_drops_post_processing():
    qc = {"form_data": {}, "queries": [{"filters": [{"col": "x"}], "post_processing": [{"operation": "pivot"}]}]}
    out = sc.with_filter(qc, "traffic_class", "real")
    q = out["queries"][0]
    assert q["post_processing"] == [] and {"col": "traffic_class", "op": "IN", "val": ["real"]} in q["filters"]
    assert "form_data" not in out and qc["queries"][0]["post_processing"]  # original untouched
    assert sc.with_filter(qc, "traffic_class", "")["queries"][0]["filters"] == [{"col": "x"}]  # cleared


def meta(traffic_charts, users_charts, traffic_default=("real",)):
    def f(name, col, charts, default):
        mask = {"filterState": {"value": list(default) if default else None}}
        return {"name": name, "targets": [{"column": {"name": col}}], "chartsInScope": charts, "defaultDataMask": mask}

    return {"native_filter_configuration": [f("Traffic", "traffic_class", traffic_charts, traffic_default),
                                            f("Users", "user_class", users_charts, None)]}  # fmt: skip


def test_structure_ok_and_each_problem_found():
    names = list(CHECKS["charts"])
    charts = {n: i for i, n in enumerate(names)}
    t = [charts[n] for n, c in CHECKS["charts"].items() if c["filter"] == "Traffic"]
    u = [charts[n] for n, c in CHECKS["charts"].items() if c["filter"] == "Users"]
    assert sc.structure_problems(CHECKS, sc.native_filters(meta(t, u)), charts) == []
    probs = sc.structure_problems(CHECKS, sc.native_filters(meta(t, u[:-1], None)), {**charts, "new": 99})
    text = " | ".join(probs)
    assert "default None != ['real']" in text  # Traffic default lost
    assert "not in scope of Users" in text  # a user chart outside the Users filter
    assert "'new' is on the dashboard but not in" in text  # unreviewed chart
    probs = sc.structure_problems(CHECKS, sc.native_filters(meta(t + [charts["Data up to"]], u)), charts)
    assert any("'Data up to': in scope of Traffic" in p for p in probs)  # freshness must not be filtered
