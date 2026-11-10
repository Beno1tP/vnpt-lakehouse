"""Task 3 steps 3.4 + 3.6: Trino reconciliation shared by the laptop (tools/trino_check.py) and Airflow (DAG
trino_reconcile). One implementation of the comparison: Trino's per-day Silver numbers (sql/trino/reconcile.sql,
rows day,key,value) against the pandas expectations (tests/expected/silver_daily.json + silver.json).

Also small stdlib helpers for the DAG: a Trino REST client (no provider in the Airflow image), Kubernetes calls to scale
Trino and to see whether a Spark application is still running (Trino 2 Gi + a Spark run do not fit the node, D12).
Counts only, never row contents.
"""

from __future__ import annotations

import csv
import json
import time
import urllib.request

TRINO_URL = "http://trino.trino.svc.cluster.local:8080"
TRINO_NS, TRINO_DEPLOY = "trino", "trino-coordinator"
SPARK_NS = "spark-jobs"


# --- comparison (pure, unit-tested) ---------------------------------------------------------------------------------
NESTED = ("final_intent", "step")  # keys whose value is {label: count}; Trino sends them as '<key>.<label>'


def parse(rows) -> dict[str, dict]:
    """day -> {key: int, "final_intent"/"step": {label: int}} from (day, key, value) rows or 'day,key,value' CSV."""
    days: dict[str, dict] = {}
    if rows and isinstance(next(iter(rows), None), str):
        rows = csv.reader(rows)
    for row in rows:
        if not row or len(row) != 3:
            continue
        day, key, value = str(row[0]).strip(), str(row[1]).strip(), int(row[2])
        d = days.setdefault(day, {k: {} for k in NESTED})
        group, _, label = key.partition(".")
        if group in NESTED and label:
            d[group][label] = value
        else:
            d[key] = value
    return days


def totals(days: dict[str, dict]) -> dict:
    out: dict = {k: {} for k in NESTED}
    for d in days.values():
        for k, v in d.items():
            if k in NESTED:
                for label, n in v.items():
                    out[k][label] = out[k].get(label, 0) + n
            else:
                out[k] = out.get(k, 0) + v
    out["event_dates"] = len(days)
    return out


def compare(got: dict, want: dict, label: str, report: list[bool], out=print) -> None:
    for key in sorted(set(want) | set(got)):
        ok = got.get(key) == want.get(key)
        report.append(ok)
        if not ok or key in NESTED:
            out(
                f"[trino-check] {'PASS' if ok else 'FAIL'}  {label:<12} {key:<30} "
                f"expected={want.get(key)}  trino={got.get(key)}"
            )


def reconcile(got: dict[str, dict], want_days: dict, want_total: dict, out=print) -> tuple[int, int]:
    """Compare every day and the totals; returns (passed, failed)."""
    report: list[bool] = []
    for day in sorted(set(want_days) | set(got)):
        compare(got.get(day, {}), want_days.get(day, {}), day, report, out)
    compare(totals(got), want_total, "TOTAL", report, out)
    failed = report.count(False)
    out(
        f"[trino-check] {len(report) - failed}/{len(report)} values equal (Trino = pandas = Spark-validated), "
        f"{len(got)} days"
    )
    return len(report) - failed, failed


# --- Trino REST (stdlib) --------------------------------------------------------------------------------------------
def trino_query(sql: str, base: str = TRINO_URL, user: str = "airflow", catalog: str = "lake", timeout: int = 60):
    """Run one statement through Trino's client REST protocol and return all data rows (lists)."""
    req = urllib.request.Request(
        f"{base}/v1/statement",
        data=sql.strip().rstrip(";").encode(),
        headers={"X-Trino-User": user, "X-Trino-Catalog": catalog},
        method="POST",
    )
    rows: list = []
    with urllib.request.urlopen(req, timeout=timeout) as r:
        page = json.load(r)
    while True:
        if page.get("error"):
            err = page["error"]
            raise RuntimeError(f"Trino {err.get('errorName')}: {err.get('message')}")
        rows.extend(page.get("data") or [])
        nxt = page.get("nextUri")
        if not nxt:
            return rows
        with urllib.request.urlopen(nxt, timeout=timeout) as r:
            page = json.load(r)


# --- Kubernetes (in-cluster, service account airflow-scheduler) ------------------------------------------------------
def _k8s():
    from kubernetes import client, config  # in the Airflow image via the cncf.kubernetes provider

    config.load_incluster_config()
    return client


def unfinished_spark_apps() -> int:
    items = (
        _k8s()
        .CustomObjectsApi()
        .list_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", SPARK_NS, "sparkapplications")["items"]
    )
    states = [((i.get("status") or {}).get("applicationState") or {}).get("state") for i in items]
    return sum(s not in ("COMPLETED", "FAILED") for s in states)


def scale_trino(replicas: int, wait_s: int = 600) -> None:
    apps = _k8s().AppsV1Api()
    apps.patch_namespaced_deployment_scale(TRINO_DEPLOY, TRINO_NS, {"spec": {"replicas": replicas}})
    deadline = time.time() + wait_s
    while time.time() < deadline:
        status = apps.read_namespaced_deployment(TRINO_DEPLOY, TRINO_NS).status
        if (status.ready_replicas or 0) == replicas or (replicas == 0 and not status.replicas):
            return
        time.sleep(5)
    raise TimeoutError(f"{TRINO_DEPLOY} did not reach {replicas} ready replicas in {wait_s} s")
