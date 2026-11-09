"""Freeze plan step 1: prove the demo user "viewer" (Viewer role) can read but not change runs.
Runs inside the api-server container (make airflow-viewer-check); argv[1] = viewer password (never printed).
Both write attempts are harmless even if the role were wrong: the trigger uses a logical date that already has a
run (E49 -> 409 instead of a new run) and the clear is a dry run. Prints HTTP codes only."""

import sys

import requests

BASE, DAG = "http://localhost:8080", "chatbot_bronze"
r = requests.post(f"{BASE}/auth/token", json={"username": "viewer", "password": sys.argv[1]}, timeout=30)
if r.status_code not in (200, 201):
    sys.exit(f"[viewer] login failed: HTTP {r.status_code} (does the auth manager expose /auth/token?)")
h = {"Authorization": f"Bearer {r.json()['access_token']}"}
checks = [  # (name, method, path, body, expected status)
    ("read DAG", "GET", f"/api/v2/dags/{DAG}", None, 200),
    ("read runs", "GET", f"/api/v2/dags/{DAG}/dagRuns?limit=1", None, 200),
    ("trigger run", "POST", f"/api/v2/dags/{DAG}/dagRuns", {"logical_date": "2024-07-24T00:00:00Z"}, 403),
    ("clear tasks", "POST", f"/api/v2/dags/{DAG}/clearTaskInstances", {"dry_run": True}, 403),
]
failed = 0
for name, method, path, body, want in checks:
    got = requests.request(method, BASE + path, json=body, headers=h, timeout=30).status_code
    ok = got == want
    failed += not ok
    print(f"[viewer] {'PASS' if ok else 'FAIL'}  {name:<12} expected={want} actual={got}")
print(f"[viewer] {len(checks) - failed}/{len(checks)} as expected")
sys.exit(1 if failed else 0)
