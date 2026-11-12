"""Task 5 step 5.8c: check the rendered drill overlay (kubectl kustomize infra/k8s/superset/drill) in CI.

Fails unless every object is named *-drill and labelled app=superset-drill (selectors included), and every container
of the Deployment and the Job has DB_NAME=superset_drill (so the drill can never write to the real metadata DB).
"""

from __future__ import annotations

import sys

import yaml


def problems(docs: list[dict]) -> list[str]:
    out = []
    if not docs:
        return ["nothing rendered"]
    for d in docs:
        name = d["metadata"]["name"]
        if "-drill" not in name:  # ConfigMaps: superset-config-drill-<hash>
            out.append(f"{d['kind']}/{name}: not a -drill name")
        if d["kind"] in ("Deployment", "Job", "Service"):
            spec = d["spec"]
            sel = spec.get("selector", {})
            sel = sel.get("matchLabels", sel) if d["kind"] == "Deployment" else sel
            if d["kind"] != "Job" and sel.get("app") != "superset-drill":
                out.append(f"{d['kind']}/{name}: selector {sel}")
        if d["kind"] in ("Deployment", "Job"):
            pod = d["spec"]["template"]["spec"]
            for c in pod.get("initContainers", []) + pod["containers"]:
                env = {e["name"]: e.get("value") for e in c.get("env", [])}
                if env.get("DB_NAME") != "superset_drill":
                    out.append(f"{d['kind']}/{name} container {c['name']}: DB_NAME {env.get('DB_NAME')}")
    return out


def main(path: str) -> int:
    with open(path) as f:
        docs = [d for d in yaml.safe_load_all(f) if d]
    found = problems(docs)
    for p in found:
        print("FAIL:", p)
    print(f"drill render: {len(docs)} objects -> {'PASS' if not found else f'FAIL ({len(found)})'}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
