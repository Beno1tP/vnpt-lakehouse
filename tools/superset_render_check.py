"""Task 5 step 5.3a: check the rendered Superset chart (helm template on stdin) before anything is installed.

    helm template superset <chart> -n superset -f infra/helm/superset/values-local.yaml \\
        | .venv/bin/python -I tools/superset_render_check.py <SUPERSET_IMAGE_REF from images.env>

Fails unless: every container image is our image by the digest in images.env; there is one Deployment (the web
node) and no StatefulSet, Job, worker, beat or websocket; the chart creates no env Secret (we own superset-env) and
no Secret carries DB_PASS / SUPERSET_SECRET_KEY / an admin password; no pod runs as root; the web pod reads
superset-env; the generated superset_config.py has no Redis/Celery and builds the DB URI from env(). Prints the
config lines that matter, never secret values (there are none in a render).
"""

from __future__ import annotations

import base64
import sys

import yaml

FORBIDDEN_SECRET_KEYS = {"DB_PASS", "SUPERSET_SECRET_KEY", "ADMIN_PASSWORD", "REDIS_PASSWORD"}
CONFIG_MARKERS = (
    "SQLALCHEMY_DATABASE_URI",
    "DB_HOST",
    "SECRET_KEY",
    "CACHE_CONFIG",
    "REDIS",
    "Celery",
    "RESULTS_BACKEND",
)


def strip_tag(ref: str) -> str:
    """repo:tag@sha256:x -> repo@sha256:x (the tag is informative, the digest decides)."""
    if "@" not in ref:
        return ref
    name, digest = ref.split("@", 1)
    last = name.rsplit("/", 1)
    if ":" in last[-1]:
        name = name[: name.rindex(":")]
    return f"{name}@{digest}"


def pod_spec(doc: dict) -> dict | None:
    spec = doc.get("spec") or {}
    if doc.get("kind") in ("Deployment", "StatefulSet", "DaemonSet", "Job"):
        return (spec.get("template") or {}).get("spec")
    if doc.get("kind") == "Pod":
        return spec
    return None


def secret_items(doc: dict) -> dict[str, str]:
    items = dict(doc.get("stringData") or {})
    for k, v in (doc.get("data") or {}).items():
        items[k] = base64.b64decode(v).decode(errors="replace")
    return items


def check(docs: list[dict], image_ref: str) -> tuple[list[str], list[str]]:
    fails: list[str] = []
    info: list[str] = []
    if not docs:
        return ["no documents on stdin (did helm template fail?)"], info
    kinds = sorted(f"{d.get('kind')}/{d['metadata']['name']}" for d in docs)
    info.append("rendered: " + ", ".join(kinds))
    deployments = [d for d in docs if d.get("kind") == "Deployment"]
    if len(deployments) != 1:
        fails.append(f"expected exactly 1 Deployment (web), got {[d['metadata']['name'] for d in deployments]}")
    for d in docs:
        name = f"{d.get('kind')}/{d['metadata']['name']}"
        if d.get("kind") in ("StatefulSet", "Job", "CronJob"):
            fails.append(f"{name}: not expected (subcharts / chart init must be off)")
        if any(w in d["metadata"]["name"] for w in ("worker", "beat", "websocket", "redis", "postgresql")):
            fails.append(f"{name}: component must be disabled")
        spec = pod_spec(d)
        if spec is not None:
            pod_uid = (spec.get("securityContext") or {}).get("runAsUser")
            for c in (spec.get("initContainers") or []) + (spec.get("containers") or []):
                if strip_tag(c.get("image", "")) != image_ref:
                    fails.append(f"{name} container {c['name']}: image {c.get('image')} is not {image_ref}")
                uid = (c.get("securityContext") or {}).get("runAsUser", pod_uid)
                if uid in (None, 0):
                    fails.append(f"{name} container {c['name']}: runAsUser {uid} (root or unset)")
                info.append(f"{name} container {c['name']}: runAsUser {uid}")
            refs = [
                e["secretRef"]["name"]
                for c in spec.get("containers") or []
                for e in c.get("envFrom") or []
                if "secretRef" in e
            ]
            if d.get("kind") == "Deployment" and "superset-env" not in refs:
                fails.append(f"{name}: does not read Secret superset-env (envFrom {refs})")
        if d.get("kind") == "Secret":
            items = secret_items(d)
            if d["metadata"]["name"] == "superset-env":
                fails.append("Secret superset-env rendered by the chart (secretEnv.create must be false)")
            bad = FORBIDDEN_SECRET_KEYS & set(items)
            if bad:
                fails.append(f"{name}: carries {sorted(bad)}")
            for key, text in items.items():
                if "admin@superset.com" in text:
                    fails.append(f"{name}/{key}: chart admin defaults present")
                if key.endswith(".py") and "superset_config" in key:
                    info.append(f"{name}/{key} (lines that matter):")
                    lines = [ln.strip() for ln in text.splitlines()]
                    info += [f"    {ln}" for ln in lines if any(m in ln for m in CONFIG_MARKERS)]
                    if "REDIS_BASE_URL" in text or "CeleryConfig" in text or "CACHE_CONFIG" in text:
                        fails.append(f"{name}/{key}: Redis/Celery/cache config still generated")
                    if "DB_PASS" not in text:
                        fails.append(f"{name}/{key}: DB URI does not read env DB_PASS")
    if not any(ln.startswith("Secret/") or ln.startswith("ConfigMap/") for ln in kinds):
        fails.append("no config Secret/ConfigMap rendered")
    if not any("superset_config" in ln for ln in info):
        fails.append("superset_config.py not found in the rendered Secrets")
    return fails, info


def main() -> int:
    if len(sys.argv) != 2 or "@sha256:" not in sys.argv[1]:
        print("usage: superset_render_check.py <repo@sha256:digest>", file=sys.stderr)
        return 2
    docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
    fails, info = check(docs, sys.argv[1])
    print("\n".join(info))
    for f in fails:
        print("FAIL:", f)
    print("superset-render: " + ("PASS" if not fails else f"FAIL ({len(fails)})"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
