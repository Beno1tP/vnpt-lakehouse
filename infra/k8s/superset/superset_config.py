"""Superset 6.1.0 config for the lab (Task 5, D24, D27): our own file instead of the deprecated Helm chart's.

Mounted from ConfigMap superset-config at /app/pythonpath; SUPERSET_CONFIG_PATH points here. Every secret comes from
the environment (Secret superset-env, rendered by make k8s-secrets): nothing secret is in this file or in Git.
No Redis/Celery (D24): Superset's defaults are used for caches (NullCache) and for filter / explore state (stored in
the metadata DB, SupersetMetastoreCache), so native filters work without Redis.
"""

import os
from urllib.parse import quote

# fail at start-up (KeyError) instead of running with Superset's built-in insecure default key
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{quote(os.environ['DB_USER'], safe='')}:{quote(os.environ['DB_PASS'], safe='')}"
    f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
)

# one web pod, laptop RAM: small metadata-DB pool (default 5 + 10 overflow per process)
SQLALCHEMY_ENGINE_OPTIONS = {"pool_size": 3, "max_overflow": 2, "pool_pre_ping": True}

# StarRocks Gold is pre-aggregated: a short SQL Lab limit keeps a mistyped query from loading 10k rows into the pod
SQL_MAX_ROW = 10000
SQLLAB_TIMEOUT = 60
