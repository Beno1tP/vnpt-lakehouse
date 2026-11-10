"""Render Task 2 secret files from infra/k8s/secrets.env (git-ignored) into infra/k8s/secrets/ (git-ignored, mode 600).

kustomize (secretGenerator in infra/k8s/kustomization.yaml) turns these files into Kubernetes Secrets,
one Secret per consumer, so each pod only gets the keys it needs. Run: make k8s-secrets [K8S_ENV_FILE=...]
"""

import base64
import json
import os
import secrets
import sys
import urllib.parse
import xml.sax.saxutils as xs

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "secrets")
REQUIRED = (
    "PG_USER",
    "PG_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "GHCR_PULL_TOKEN",
)
# generated once and appended to secrets.env, so they stay stable across renders (a new fernet key would make
# stored Airflow connections unreadable; a new JWT secret breaks running tasks)
GENERATED = {
    "AIRFLOW_FERNET_KEY": lambda: base64.urlsafe_b64encode(os.urandom(32)).decode(),
    "AIRFLOW_API_SECRET_KEY": lambda: secrets.token_hex(32),
    "AIRFLOW_JWT_SECRET": lambda: secrets.token_hex(32),
    # MinIO service users (step 2.1b): spark-jobs, landing-writer, hive-metastore
    "MINIO_SPARK_JOBS_SECRET": lambda: secrets.token_hex(24),
    "MINIO_LANDING_WRITER_SECRET": lambda: secrets.token_hex(24),
    "MINIO_HMS_SECRET": lambda: secrets.token_hex(24),
    "MINIO_AIRFLOW_LANDING_SECRET": lambda: secrets.token_hex(24),  # step 2.4: DAG check_landing
    "MINIO_AIRFLOW_LOGS_SECRET": lambda: secrets.token_hex(24),  # step 2.6: Airflow remote task logs
    "MINIO_BACKUP_SECRET": lambda: secrets.token_hex(24),  # step 2.5b: pg-backup (hex: URL-safe)
    "MINIO_DEMO_VIEWER_SECRET": lambda: secrets.token_hex(24),  # demo console login "demo-viewer": read-only
    "MINIO_TRINO_SECRET": lambda: secrets.token_hex(24),  # Task 3: Trino reads lakehouse/ only (D11)
    "AIRFLOW_VIEWER_PASSWORD": lambda: secrets.token_urlsafe(18),  # Airflow UI user "viewer" (Viewer role) for demos
    "STARROCKS_ROOT_PASSWORD": lambda: secrets.token_hex(24),  # Task 4: StarRocks root (hex: safe inside SQL quotes)
}


def read_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def write_private(name, text):
    path = os.path.join(OUT, name)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.chmod(path, 0o600)


def main():
    env_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "secrets.env")
    env = read_env(env_path)
    missing = [k for k in REQUIRED if not env.get(k)]
    if missing:
        sys.exit(f"missing keys in secrets.env: {', '.join(missing)}")
    if any(v.startswith("change-me") for v in env.values()):
        sys.exit("secrets.env still has change-me passwords: edit it first")
    new = {k: make() for k, make in GENERATED.items() if not env.get(k)}
    if new:
        with open(env_path, "a") as f:
            f.write("".join(f"{k}={v}\n" for k, v in new.items()))
        env.update(new)
        print("generated and appended to secrets.env:", ", ".join(new))
    os.makedirs(OUT, mode=0o700, exist_ok=True)
    # Postgres: superuser for the metastore DB + password of the airflow DB role (created by the init script)
    write_private("postgres.env", "".join(f"{k}={env[k]}\n" for k in ("PG_USER", "PG_PASSWORD", "AIRFLOW_DB_PASSWORD")))
    # MinIO root credentials (server, bucket-init, and S3 clients until per-service keys exist)
    write_private("minio.env", "".join(f"{k}={env[k]}\n" for k in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")))
    # MinIO service users (step 2.1b): passwords for the minio-users Job, and per-consumer S3 keys (AWS SDK env names)
    users = (
        "MINIO_SPARK_JOBS_SECRET",
        "MINIO_LANDING_WRITER_SECRET",
        "MINIO_HMS_SECRET",
        "MINIO_AIRFLOW_LANDING_SECRET",
        "MINIO_AIRFLOW_LOGS_SECRET",
        "MINIO_BACKUP_SECRET",
        "MINIO_DEMO_VIEWER_SECRET",
        "MINIO_TRINO_SECRET",
    )
    write_private("minio-service-users.env", "".join(f"{k}={env[k]}\n" for k in users))

    def s3_keys(user, secret_key):
        return f"AWS_ACCESS_KEY_ID={user}\nAWS_SECRET_ACCESS_KEY={env[secret_key]}\n"

    write_private("s3.env", s3_keys("spark-jobs", "MINIO_SPARK_JOBS_SECRET"))  # r landing, rw lakehouse
    write_private("landing-writer.env", s3_keys("landing-writer", "MINIO_LANDING_WRITER_SECRET"))  # rw landing
    write_private("hms-s3.env", s3_keys("hive-metastore", "MINIO_HMS_SECRET"))  # rw lakehouse
    write_private("demo-viewer.env", s3_keys("demo-viewer", "MINIO_DEMO_VIEWER_SECRET"))  # read-only (s3-access-test)
    write_private("trino-s3.env", f"TRINO_S3_ACCESS_KEY=trino\nTRINO_S3_SECRET_KEY={env['MINIO_TRINO_SECRET']}\n")
    write_private("trino-access-test.env", s3_keys("trino", "MINIO_TRINO_SECRET"))  # same user, AWS names
    write_private("starrocks-root.env", f"password={env['STARROCKS_ROOT_PASSWORD']}\n")  # key read by the chart
    # Airflow (namespace airflow): metadata DB connection + keys, one file per Secret the Helm chart expects
    pw = urllib.parse.quote(env["AIRFLOW_DB_PASSWORD"], safe="")
    write_private(
        "airflow-metadata.env",
        f"connection=postgresql+psycopg2://airflow:{pw}@postgres.lake.svc.cluster.local:5432/airflow\n",
    )
    write_private("airflow-fernet.env", f"fernet-key={env['AIRFLOW_FERNET_KEY']}\n")
    write_private("airflow-api.env", f"api-secret-key={env['AIRFLOW_API_SECRET_KEY']}\n")
    write_private("airflow-jwt.env", f"jwt-secret={env['AIRFLOW_JWT_SECRET']}\n")
    write_private("airflow-admin-password", env["AIRFLOW_ADMIN_PASSWORD"])
    write_private("airflow-viewer-password", env["AIRFLOW_VIEWER_PASSWORD"])
    # Airflow connections as env vars (AIRFLOW_CONN_<ID>, JSON form) -> Secret airflow-connections (step 2.4)
    minio = {
        "conn_type": "aws",
        "login": "airflow-landing",
        "password": env["MINIO_AIRFLOW_LANDING_SECRET"],
        "extra": {"endpoint_url": "http://minio.lake.svc.cluster.local:9000", "region_name": "us-east-1"},
    }
    logs = {
        "conn_type": "aws",
        "login": "airflow-logs",
        "password": env["MINIO_AIRFLOW_LOGS_SECRET"],
        "extra": {"endpoint_url": "http://minio.lake.svc.cluster.local:9000", "region_name": "us-east-1"},
    }
    kube = {"conn_type": "kubernetes", "extra": {"in_cluster": True}}
    write_private(
        "airflow-connections.env",
        f"AIRFLOW_CONN_MINIO_LANDING={json.dumps(minio)}\n"
        f"AIRFLOW_CONN_MINIO_LOGS={json.dumps(logs)}\n"
        f"AIRFLOW_CONN_KUBERNETES_IN_CLUSTER={json.dumps(kube)}\n",
    )
    # Telegram (step 2.6) for plugins/lake_alerts.py, in its own Secret
    write_private("airflow-alerts.env", "".join(f"{k}={env[k]}\n" for k in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")))
    # GHCR pull secret (2.0c), rendered since 2.8 so a from-zero cluster needs no typed token; read:packages only
    user = env.get("GHCR_USER", "Beno1tP")
    auth = base64.b64encode(f"{user}:{env['GHCR_PULL_TOKEN']}".encode()).decode()
    cfg = {"auths": {"ghcr.io": {"username": user, "password": env["GHCR_PULL_TOKEN"], "auth": auth}}}
    write_private("ghcr-dockerconfig.json", json.dumps(cfg))
    # HMS JDBC settings as a file (never in SERVICE_OPTS: the entrypoint prints it with set -x)
    props = {
        "javax.jdo.option.ConnectionDriverName": "org.postgresql.Driver",
        "javax.jdo.option.ConnectionURL": "jdbc:postgresql://postgres.lake.svc.cluster.local:5432/metastore",
        "javax.jdo.option.ConnectionUserName": env["PG_USER"],
        "javax.jdo.option.ConnectionPassword": env["PG_PASSWORD"],
    }
    body = "".join(f"  <property><name>{k}</name><value>{xs.escape(v)}</value></property>\n" for k, v in props.items())
    write_private(
        "hive-site.xml",
        '<?xml version="1.0"?>\n<!-- generated by render-secrets.py from secrets.env: do not commit -->\n'
        f"<configuration>\n{body}</configuration>\n",
    )
    print("k8s secrets rendered (mode 600):", ", ".join(sorted(os.listdir(OUT))))


if __name__ == "__main__":
    main()
