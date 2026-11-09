import base64
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def load_renderer(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("render_secrets", ROOT / "infra/k8s/render-secrets.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "OUT", str(tmp_path / "out"))  # never touch the real infra/k8s/secrets/
    return mod


def env_file(tmp_path, password="Pw1@x"):
    text = (ROOT / "infra/k8s/secrets.env.example").read_text()
    path = tmp_path / "secrets.env"
    path.write_text(
        "\n".join(
            line.replace("change-me-pg", password)
            .replace("change-me-airflow-db", password)
            .replace("change-me-minio", password)
            .replace("change-me-airflow-admin", password)
            .replace("change-me-telegram-token", "tg-token")
            .replace("change-me-telegram-chat", "-1")
            .replace("change-me-ghcr-token", "ghp_dummy")
            for line in text.splitlines()
        )
        + "\n"
    )
    return path


def test_refuses_change_me(tmp_path, monkeypatch):
    mod = load_renderer(tmp_path, monkeypatch)
    path = tmp_path / "secrets.env"
    path.write_text((ROOT / "infra/k8s/secrets.env.example").read_text())
    monkeypatch.setattr("sys.argv", ["render", str(path)])
    with pytest.raises(SystemExit):
        mod.main()


def test_generates_keys_once_and_escapes(tmp_path, monkeypatch):
    mod = load_renderer(tmp_path, monkeypatch)
    path = env_file(tmp_path)
    monkeypatch.setattr("sys.argv", ["render", str(path)])
    mod.main()
    first = path.read_text()
    mod.main()
    assert path.read_text() == first  # keys appended once, then stable
    out = tmp_path / "out"
    assert (out / "airflow-metadata.env").read_text().count("Pw1%40x") == 1  # password URL-encoded
    assert "Pw1@x" in (out / "hive-site.xml").read_text()
    assert oct((out / "s3.env").stat().st_mode & 0o777) == "0o600"
    assert (out / "s3.env").read_text().startswith("AWS_ACCESS_KEY_ID=spark-jobs\n")  # not the root user (2.1b)
    assert (out / "landing-writer.env").read_text().startswith("AWS_ACCESS_KEY_ID=landing-writer\n")
    assert (out / "hms-s3.env").read_text().startswith("AWS_ACCESS_KEY_ID=hive-metastore\n")
    assert (out / "trino-s3.env").read_text().startswith("TRINO_S3_ACCESS_KEY=trino\nTRINO_S3_SECRET_KEY=")  # Task 3
    assert (out / "trino-access-test.env").read_text().startswith("AWS_ACCESS_KEY_ID=trino\n")
    assert first.count("MINIO_SPARK_JOBS_SECRET=") == 1
    conns = dict(line.split("=", 1) for line in (out / "airflow-connections.env").read_text().splitlines())
    minio = json.loads(conns["AIRFLOW_CONN_MINIO_LANDING"])
    assert minio["login"] == "airflow-landing" and minio["extra"]["endpoint_url"].endswith(":9000")
    assert json.loads(conns["AIRFLOW_CONN_KUBERNETES_IN_CLUSTER"])["extra"]["in_cluster"] is True
    assert json.loads(conns["AIRFLOW_CONN_MINIO_LOGS"])["login"] == "airflow-logs"
    assert (out / "airflow-alerts.env").read_text() == "TELEGRAM_BOT_TOKEN=tg-token\nTELEGRAM_CHAT_ID=-1\n"
    ghcr = json.loads((out / "ghcr-dockerconfig.json").read_text())["auths"]["ghcr.io"]  # Secret ghcr-pull (2.8)
    assert ghcr["username"] == "Beno1tP" and ghcr["password"] == "ghp_dummy"
    assert base64.b64decode(ghcr["auth"]).decode() == "Beno1tP:ghp_dummy"
