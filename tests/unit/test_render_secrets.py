import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def load_renderer(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("render_secrets", ROOT / "infra/k8s/render-secrets.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "OUT", str(tmp_path / "out"))      # never touch the real infra/k8s/secrets/
    return mod


def env_file(tmp_path, password="Pw1@x"):
    text = (ROOT / "infra/k8s/secrets.env.example").read_text()
    path = tmp_path / "secrets.env"
    path.write_text("\n".join(line.replace("change-me-pg", password).replace("change-me-airflow-db", password)
                              .replace("change-me-minio", password).replace("change-me-airflow-admin", password)
                              for line in text.splitlines()) + "\n")
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
    assert path.read_text() == first                                  # keys appended once, then stable
    out = tmp_path / "out"
    assert (out / "airflow-metadata.env").read_text().count("Pw1%40x") == 1   # password URL-encoded
    assert "Pw1@x" in (out / "hive-site.xml").read_text()
    assert oct((out / "s3.env").stat().st_mode & 0o777) == "0o600"
