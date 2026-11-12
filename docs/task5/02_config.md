# Task 5 — Config: versions, RAM, disk

## 1. Versions (checked 2026-11-12, Bee)

| Component | Pin | Evidence |
|---|---|---|
| Superset | **6.1.0**, `apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88` | `docker buildx imagetools inspect` |
| Superset Helm chart | **0.22.8** (appVersion 6.1.0; 0.22.7 / 0.22.4 also 6.1.0); download + checksum at 5.3 (as the other charts), `image.tag` + digest set explicitly | `helm search repo superset/superset --versions` |
| `starrocks` SQLAlchemy dialect | **1.3.4** (2026-08-01, Python ≥ 3.10): requires `sqlalchemy>=1.4`, `pymysql>=1.1.0`, `alembic>=1.14.0`, `lark>=1.3.1`, `asyncmy2>=0.2.20` | PyPI `requires_dist` |
| `psycopg2-binary` | **2.9.13** (metadata DB in our Postgres) | `pip index versions` |
| Install rule (D26) | the two packages are installed with Superset's own installed versions as constraints; CI `pip check` + import test; a conflict fails the build | — |

## 2. RAM budget (Allocatable 7,268 Mi, measured values)

| Pod | Request | Limit | Note |
|---|---|---|---|
| Superset web | 1 Gi | 1.5 Gi | Gunicorn 2 workers; Redis, Celery, Postgres subcharts **off** (D24); measure at 5.3 |
| Superset init / db-init Jobs | 256–512 Mi | 1 Gi | run, then exit |
| StarRocks FE + BE + operator | 2,176 Mi | 3,840 Mi | Task 4 D15, measured |

| Mode | Running | Requests | Fits? |
|---|---|---|---|
| demo-task4 (measured 11-12) | lake, Airflow api-server, History, StarRocks; scheduler 0 | **5,092 Mi (70 %)** | ✅ |
| **serving = demo-task5** (D22) | demo-task4 + Superset | ≈ **6,116 Mi (84 %)** | ✅ (≈ 1.1 Gi left; Jobs run one at a time) |
| serving + scheduler/dag-processor/triggerer | | ≈ 7,236 Mi | ❌ too tight (real usage above requests, Task 4 §4) |

After every `make k8s-up` rerun the profile: `airflow-install` resets the scheduler replicas (seen 11-12).

## 3. Disk budget

| Item | Estimate | Measure |
|---|---|---|
| Superset image + driver layer in the node | ~1–1.5 GB × 1.35 | `crictl images` |
| `superset` metadata DB | < 50 MB | `\l+` in psql |
| Screenshots in `docs/task5/img/` | < 5 MB | keep PNGs small |

End of the lab: the full stack (Task 2–5 images + data) lives in `docker_data.vhdx`. After the final demo: decide with Bee what to keep; `minikube delete` frees everything but deletes all data (backups live in the same MinIO).
