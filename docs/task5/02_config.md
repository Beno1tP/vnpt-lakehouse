# Task 5 — Config: versions, RAM, disk

## 1. Versions (checked 2026-11-12, Bee)

| Component | Pin | Evidence |
|---|---|---|
| Superset | **6.1.0**, `apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88` | `docker buildx imagetools inspect` |
| ~~Superset Helm chart 0.22.8~~ | not used (D27): `deprecated: true` upstream; successor Superset Kubernetes Operator (v0.3.0, 2026-09-30) not used either | CI render, E80 |
| `starrocks` SQLAlchemy dialect | **1.3.4** (2026-08-01, Python ≥ 3.10): requires `sqlalchemy>=1.4`, `pymysql>=1.1.0`, `alembic>=1.14.0`, `lark>=1.3.1`, `asyncmy2>=0.2.20` | PyPI `requires_dist` |
| `psycopg2-binary` | **2.9.13** (metadata DB in our Postgres) | `pip index versions` |
| Install rule (D26) | the two packages are installed with Superset's own installed versions as constraints; CI `pip check` + import test; a conflict fails the build | — |
| Built image (5.1) | `vnpt-lake-superset@sha256:a0307416…946b` from 4662349: SQLAlchemy stays **1.4.54** (Superset's pin), psycopg2 2.9.13, packages 157 → 162 (+5: the dialect, the driver and their new dependencies), none changed or removed, `superset version` 6.1.0 | CI `superset-image` (PR #115) |

## 2. RAM budget (Allocatable 7,268 Mi, measured values)

| Pod | Request | Limit | Note |
|---|---|---|---|
| Superset web | 1 Gi | 1.5 Gi | Gunicorn 1 worker × 20 threads (image default); no Redis/Celery (D24); measure at 5.3b |
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
