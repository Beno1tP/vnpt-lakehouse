# Task 5 — Config: versions, RAM, disk

## 1. Versions (re-check before installing)

| Component | Pin | Check command (Bee) |
|---|---|---|
| Superset | input plan: **6.1.0** = latest stable, 7.0 only RC — **not verified here** | GitHub releases of `apache/superset`; `docker buildx imagetools inspect apache/superset:6.1.0` |
| Superset Helm chart | `https://apache.github.io/superset`, exact version + checksum; set `image.tag` explicitly (chart `appVersion` may lag) | `helm repo add superset https://apache.github.io/superset && helm search repo superset/superset --versions \| head` |
| `starrocks` SQLAlchemy dialect (+ MySQL driver) | pinned in `docker/superset/requirements.txt` | `pip index versions starrocks` |

## 2. RAM budget (Allocatable 7,268 Mi)

| Pod | Request | Limit | Note |
|---|---|---|---|
| Superset web | 1 Gi | 1.5 Gi | Gunicorn 2 workers; Redis, Celery, Postgres subchart **off** |
| Superset init / import Jobs | 512 Mi | 1 Gi | run, then exit |
| StarRocks FE + BE | 2.5 Gi | 3.5 Gi | Task 4 D13 |

| Mode | Running | Requests | Fits? |
|---|---|---|---|
| **serving** (demo) | lake, StarRocks, Superset; Airflow 0, Trino 0, no Spark | 4,036 − 2,048 (Airflow) + 2,688 + 1,024 ≈ **5,700** | ✅ |
| serving + Airflow | | ≈ 7,750 | ❌ |

So `make mode-serving` scales the Airflow deployments (api-server, scheduler, dag-processor, triggerer) to 0, Trino to 0, StarRocks + Superset to 1. `make mode-pipeline` does the reverse. Airflow scaled to 0 loses nothing (state in Postgres, logs in MinIO); scheduled runs missed while down are caught up per the DAG's `catchup` setting, so pause DAGs before a long demo if that is not wanted.

## 3. Disk budget

| Item | Estimate | Measure |
|---|---|---|
| Superset image + driver layer in the node | ~1–1.5 GB × 1.35 | `crictl images` |
| `superset` metadata DB | < 50 MB | `\l+` in psql |
| Screenshots in `docs/task5/img/` | < 5 MB | keep PNGs small |

End of the lab: the full stack (Task 2–5 images + data) lives in `docker_data.vhdx`. After the final demo: decide with Bee what to keep; `minikube delete` frees everything but deletes all data (backups live in the same MinIO).
