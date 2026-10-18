# VNPT AI Lakehouse Lab

Onboarding lab: an end-to-end lakehouse on trial chatbot logs (imaginary data for freshers; kept on the laptop). Each task is a standalone mini-project on the same data and can chain into one pipeline.

| Task | Scope | Status | Docs |
|---|---|---|---|
| 1 | Excel → Spark → Iceberg on MinIO, Hive Metastore (Docker Compose) | ✅ Bronze done; Silver stretch | [docs/task1/README.md](docs/task1/README.md) |
| 2 | Spark on Kubernetes (Minikube), orchestrated by Airflow | ⬜ | — |
| 3 | Trino queries the Iceberg tables | ⬜ | — |
| 4 | StarRocks Gold tables with SQL/dbt | ⬜ | — |
| 5 | Superset dashboard (UC4–UC8) | ⬜ | — |

Quick start (Task 1): `cp infra/compose/.env.example infra/compose/.env` (edit it), then `make up` and `make bronze DATE=YYYY-MM-DD`.

Progress and decisions: [docs/progress.md](docs/progress.md).
