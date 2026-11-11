-- Task 4 (D17): idempotent StarRocks setup, run by the Job starrocks-init (make starrocks-init). Mounted from the
-- ConfigMap starrocks-init-sql (kustomize), so a grant change needs `make k8s-up` + `make starrocks-init`, not an
-- image build. One statement per block, ending with ";" at the end of a line. ${NAME} = a value from the Job's
-- environment (Secret starrocks-init-credentials), filled in by scripts/starrocks_init.py, which also masks them.

-- External catalog: Iceberg tables via HMS + MinIO, user starrocks (reads lakehouse/ only: two layers, as D11)
CREATE EXTERNAL CATALOG IF NOT EXISTS lake_iceberg PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hive",
  "hive.metastore.uris" = "thrift://hive-metastore.lake.svc.cluster.local:9083",
  "aws.s3.endpoint" = "http://minio.lake.svc.cluster.local:9000",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.enable_ssl" = "false",
  "aws.s3.region" = "us-east-1",
  "aws.s3.access_key" = "${S3_ACCESS_KEY}",
  "aws.s3.secret_key" = "${S3_SECRET_KEY}"
);

CREATE DATABASE IF NOT EXISTS gold;

-- Users: passwords reset on every run (rotation = change secrets.env, make k8s-up, make starrocks-init)
CREATE USER IF NOT EXISTS 'dbt_rw'@'%' IDENTIFIED BY '${STARROCKS_DBT_PASSWORD}';
ALTER USER 'dbt_rw'@'%' IDENTIFIED BY '${STARROCKS_DBT_PASSWORD}';
CREATE USER IF NOT EXISTS 'bi_ro'@'%' IDENTIFIED BY '${STARROCKS_BI_PASSWORD}';
ALTER USER 'bi_ro'@'%' IDENTIFIED BY '${STARROCKS_BI_PASSWORD}';

-- dbt_rw: everything on gold; read + REFRESH (metadata cache, E72) on the catalog, no write
GRANT ALL ON DATABASE gold TO USER 'dbt_rw'@'%';
GRANT ALL ON ALL TABLES IN DATABASE gold TO USER 'dbt_rw'@'%';
GRANT ALL ON ALL VIEWS IN DATABASE gold TO USER 'dbt_rw'@'%';
GRANT USAGE ON CATALOG lake_iceberg TO USER 'dbt_rw'@'%';
SET CATALOG lake_iceberg;
GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'dbt_rw'@'%';
GRANT REFRESH ON ALL TABLES IN ALL DATABASES TO USER 'dbt_rw'@'%';
SET CATALOG default_catalog;

-- bi_ro: read gold only (Superset, Task 5). Stored as a wildcard: also covers tables dbt drops and recreates
-- (tested 11-11: bi_ro read gold_uc5 right after a rebuild)
GRANT SELECT ON ALL TABLES IN DATABASE gold TO USER 'bi_ro'@'%';
GRANT SELECT ON ALL VIEWS IN DATABASE gold TO USER 'bi_ro'@'%';
