#!/bin/bash
# Step 2.1b (gap #9): one MinIO user per consumer, each with only the policy it needs. Idempotent: policies are
# (re)created, users (re)set to the password from the secret, a policy is attached only if not attached yet.
# Root keys are used here and by bucket-init only. Prints user/policy names, never keys.
set -euo pipefail
cp /tools/mc /tmp/mc && chmod 0755 /tmp/mc            # mc copied from the Chainguard minio-client image
MC=(/tmp/mc --config-dir /tmp/mc-config --quiet)
for i in $(seq 1 60); do
  "${MC[@]}" alias set lake http://minio.lake.svc.cluster.local:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && break
  echo "waiting for minio ($i/60)"; sleep 2
done
"${MC[@]}" admin info lake >/dev/null

for policy in /policies/*.json; do
  name=$(basename "$policy" .json)
  "${MC[@]}" admin policy create lake "$name" "$policy" >/dev/null
  echo "policy $name: created/updated"
done

ensure_user() {   # user  secret-env-name  policy
  local user=$1 secret=${!2} policy=$3 attached
  "${MC[@]}" admin user add lake "$user" "$secret" >/dev/null
  attached=$("${MC[@]}" admin user info lake "$user" --json | python3 -c 'import sys,json;print(json.load(sys.stdin).get("policyName",""))')
  if [[ ",$attached," != *",$policy,"* ]]; then
    "${MC[@]}" admin policy attach lake "$policy" --user "$user" >/dev/null
  fi
  echo "user $user: policy $policy"
}
ensure_user spark-jobs      MINIO_SPARK_JOBS_SECRET      lake-spark-jobs
ensure_user landing-writer  MINIO_LANDING_WRITER_SECRET  lake-landing-writer
ensure_user hive-metastore  MINIO_HMS_SECRET             lake-hms
ensure_user airflow-landing MINIO_AIRFLOW_LANDING_SECRET lake-landing-reader
ensure_user airflow-logs    MINIO_AIRFLOW_LOGS_SECRET    lake-airflow-logs
echo "minio users ok"
