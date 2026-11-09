"""Per-day report of a DAG from its task logs in s3://airflow-logs (step 2.6 remote logging), run INSIDE the
scheduler pod via stdin: kubectl -n airflow exec -i deploy/airflow-scheduler -c scheduler -- python - <dag_id>
Reads with the Airflow connection minio_logs (MinIO user airflow-logs). Prints counts only, never row contents.
Each pattern is counted once per log file: the operator may repeat the last second of driver log (seen in 2.5c)."""

import collections
import re
import sys

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

PATTERN = re.compile(
    r"\[landing\] [0-9-]+: chatlog=[a-z]+, bot_predict=[a-z]+"
    r"|\[bronze\] bronze\.[a-z_]+: [0-9]+ rows read, [0-9]+ rows in partition [0-9-]+"
    r"|\[validate\] [0-9]+/[0-9]+ checks passed for _ingest_date=[0-9-]+"
    r"|\[silver\] [a-z_]+: [0-9]+ rows in partition [0-9-]+"
    r"|\[validate_silver\] [0-9]+/[0-9]+ checks passed for event_date=[0-9-]+"
)

dag_id = sys.argv[1] if len(sys.argv) > 1 else "chatbot_bronze"
hook = S3Hook(aws_conn_id="minio_logs")
keys = hook.list_keys(bucket_name="airflow-logs", prefix=f"dag_id={dag_id}/") or []
seen = collections.Counter()
for key in keys:
    for line in set(PATTERN.findall(hook.read_key(key, bucket_name="airflow-logs"))):
        seen[line] += 1
for line, n in sorted(seen.items()):
    print(f"{n:5d} {line}")
print(f"[report] {len(keys)} log files read from s3://airflow-logs/dag_id={dag_id}/")
if not seen:
    sys.exit("[report] no landing/bronze/validate lines found: wrong DAG, no runs yet, or logs not uploaded")
