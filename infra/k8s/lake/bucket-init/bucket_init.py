"""Create the S3 buckets on MinIO if missing (idempotent). Runs in the thin Spark image (pyarrow already inside),
so no extra image (Task 1 used amazon/aws-cli, ~650 MB). Credentials from mounted secret files; prints names only."""
import os
import sys
import time

import pyarrow.fs as pafs

SECRETS = "/run/secrets/minio"
ENDPOINT = os.environ["S3_ENDPOINT"]
BUCKETS = os.environ["BUCKETS"].split()


def read(name):
    with open(os.path.join(SECRETS, name)) as f:
        return f.read().strip()


fs = pafs.S3FileSystem(access_key=read("MINIO_ROOT_USER"), secret_key=read("MINIO_ROOT_PASSWORD"),
                       endpoint_override=ENDPOINT, scheme="http", region="us-east-1",
                       allow_bucket_creation=True)

for attempt in range(60):                       # wait up to ~2 min for MinIO to answer
    try:
        fs.get_file_info(BUCKETS[0])
        break
    except OSError as e:
        print(f"waiting for minio ({attempt + 1}/60): {str(e)[:120]}", flush=True)
        time.sleep(2)
else:
    sys.exit("ERROR: MinIO did not answer")

for bucket in BUCKETS:
    if fs.get_file_info(bucket).type == pafs.FileType.NotFound:
        fs.create_dir(bucket)
        print(f"bucket {bucket}: created")
    else:
        print(f"bucket {bucket}: exists")

missing = [b for b in BUCKETS if fs.get_file_info(b).type == pafs.FileType.NotFound]
if missing:
    sys.exit(f"ERROR: buckets still missing: {missing}")
print("buckets ok:", " ".join(BUCKETS))
