"""Step 2.1b negative/positive test of the MinIO service users (gap #9). Runs in a pod with one user's keys
(AWS_* env) and checks that exactly the expected operations are allowed. Prints names and results only."""

import os
import sys

import pyarrow.fs as pafs

PROFILES = {  # operation -> expected result for each user
    "spark-jobs": {
        "read landing": True,
        "read lakehouse": True,
        "write landing": False,
        "write+delete lakehouse": True,
        "create bucket": False,
        "read backup": False,
    },
    "landing-writer": {
        "read landing": True,
        "read lakehouse": False,
        "write landing": True,
        "write+delete lakehouse": False,
        "create bucket": False,
        "read backup": False,
    },
    "demo-viewer": {  # demo console login (freeze plan step 1): read and list only, never backups
        "read landing": True,
        "read lakehouse": True,
        "write landing": False,
        "write+delete lakehouse": False,
        "create bucket": False,
        "read backup": False,
    },
    "trino": {  # Task 3 (D11): Trino reads Iceberg files in lakehouse/ and nothing else
        "read landing": False,
        "read lakehouse": True,
        "write landing": False,
        "write+delete lakehouse": False,
        "create bucket": False,
        "read backup": False,
    },
}
fs = pafs.S3FileSystem(
    endpoint_override=os.environ["S3_ENDPOINT"], scheme="http", region="us-east-1", allow_bucket_creation=True
)


def attempt(op: str) -> bool:
    try:
        if op == "read landing":
            files = [
                i
                for i in fs.get_file_info(pafs.FileSelector("landing", recursive=True))
                if i.type == pafs.FileType.File
            ]
            with fs.open_input_stream(files[0].path) as f:
                f.read(16)
        elif op == "read lakehouse":
            files = [
                i
                for i in fs.get_file_info(pafs.FileSelector("lakehouse/warehouse", recursive=True))
                if i.type == pafs.FileType.File
            ]
            with fs.open_input_stream(files[0].path) as f:
                f.read(16)
        elif op == "write landing":
            with fs.open_output_stream("landing/_access_test/probe.txt") as f:
                f.write(b"probe")
            fs.delete_file("landing/_access_test/probe.txt")
        elif op == "write+delete lakehouse":
            with fs.open_output_stream("lakehouse/_access_test/probe.txt") as f:
                f.write(b"probe")
            fs.delete_file("lakehouse/_access_test/probe.txt")
        elif op == "read backup":
            fs.get_file_info(pafs.FileSelector("backup", recursive=False))
        elif op == "create bucket":
            fs.create_dir("access-test-should-not-exist")
            fs.delete_dir("access-test-should-not-exist")
        return True
    except OSError as e:
        if "ACCESS_DENIED" in str(e).upper() or "ACCESSDENIED" in str(e).upper().replace(" ", ""):
            return False
        raise


profile = sys.argv[1]
failed = 0
for op, want in PROFILES[profile].items():
    got = attempt(op)
    ok = got == want
    failed += not ok
    print(
        f"[s3-access] {'PASS' if ok else 'FAIL'}  {profile:<15} {op:<24} expected={'allowed' if want else 'denied'}"
        f"  actual={'allowed' if got else 'denied'}",
        flush=True,
    )
print(f"[s3-access] {len(PROFILES[profile]) - failed}/{len(PROFILES[profile])} as expected for {profile}")
sys.exit(1 if failed else 0)
