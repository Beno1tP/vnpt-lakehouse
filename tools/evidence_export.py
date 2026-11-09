"""Freeze step 2 (B2): copy Task evidence out of MinIO so it survives `minikube delete` (closes gap #5 for evidence).
Runs in the one-off pod infra/k8s/lake/evidence-export-pod.yaml: airflow-logs as MinIO user demo-viewer (read-only),
backup as backup-writer (list/read, no delete). Writes /export/<bucket>/… and /export/manifest.tsv (path, bytes).
Prints counts and sizes only, never file contents."""

import os
import sys

import pyarrow.fs as pafs

OUT = sys.argv[1] if len(sys.argv) > 1 else "/export"
SOURCES = {  # bucket -> (user, env var holding its secret key)
    "airflow-logs": ("demo-viewer", "MINIO_DEMO_VIEWER_SECRET"),
    "backup": ("backup-writer", "MINIO_BACKUP_SECRET"),
}
manifest = []
for bucket, (user, secret_env) in SOURCES.items():
    fs = pafs.S3FileSystem(
        endpoint_override=os.environ["S3_ENDPOINT"],
        scheme="http",
        region="us-east-1",
        access_key=user,
        secret_key=os.environ[secret_env],
    )
    files = [i for i in fs.get_file_info(pafs.FileSelector(bucket, recursive=True)) if i.type == pafs.FileType.File]
    total = 0
    for info in files:
        dest = os.path.join(OUT, info.path)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with fs.open_input_stream(info.path) as src, open(dest, "wb") as dst:
            dst.write(src.read())
        total += info.size
        manifest.append(f"{info.path}\t{info.size}")
    print(f"[evidence] {bucket}: {len(files)} files, {total / 1024:.1f} KiB", flush=True)
with open(os.path.join(OUT, "manifest.tsv"), "w") as f:
    f.write("\n".join(manifest) + "\n")
print(f"[evidence] manifest: {len(manifest)} files", flush=True)
