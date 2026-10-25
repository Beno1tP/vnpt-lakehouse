"""Task 2 — landing simulator: play the source system that drops one Excel export per day.

Splits each source Excel file by event day (the date as written in CREATED_TIME, no time zone conversion) and
writes one file per day to the landing zone:
    <dest>/chatlog/dt=YYYY-MM-DD/bot_dvc_hcm.xlsx
    <dest>/bot_predict/dt=YYYY-MM-DD/bot_ddvc_hcm_bot_predict.xlsx
Rerunning replaces the whole landing prefix of each table (idempotent). Prints counts only, never row contents.

    python3 landing_simulator.py --src /work --dest s3://landing [--expected bronze_daily.json]
    python3 landing_simulator.py --src /data/raw --dest /tmp/landing      (local test, no S3)
S3 credentials: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env; endpoint: S3_ENDPOINT env (host:port, http).
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys

import pandas as pd
import pyarrow.fs as pafs

SOURCES = {
    # table        : (file name,                       sheet)
    "chatlog":     ("bot_dvc_hcm.xlsx",                "Export Worksheet"),
    "bot_predict": ("bot_ddvc_hcm_bot_predict.xlsx",   "Export Worksheet"),
}
EXPECTED_KEY = {"chatlog": "chatlog_rows", "bot_predict": "bot_predict_rows"}


def filesystem(dest: str):
    """(filesystem, base path) for an s3:// URI or a local directory."""
    if dest.startswith("s3://"):
        fs = pafs.S3FileSystem(endpoint_override=os.environ["S3_ENDPOINT"], scheme="http", region="us-east-1")
        return fs, dest[len("s3://"):].rstrip("/")
    os.makedirs(dest, exist_ok=True)
    return pafs.LocalFileSystem(), os.path.abspath(dest)


def event_day(created_time: pd.Series) -> pd.Series:
    # '02-JUL-24 03.40.09.895455000 PM' -> 2024-07-02 (date part only; strptime %b is case-insensitive)
    return pd.to_datetime(created_time.str[:9], format="%d-%b-%y", errors="coerce").dt.strftime("%Y-%m-%d")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="folder with the 2 source Excel files")
    ap.add_argument("--dest", required=True, help="s3://landing or a local folder")
    ap.add_argument("--expected", help="tests/expected/bronze_daily.json: fail if per-day counts differ")
    args = ap.parse_args()

    fs, base = filesystem(args.dest)
    expected = json.load(open(args.expected))["days"] if args.expected else None
    failed = False

    for table, (fname, sheet) in SOURCES.items():
        pdf = pd.read_excel(os.path.join(args.src, fname), sheet_name=sheet, dtype=str, engine="openpyxl")
        days = event_day(pdf["CREATED_TIME"])
        if days.isna().any():
            sys.exit(f"ERROR: {table}: {int(days.isna().sum())} rows with unparseable CREATED_TIME")

        prefix = f"{base}/{table}"
        fs.delete_dir_contents(prefix, missing_dir_ok=True)          # idempotent: replace the table's landing data
        written = 0
        for day, part in pdf.groupby(days, sort=True):
            buf = io.BytesIO()
            part.to_excel(buf, sheet_name=sheet, index=False, engine="openpyxl")
            fs.create_dir(f"{prefix}/dt={day}")
            with fs.open_output_stream(f"{prefix}/dt={day}/{fname}") as out:
                out.write(buf.getvalue())
            written += len(part)
            want = expected.get(day, {}).get(EXPECTED_KEY[table]) if expected else None
            mark = "" if want is None else (" OK" if want == len(part) else f" FAIL expected={want}")
            failed |= mark.startswith(" FAIL")
            print(f"[landing] {table} dt={day}: {len(part)} rows{mark}")
        print(f"[landing] {table}: {len(pdf)} rows read, {written} rows written, {days.nunique()} days")
        if written != len(pdf):
            sys.exit(f"ERROR: {table}: written {written} != read {len(pdf)}")

    if failed:
        sys.exit("ERROR: per-day counts differ from --expected")
    print("[landing] done")


if __name__ == "__main__":
    main()
