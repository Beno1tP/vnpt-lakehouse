"""Task 3 freeze: verify the Iceberg tag through TRINO (no Spark needed, so it fits next to Trino in a demo profile).
stdin: CSV 'table,rows' (rows read FOR VERSION AS OF the tag); argv[1] = tag. Expected rows = the same totals as
jobs/catalog_tool.py verify-tag (EXPECTED_KEY), read from tests/expected/. PASS/FAIL per table, exit 1 on any FAIL."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

EXPECTED_DIR = Path(__file__).resolve().parents[1] / "tests/expected"
EXPECTED_KEY = {  # keep equal to jobs/catalog_tool.py (unit-tested)
    "bronze.chatlog": ("bronze_daily.json", "chatlog_rows"),
    "bronze.bot_predict": ("bronze_daily.json", "bot_predict_rows"),
    "silver.chat_message": ("silver.json", "chat_rows"),
    "silver.nlu_prediction": ("silver.json", "predict_rows"),
    "silver.user_message": ("silver.json", "user_message_rows"),
}


def main() -> None:
    tag = sys.argv[1]
    rows = {r[0].strip(): int(r[1]) for r in csv.reader(sys.stdin) if len(r) == 2}
    if not rows:
        sys.exit("[tag-check] no rows from Trino (is the tag missing? query mode?)")
    failed = 0
    for table, got in sorted(rows.items()):
        file, key = EXPECTED_KEY[table]
        want = json.loads((EXPECTED_DIR / file).read_text())["totals"][key]
        ok = got == want
        failed += not ok
        print(f"[catalog] {'PASS' if ok else 'FAIL'} {table}: tag {tag} rows {got} (expected {want}, read by Trino)")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
