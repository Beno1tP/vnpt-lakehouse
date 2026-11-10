"""Task 3 step 3.4: compare Trino's Silver numbers (sql/trino/reconcile.sql, CSV day,key,value on stdin) with the
pandas expectations: tests/expected/silver_daily.json per day and silver.json in total. PASS/FAIL per check, exit 1 on
any mismatch, missing or unexpected value. The comparison lives in plugins/lake_reconcile.py (shared with the Airflow
DAG trino_reconcile, step 3.6).

    kubectl -n trino exec deploy/trino-coordinator -- trino --catalog lake --output-format CSV_UNQUOTED \\
        --execute "$(cat sql/trino/reconcile.sql)" | python3 tools/trino_check.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins"))  # run with python3 -I: no script dir on sys.path

from lake_reconcile import compare, parse, reconcile, totals  # noqa: E402

EXPECTED = ROOT / "tests/expected"
__all__ = ["compare", "parse", "totals", "main"]


def main() -> None:
    got = parse(list(sys.stdin))
    if not got:
        sys.exit("[trino-check] no rows from Trino (query mode? make mode-query)")
    want_days = json.loads((EXPECTED / "silver_daily.json").read_text())["days"]
    want_total = json.loads((EXPECTED / "silver.json").read_text())["totals"]
    _, failed = reconcile(got, want_days, want_total)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
