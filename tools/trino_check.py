"""Task 3 step 3.4: compare Trino's Silver numbers (sql/trino/reconcile.sql, CSV day,key,value on stdin) with the
pandas expectations: tests/expected/silver_daily.json per day and silver.json in total. PASS/FAIL per check, exit 1 on
any mismatch, missing or unexpected value. Stdlib only (runs on the laptop: make trino-check pipes Trino's output in).

    kubectl -n trino exec deploy/trino-coordinator -- trino --catalog lake --output-format CSV_UNQUOTED \\
        --execute "$(cat sql/trino/reconcile.sql)" | python3 tools/trino_check.py
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

EXPECTED = Path(__file__).resolve().parents[1] / "tests/expected"


def parse(lines) -> dict[str, dict]:
    """day -> {key: int, "final_intent": {intent: int}} from 'day,key,value' rows."""
    days: dict[str, dict] = {}
    for row in csv.reader(lines):
        if not row or len(row) != 3:
            continue
        day, key, value = row[0].strip(), row[1].strip(), int(row[2])
        d = days.setdefault(day, {"final_intent": {}})
        if key.startswith("final_intent."):
            d["final_intent"][key.split(".", 1)[1]] = value
        else:
            d[key] = value
    return days


def totals(days: dict[str, dict]) -> dict:
    out: dict = {"final_intent": {}}
    for d in days.values():
        for k, v in d.items():
            if k == "final_intent":
                for intent, n in v.items():
                    out["final_intent"][intent] = out["final_intent"].get(intent, 0) + n
            else:
                out[k] = out.get(k, 0) + v
    out["event_dates"] = len(days)
    return out


def compare(got: dict, want: dict, label: str, report: list[bool]) -> None:
    for key in sorted(set(want) | set(got)):
        ok = got.get(key) == want.get(key)
        report.append(ok)
        if not ok or key == "final_intent":
            print(
                f"[trino-check] {'PASS' if ok else 'FAIL'}  {label:<12} {key:<30} "
                f"expected={want.get(key)}  trino={got.get(key)}",
                flush=True,
            )


def main() -> None:
    got = parse(sys.stdin)
    if not got:
        sys.exit("[trino-check] no rows from Trino (query mode? make mode-query)")
    want_days = json.loads((EXPECTED / "silver_daily.json").read_text())["days"]
    want_total = json.loads((EXPECTED / "silver.json").read_text())["totals"]
    report: list[bool] = []
    for day in sorted(set(want_days) | set(got)):
        compare(got.get(day, {}), want_days.get(day, {}), day, report)
    compare(totals(got), want_total, "TOTAL", report)
    failed = report.count(False)
    print(
        f"[trino-check] {len(report) - failed}/{len(report)} values equal (Trino = pandas = Spark-validated), "
        f"{len(got)} days",
        flush=True,
    )
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
