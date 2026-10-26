"""Summarise a Trivy JSON report: one line per scanned target with counts by severity (no CVE details)."""

import json
import sys
from collections import Counter

with open(sys.argv[1]) as f:
    report = json.load(f)
total = Counter()
for result in report.get("Results", []):
    counts = Counter(v["Severity"] for v in result.get("Vulnerabilities") or [])
    if counts:
        total.update(counts)
        print(f"- `{result['Target']}`: CRITICAL {counts['CRITICAL']}, HIGH {counts['HIGH']}")
print(f"- **total (fixable)**: CRITICAL {total['CRITICAL']}, HIGH {total['HIGH']}")
