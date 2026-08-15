from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_secret_scan.py SCAN.json")
    path = Path(sys.argv[1])
    payload = json.loads(path.read_text(encoding="utf-8"))
    results = payload.get("results")
    if not isinstance(results, dict):
        raise ValueError("detect-secrets output has no results object")
    findings = sum(len(value) for value in results.values() if isinstance(value, list))
    if findings:
        raise ValueError(f"secret scan found {findings} unreviewed candidates")
    print("SECRET_SCAN_OK findings=0")


if __name__ == "__main__":
    main()
