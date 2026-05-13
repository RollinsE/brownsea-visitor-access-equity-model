#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI wrapper for app smoke tests."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app_smoke import print_smoke_result, result_to_dict, smoke_test_app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the Brownsea postcode app against a completed release/build.")
    parser.add_argument("target", nargs="?", default="outputs", help="Outputs root, build directory, or release directory")
    parser.add_argument("--release-name", default="latest", help="Release name when target is an outputs root")
    parser.add_argument("--json", action="store_true", help="Print full smoke-test result as JSON")
    args = parser.parse_args(argv)

    try:
        result = smoke_test_app(args.target, release_name=args.release_name)
        if args.json:
            print(json.dumps(result_to_dict(result), indent=2))
        else:
            print_smoke_result(result)
        return 0 if result.ok else 1
    except Exception as exc:
        if args.json:
            print(json.dumps({"status": "error", "error": str(exc)}, indent=2))
        else:
            print("App smoke test: ERROR")
            print(f"  {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
