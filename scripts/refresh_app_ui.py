#!/usr/bin/env python3
"""Refresh the postcode lookup HTML UI from an existing release artifact.

This does not rerun the pipeline. It reads artifacts/postcode_lookup.json and
rewrites the static HTML reports that display the lookup.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.web_ui import build_downloads_html, build_postcode_app_html
from src.help_page import write_help_html


def _release_dir(outputs_root: Path, release_name: str) -> Path:
    return outputs_root / "releases" / release_name


def refresh_app_ui(outputs_root: Path, release_name: str = "latest") -> list[Path]:
    release = _release_dir(outputs_root, release_name)
    lookup_json = release / "artifacts" / "postcode_lookup.json"
    if not lookup_json.exists():
        raise FileNotFoundError(f"Missing postcode lookup JSON: {lookup_json}")

    records = json.loads(lookup_json.read_text(encoding="utf-8"))
    df = pd.DataFrame(records)
    reports = release / "reports"
    reports.mkdir(parents=True, exist_ok=True)

    metadata = {
        "title": "Brownsea Visitor Opportunity Lookup",
        "generated_at": "refreshed from release artifact",
        "download_csv": "../artifacts/postcode_lookup.csv",
        "download_json": "../artifacts/postcode_lookup.json",
        "reports_index": "index.html",
        "downloads_page": "downloads.html",
        "help_page": "help.html",
    }

    targets = [reports / "postcode_app.html", reports / "postcode_lookup.html"]
    for target in targets:
        build_postcode_app_html(df, target, metadata)
    downloads = reports / "downloads.html"
    build_downloads_html(release, downloads)
    targets.append(downloads)
    help_page = reports / "help.html"
    write_help_html(help_page, home_href="postcode_app.html", downloads_href="downloads.html", reports_href="index.html")
    targets.append(help_page)
    return targets


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh postcode app HTML from an existing release without rerunning the pipeline")
    parser.add_argument("outputs_root", help="Outputs root containing releases/, for example /content/drive/MyDrive/brownsea/outputs")
    parser.add_argument("--release-name", default="latest")
    args = parser.parse_args()

    try:
        targets = refresh_app_ui(Path(args.outputs_root), args.release_name)
    except Exception as exc:
        print(f"Refresh app UI: FAIL")
        print(f"  reason: {exc}")
        return 1

    print("Refresh app UI: PASS")
    for path in targets:
        print(f"  wrote: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
