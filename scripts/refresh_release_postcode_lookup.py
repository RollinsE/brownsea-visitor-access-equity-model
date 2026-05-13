#!/usr/bin/env python3
"""Refresh released postcode lookup artifacts without rerunning the full pipeline.

This is intended for app-display/postcode-access fixes that depend only on the
released Stage 1 checkpoints and Stage 4 district analysis. It updates the
selected release in place, so run release QA afterwards.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.postcode_lookup import build_postcode_lookup_artifacts
from src.release_qa import validate_release


def _read_dataframe(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)


def refresh_release_postcode_lookup(outputs_root: Path, release_name: str = "latest") -> Path:
    release = outputs_root / "releases" / release_name
    if not release.exists():
        raise FileNotFoundError(f"Release not found: {release}")

    checkpoints = release / "checkpoints"
    artifacts = release / "artifacts"
    reports = release / "reports"
    artifacts.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    ons_df = _read_dataframe(checkpoints / "ons_clean.parquet")
    lsoa_master_df = _read_dataframe(checkpoints / "lsoa_master.parquet")

    analysis_path = artifacts / "three_way_intersection_analysis_v2.csv"
    if not analysis_path.exists():
        alias_path = artifacts / "tables" / "analysis_table.csv"
        if alias_path.exists():
            analysis_path = alias_path
    analysis_df = _read_dataframe(analysis_path)

    nt_sites = Path("data/reference/nt_sites.csv")
    config = {
        "reference_paths": {"nt_sites": str(nt_sites) if nt_sites.exists() else None},
        "output_files": {
            "postcode_lookup_csv": str(artifacts / "postcode_lookup.csv"),
            "postcode_lookup_parquet": str(artifacts / "postcode_lookup.parquet"),
            "postcode_lookup_json": str(artifacts / "postcode_lookup.json"),
            "postcode_lookup_html": str(reports / "postcode_lookup.html"),
            "postcode_app_html": str(reports / "postcode_app.html"),
        },
    }
    build_postcode_lookup_artifacts(ons_df, lsoa_master_df, analysis_df, config)
    return release


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh postcode lookup artifacts inside an existing release")
    parser.add_argument("outputs_root", help="Outputs root containing releases/, for example /content/drive/MyDrive/brownsea/outputs")
    parser.add_argument("--release-name", default="latest")
    parser.add_argument("--qa", action="store_true", help="Run release QA after refreshing")
    args = parser.parse_args()

    try:
        release = refresh_release_postcode_lookup(Path(args.outputs_root), args.release_name)
    except Exception as exc:
        print("Refresh release postcode lookup: FAIL")
        print(f"  reason: {exc}")
        return 1

    print("Refresh release postcode lookup: PASS")
    print(f"  release: {release}")
    print(f"  updated: {release / 'artifacts' / 'postcode_lookup.json'}")
    print(f"  updated: {release / 'reports' / 'postcode_app.html'}")

    if args.qa:
        result = validate_release(Path(args.outputs_root), args.release_name)
        if not result.ok:
            print("Release QA after refresh: FAIL")
            return 1
        print("Release QA after refresh: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
