# -*- coding: utf-8 -*-
"""Maintained National Trust competitor site reference loader."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

LOG = logging.getLogger("Brownsea_Equity_Analysis")
REQUIRED_COLUMNS = {"site_id", "site_name", "lat", "lon", "active"}


def get_default_nt_sites_path() -> Path:
    env_path = os.environ.get("BROWSEA_NT_SITES_PATH", "").strip()
    if env_path:
        return Path(env_path)

    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    return project_root / "data" / "reference" / "nt_sites.csv"


def load_nt_sites(path: str | Path | None = None) -> pd.DataFrame:
    nt_path = Path(path) if path else get_default_nt_sites_path()
    if not nt_path.exists():
        raise FileNotFoundError(f"National Trust reference file not found: {nt_path}")

    df = pd.read_csv(nt_path)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"nt_sites.csv is missing required columns: {sorted(missing)}")

    df = df.copy()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["active"] = df["active"].astype(str).str.strip().str.lower().isin(["true", "1", "yes", "y"])
    df = df[df["active"]].dropna(subset=["lat", "lon", "site_name"])
    # Brownsea is the destination being assessed, not a competing NT site.
    df = df[~df["site_name"].astype(str).str.contains("brownsea", case=False, na=False)]

    if df.empty:
        raise ValueError(f"No active NT competitor sites available in {nt_path}")

    LOG.info(f"Loaded {len(df)} active NT competitor sites from {nt_path}")
    return df.reset_index(drop=True)
