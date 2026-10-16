# -*- coding: utf-8 -*-
"""Reporting helpers for file-first outputs."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from src.constants import DeprivationConstants

LOG = logging.getLogger("Brownsea_Equity_Analysis")

# Readable headings for the HTML tables. CSV and parquet files keep the column names.
COLUMN_LABELS = {
    "District": "District",
    "total_journey_min": "Journey to Brownsea (min)",
    "nearest_competitor_drive_min": "Drive to nearest NT site (min)",
    "imd_decile_mean": "IMD decile",
    "geo_barriers_decile": "Geographical barriers decile",
    "wider_barriers_decile": "Wider barriers decile",
    "income_decile": "Income decile",
    "avg_fsm%": "Free school meals (%)",
    "pop%_most_deprived": "Most deprived residents (%)",
    "pop%_moderately_deprived": "Moderately deprived residents (%)",
    "pop%_least_deprived": "Least deprived residents (%)",
    "Population": "Population",
    "Authority_Name": "Local authority",
    "Region_Name": "Region",
    "Post_Town": "Post town",
    "lsoa_count": "LSOAs",
    "nearest_ferry_terminal": "Nearest ferry terminal",
    "competitor_context": "Nearby NT sites",
    "Visits": "Visits",
    "visits_per_1000": "Visits per 1,000",
    "predicted_visit_rate": "Expected visits per 1,000",
    "performance_gap": "Gap to expected",
    "visits_gap_raw": "Gap to target (signed)",
    "visits_gap": f"Gap to target ({DeprivationConstants.TARGET_VISIT_RATE:g} per 1,000)",
    "composite_need_score": "Need score",
    "impact_score": "Impact score",
    "need_tier": "Need tier",
    "visit_tier": "Visit tier",
    "priority_zone": "Priority zone",
    "intervention_type": "Intervention type",
    "deprivation_tier": "Deprivation tier",
    "fsm_tier": "Free school meals tier",
    "visit_rate_tier": "Visit rate tier",
    "composite_need_tier": "Need score tier",
    "intersection_segment": "Segment",
    "performance_ratio": "Observed / expected",
    "fragility_score": "Fragility score",
    "fragility_tier": "Fragility tier",
    "fragility_alert": "Fragility alert",
    "gap_score": "Gap score",
    "need_inverse_score": "Low-need score",
    "population_score": "Population score",
    "accessibility_score_norm": "Accessibility score",
    "growth_potential_score": "Growth potential score",
    "growth_potential_tier": "Growth potential tier",
    "safe_zone_band_width": "Model error band",
    "safe_zone_lower_1rmse": "Safe zone lower (1 RMSE)",
    "safe_zone_upper_1rmse": "Safe zone upper (1 RMSE)",
    "safe_zone_lower_2rmse": "Safe zone lower (2 RMSE)",
    "safe_zone_upper_2rmse": "Safe zone upper (2 RMSE)",
    "safe_zone_status": "Safe zone status",
    "needs_intervention": "Needs intervention",
    "high_potential_flag": "High potential",
    "underperformance_confidence": "Shortfall in RMSE",
    "risk_score": "Risk score",
    "risk_flags": "Risk flags",
    "risk_tier": "Risk tier",
    "sensitivity_total_journey_min_delta": "What-if: journey change (min)",
    "sensitivity_total_journey_min_new_pred": "What-if: expected visits per 1,000",
    "sensitivity_total_journey_min_impact": "What-if: change in expected visits",
    "shap_narrative": "Summary",
    "shap_error": "Summary error",
    "total_districts": "Districts",
    "total_population": "Population",
    "total_visits": "Visits",
    "avg_performance_gap": "Average gap to expected",
}


def readable_table(df: pd.DataFrame) -> pd.DataFrame:
    """Copy of a table with readable headings and numbers rounded for display."""
    shown = df.rename(columns=lambda column: COLUMN_LABELS.get(column, column))
    return shown.round(2)


def _reports_dir(config: dict) -> Path:
    path = Path(config.get("report_dir", config.get("output_dir", "outputs")))
    path.mkdir(parents=True, exist_ok=True)
    return path


def _artifacts_dir(config: dict) -> Path:
    path = Path(config.get("artifact_dir", config.get("output_dir", "outputs")))
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_subdirs(config: dict, *parts: str) -> tuple[Path, Path]:
    report_base = _reports_dir(config)
    artifact_base = _artifacts_dir(config)
    for part in parts:
        report_base = report_base / part
        artifact_base = artifact_base / part
    report_base.mkdir(parents=True, exist_ok=True)
    artifact_base.mkdir(parents=True, exist_ok=True)
    return report_base, artifact_base


def save_dataframe_bundle(
    df: pd.DataFrame,
    base_name: str,
    config: dict,
    *,
    title: Optional[str] = None,
    index: bool = False,
    section: str = "tables",
) -> dict:
    report_dir, artifact_dir = ensure_subdirs(config, section)
    csv_path = artifact_dir / f"{base_name}.csv"
    html_path = report_dir / f"{base_name}.html"
    parquet_path = artifact_dir / f"{base_name}.parquet"

    df.to_csv(csv_path, index=index)
    try:
        df.to_parquet(parquet_path, index=index)
    except Exception as exc:
        LOG.warning(f"Could not write parquet for {base_name}: {exc}")

    safe_title = title or base_name.replace("_", " ").title()
    table_html = readable_table(df).to_html(index=index, border=0, classes="dataframe")
    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\" />
<title>{safe_title}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
h1 {{ margin-bottom: 16px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }}
th {{ background: #2c3e50; color: white; position: sticky; top: 0; }}
tr:nth-child(even) {{ background: #f8f8f8; }}
.small {{ color: #666; margin-bottom: 18px; }}
</style>
</head>
<body>
<h1>{safe_title}</h1>
<div class=\"small\">Rows: {len(df):,}</div>
{table_html}
</body>
</html>"""
    html_path.write_text(html, encoding="utf-8")
    LOG.info(f"Saved table bundle: {csv_path} / {html_path}")
    return {"csv": str(csv_path), "html": str(html_path), "parquet": str(parquet_path)}


def save_plotly_bundle(fig, base_name: str, config: dict, *, section: str = "figures") -> dict:
    report_dir, _ = ensure_subdirs(config, section)
    html_path = report_dir / f"{base_name}.html"
    png_path = report_dir / f"{base_name}.png"
    fig.write_html(str(html_path), include_plotlyjs="cdn", full_html=True)
    try:
        fig.write_image(str(png_path), scale=2)
        png_written = str(png_path)
    except Exception as exc:
        LOG.warning(f"Could not write PNG for {base_name}: {exc}")
        png_written = None
    LOG.info(f"Saved figure bundle: {html_path}{' / ' + png_written if png_written else ''}")
    return {"html": str(html_path), "png": png_written}


def save_text_report(html: str, name: str, config: dict, *, section: str = "") -> str:
    report_dir = _reports_dir(config)
    if section:
        report_dir = report_dir / section
        report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / name
    path.write_text(html, encoding="utf-8")
    LOG.info(f"Saved report: {path}")
    return str(path)


def build_reports_index(config: dict) -> str:
    report_dir = _reports_dir(config)
    artifact_dir = _artifacts_dir(config)

    def list_links(base: Path, exts: set[str]):
        links = []
        for path in sorted(base.rglob("*")):
            if path.is_file() and path.suffix.lower() in exts:
                rel = path.relative_to(base)
                links.append((path.name, rel.as_posix()))
        return links

    report_links = list_links(report_dir, {".html", ".png"})
    artifact_links = list_links(artifact_dir, {".csv", ".json", ".parquet", ".xlsx"})

    def render_links(links, prefix=""):
        if not links:
            return "<p class='small'>No files yet.</p>"
        return "<ul>" + "".join([f'<li><a href="{prefix}{href}">{name}</a></li>' for name, href in links]) + "</ul>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Brownsea reports index</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
.card {{ border:1px solid #ddd; border-radius:12px; padding:16px; margin-bottom:18px; }}
a {{ color:#0f4c5c; }}
.small {{ color:#666; }}
</style>
</head>
<body>
<h1>Brownsea reports index</h1>
<div class="card">
<h2>Start here</h2>
<ul>
<li><a href="postcode_app.html">Postcode web app</a></li>
<li><a href="executive_summary.html">Executive summary</a></li>
<li><a href="strategic_framework_definitions.html">Strategic framework definitions</a></li>
<li><a href="model_performance.html">Model performance</a></li>
</ul>
</div>
<div class="card"><h2>Reports</h2>{render_links(report_links)}</div>
<div class="card"><h2>Artifacts</h2>{render_links(artifact_links, '../artifacts/')}</div>
</body>
</html>"""
    index_path = report_dir / "index.html"
    index_path.write_text(html, encoding="utf-8")
    LOG.info(f"Saved report index: {index_path}")
    return str(index_path)
