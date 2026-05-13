# -*- coding: utf-8 -*-
"""Reporting helpers for file-first outputs."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

LOG = logging.getLogger("Brownsea_Equity_Analysis")


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
    table_html = df.to_html(index=index, border=0, classes="dataframe")
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
