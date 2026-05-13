#!/usr/bin/env python3
"""Export a completed Brownsea release as a static staff-facing web app.

The static export is designed for GitHub Pages or any simple static web host.
It does not require Flask, Python, Colab, or a running server once exported.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from src.help_page import write_help_html

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SELECTED_ARTIFACTS = [
    "postcode_lookup.csv",
    "postcode_lookup.json",
    "model_performance.csv",
    "model_performance_summary.json",
    "three_way_intersection_analysis_v2.csv",
]

SELECTED_ROOT_FILES = [
    "release_manifest.json",
    "run_manifest.json",
    "stage_run_manifest.json",
    "release_lock.json",
]

STATIC_LOOKUP_FIELDS = [
    "postcode",
    "postcode_clean",
    "district",
    "authority_name",
    "region_name",
    "priority_zone",
    "intervention_type",
    "deprivation_category",
    "shap_narrative",
    "pattern_note",
    "brownsea_departure_terminal",
    "drive_to_departure_terminal_min",
    "access_route_mode",
    "chain_ferry_used",
    "chain_ferry_allowance_min",
    "brownsea_crossing_min",
    "total_brownsea_journey_min",
    "nearest_nt_site_name",
    "nearest_nt_site_drive_min",
    "brownsea_vs_nearest_nt_gap_min",
    "brownsea_accessibility_score",
    "imd_decile",
    "avg_fsm%",
    "district_visits_per_1000",
    "district_predicted_visit_rate",
    "district_model_gap_per_1000",
    "alternative_brownsea_departure_terminal",
]


def release_dir(outputs_root: Path, release_name: str) -> Path:
    return outputs_root / "releases" / release_name


def safe_clear_directory(path: Path) -> None:
    """Clear a target directory with basic guards against accidental damage."""
    resolved = path.resolve()
    if resolved == resolved.anchor or len(resolved.parts) < 3:
        raise ValueError(f"Refusing to clear unsafe target directory: {path}")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def copy_if_exists(src: Path, dst: Path) -> bool:
    if not src.exists() or not src.is_file():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def copy_tree_if_exists(src: Path, dst: Path) -> bool:
    if not src.exists() or not src.is_dir():
        return False
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    return True


def normalise_postcode(value: object) -> str:
    return re.sub(r"\s+", "", "" if value is None else str(value)).upper()


def postcode_shard_key(value: object) -> str | None:
    clean = normalise_postcode(value)
    if not clean:
        return None
    full_match = re.match(r"^([A-Z]{1,2}\d[A-Z\d]?)(\d[A-Z]{2})$", clean)
    if full_match:
        return full_match.group(1)
    outward_match = re.match(r"^([A-Z]{1,2}\d[A-Z\d]?)", clean)
    if outward_match:
        return outward_match.group(1)
    return None


def strip_large_or_missing_values(record: dict) -> dict:
    clean: dict = {}
    for field in STATIC_LOOKUP_FIELDS:
        value = record.get(field)
        if value != value:
            value = None
        clean[field] = value
    if not clean.get("postcode_clean"):
        clean["postcode_clean"] = normalise_postcode(clean.get("postcode"))
    return clean


def write_postcode_shards(records: list[dict], target: Path) -> tuple[Path, Path, dict[str, int]]:
    shards_dir = target / "artifacts" / "postcode_shards"
    if shards_dir.exists():
        shutil.rmtree(shards_dir)
    shards_dir.mkdir(parents=True, exist_ok=True)

    shards: dict[str, list[dict]] = {}
    for record in records:
        clean = strip_large_or_missing_values(record)
        key = postcode_shard_key(clean.get("postcode_clean") or clean.get("postcode"))
        if not key:
            continue
        shards.setdefault(key, []).append(clean)

    index: dict[str, int] = {}
    for key, shard_records in sorted(shards.items()):
        index[key] = len(shard_records)
        (shards_dir / f"{key}.json").write_text(
            json.dumps(shard_records, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
            encoding="utf-8",
        )

    index_path = target / "artifacts" / "postcode_shards_index.json"
    index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    return shards_dir, index_path, index


def iter_bundle_files(target: Path) -> Iterable[tuple[Path, Path]]:
    for root_name in ["reports", "artifacts"]:
        root = target / root_name
        if root.exists():
            for file_path in sorted(p for p in root.rglob("*") if p.is_file()):
                yield file_path, Path(root_name) / file_path.relative_to(root)
    for file_name in SELECTED_ROOT_FILES:
        file_path = target / file_name
        if file_path.exists():
            yield file_path, Path(file_name)


def write_reports_zip(target: Path) -> Path:
    zip_path = target / "reports.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path, arcname in iter_bundle_files(target):
            if file_path == zip_path:
                continue
            zf.write(file_path, arcname.as_posix())
    return zip_path


def file_size_label(path: Path) -> str:
    if not path.exists():
        return ""
    size = float(path.stat().st_size)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return ""


def write_static_downloads_page(target: Path) -> Path:
    items: list[tuple[str, str, str, Path]] = []

    candidates = [
        ("Postcode lookup", "Open the staff postcode search app.", "index.html", target / "index.html"),
        ("Reports ZIP", "Download the released reports and key audit files.", "reports.zip", target / "reports.zip"),
        ("Reports index", "Open the main report index.", "reports/index.html", target / "reports" / "index.html"),
        ("Postcode lookup CSV", "Spreadsheet-friendly postcode lookup results.", "artifacts/postcode_lookup.csv", target / "artifacts" / "postcode_lookup.csv"),
        ("Postcode lookup JSON", "Full machine-readable postcode lookup data.", "artifacts/postcode_lookup.json", target / "artifacts" / "postcode_lookup.json"),
        ("Static postcode shards", "Small postcode-district lookup files used by the static app.", "artifacts/postcode_shards_index.json", target / "artifacts" / "postcode_shards_index.json"),
        ("Model performance CSV", "Model ranking and performance metrics.", "artifacts/model_performance.csv", target / "artifacts" / "model_performance.csv"),
        ("Model performance summary", "Short machine-readable model performance summary.", "artifacts/model_performance_summary.json", target / "artifacts" / "model_performance_summary.json"),
        ("Three-way analysis CSV", "District-level analysis table used by the lookup.", "artifacts/three_way_intersection_analysis_v2.csv", target / "artifacts" / "three_way_intersection_analysis_v2.csv"),
        ("Release manifest", "Release file list and hashes.", "release_manifest.json", target / "release_manifest.json"),
        ("Run manifest", "Pipeline run metadata.", "run_manifest.json", target / "run_manifest.json"),
        ("Stage run manifest", "Stage execution metadata.", "stage_run_manifest.json", target / "stage_run_manifest.json"),
        ("Release lock", "Freeze record for the release candidate.", "release_lock.json", target / "release_lock.json"),
    ]
    for item in candidates:
        if item[3].exists():
            items.append(item)

    rows = "\n".join(
        f'''<div class="item"><div><div class="name">{name}</div><div class="size">{file_size_label(path)}</div></div><div class="description">{description}</div><div><a class="button" href="{href}">Open / Download</a></div></div>'''
        for name, description, href, path in items
    ) or '<div class="description">No downloadable files were found.</div>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Brownsea Reports & Downloads</title>
<style>
:root {{ --bg:#f7fafc; --card:#fff; --ink:#1f2937; --muted:#6b7280; --line:#e5e7eb; --brand:#0f4c5c; --brand2:#2c7a7b; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:Arial,sans-serif; color:var(--ink); background:var(--bg); }}
header {{ background:linear-gradient(135deg,var(--brand),var(--brand2)); color:#fff; padding:28px 24px; }}
main {{ max-width:1100px; margin:0 auto; padding:24px; }}
a {{ color:#0f4c5c; }} header a {{ color:#fff; }}
.links {{ display:flex; gap:16px; flex-wrap:wrap; margin-top:14px; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:18px; margin-top:18px; box-shadow:0 4px 12px rgba(0,0,0,.04); }}
.item {{ display:grid; grid-template-columns:minmax(220px,1fr) 2fr auto; gap:16px; align-items:start; padding:12px 0; border-top:1px dashed var(--line); }}
.item:first-child {{ border-top:0; }}
.name {{ font-weight:700; }} .description, .size {{ color:var(--muted); font-size:13px; }}
.button {{ display:inline-block; padding:8px 12px; border-radius:10px; background:#111827; color:#fff; text-decoration:none; white-space:nowrap; }}
@media (max-width:760px){{ .item {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<header>
  <h1 style="margin:0;">Reports & Downloads</h1>
  <p style="max-width:780px; margin:8px 0 0;">Open or download the released Brownsea visitor access reports and app files.</p>
  <div class="links"><a href="index.html">Back to postcode lookup</a><a href="help.html">Help</a><a href="reports/index.html">Reports index</a></div>
</header>
<main><div class="card">{rows}</div></main>
</body>
</html>"""
    path = target / "downloads.html"
    path.write_text(html, encoding="utf-8")
    return path


def static_postcode_app_html(
    *,
    title: str,
    generated_at: str,
    records_count: int,
    shards_base: str,
    downloads_page: str,
    help_page: str,
    reports_index: str,
    download_csv: str,
    download_json: str,
) -> str:
    template = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>__TITLE__</title>
<style>
:root { --bg: #f6f8fb; --card: #ffffff; --ink: #1f2937; --muted: #6b7280; --line: #e5e7eb; --brand: #0f4c5c; --brand2: #2c7a7b; }
* { box-sizing: border-box; }
body { margin: 0; font-family: Arial, sans-serif; background: var(--bg); color: var(--ink); }
header { background: linear-gradient(135deg, var(--brand), var(--brand2)); color: white; padding: 28px 24px; }
main { max-width: 1100px; margin: 0 auto; padding: 24px; }
.hero p { margin: 8px 0 0; max-width: 780px; opacity: 0.95; }
.searchbar { display:flex; gap:10px; flex-wrap: wrap; margin-top: 18px; }
input { flex: 1; min-width: 260px; padding: 14px 16px; font-size: 16px; border: 1px solid var(--line); border-radius: 12px; }
button { padding: 14px 18px; font-size: 16px; border: 0; border-radius: 12px; background: #111827; color: white; cursor: pointer; }
button:hover { opacity: .95; }
.links { margin-top: 14px; display:flex; gap:16px; flex-wrap: wrap; }
.links a { color: white; text-decoration: underline; }
.card { background: var(--card); border: 1px solid var(--line); border-radius: 16px; padding: 18px; margin-top: 18px; box-shadow: 0 4px 12px rgba(0,0,0,.04); }
.grid { display:grid; grid-template-columns: repeat(2, minmax(250px, 1fr)); gap: 14px 18px; }
@media (max-width: 760px) { .grid { grid-template-columns: 1fr; } }
.section-title { margin: 0 0 12px; font-size: 15px; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }
.kv { padding-bottom: 8px; border-bottom: 1px dashed var(--line); }
.label { font-size: 12px; color: var(--muted); text-transform: uppercase; margin-bottom: 4px; }
.value { font-size: 16px; font-weight: 600; }
.small { color: var(--muted); font-size: 13px; }
.note { margin-bottom: 12px; line-height: 1.4; }
.tag { display:inline-block; padding: 5px 10px; border-radius: 999px; background: #eef2ff; color: #3730a3; font-size: 12px; margin-right: 8px; margin-bottom: 8px; }
.banner { display:flex; gap:10px; flex-wrap: wrap; margin-top: 10px; }
.notfound { background:#fff7ed; border-color:#fed7aa; }
.footer { margin-top: 24px; color: var(--muted); font-size: 12px; }
</style>
</head>
<body>
<header>
  <div class="hero">
    <h1 style="margin:0;">__TITLE__</h1>
    <p>Search a BH, DT, or SP postcode to see Brownsea access, competitor context, local context, and district-level assessment.</p>
    <div class="searchbar">
      <input id="query" placeholder="Enter postcode, e.g. BH2 5NP" />
      <button id="search-button" onclick="searchPostcode()">Search</button>
    </div>
    <div class="links">
      <a href="__DOWNLOADS_PAGE__">Reports & Downloads</a>
      <a href="__HELP_PAGE__">Help</a>
      <a href="__REPORTS_INDEX__">Reports index</a>
      <a href="__DOWNLOAD_CSV__">Download postcode CSV</a>
      <a href="__DOWNLOAD_JSON__">Download postcode JSON</a>
    </div>
  </div>
</header>
<main>
  <div class="card small">Generated: __GENERATED_AT__ &middot; Records: __RECORDS_COUNT__<br><span id="load-status">Ready. Enter a postcode to search.</span></div>
  <div id="result"></div>
  <div class="footer">This lookup uses precomputed pipeline outputs. It is intended as a decision-support tool, not live journey planning.</div>
</main>
<script>
const SHARDS_BASE = '__SHARDS_BASE__';
const SHARD_CACHE = {};
function setLookupStatus(message) { const el = document.getElementById('load-status'); if (el) el.textContent = message; }
function normalise(v){ return (v||'').toString().replace(/\s+/g,'').toUpperCase(); }
function shardKey(q) {
  const clean = normalise(q);
  const full = clean.match(/^([A-Z]{1,2}\d[A-Z\d]?)(\d[A-Z]{2})$/);
  if (full) return full[1];
  const outward = clean.match(/^([A-Z]{1,2}\d[A-Z\d]?)/);
  return outward ? outward[1] : null;
}
async function loadShard(key) {
  if (!key) return [];
  if (SHARD_CACHE[key]) return SHARD_CACHE[key];
  setLookupStatus(`Loading postcode district ${key}...`);
  const url = `${SHARDS_BASE}/${encodeURIComponent(key)}.json`;
  const response = await fetch(url, { cache: 'force-cache' });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const records = await response.json();
  SHARD_CACHE[key] = Array.isArray(records) ? records : [];
  setLookupStatus(`Loaded postcode district ${key}: ${SHARD_CACHE[key].length.toLocaleString()} records`);
  return SHARD_CACHE[key];
}
function titleCaseText(v){ if(v===null || v===undefined || v==='') return 'Not available'; return String(v).replace(/_/g,' ').replace(/\s+/g,' ').trim().replace(/\b\w/g, c => c.toUpperCase()); }
function rawText(v){ return (v===null||v===undefined||v==='') ? 'Not available' : String(v); }
function numberValue(v){ if(v===null || v===undefined || v==='') return null; const n = Number(v); return Number.isFinite(n) ? n : null; }
function fmtNumber(v, digits=1){ const n = numberValue(v); if(n===null) return 'Not available'; return n.toLocaleString(undefined, {minimumFractionDigits: digits, maximumFractionDigits: digits}); }
function fmtMinutes(v){ return fmtNumber(v, 1); }
function fmtRate(v){ return fmtNumber(v, 1); }
function fmtPercent(v){ const n = numberValue(v); if(n===null) return 'Not available'; return `${n.toLocaleString(undefined, {minimumFractionDigits: 1, maximumFractionDigits: 1})}%`; }
function fmtCategory(v){ return titleCaseText(v); }
function fmtPlain(v){ return rawText(v); }
function isBrownseaSite(v){ return /brownsea/i.test(rawText(v)); }
function competitorName(row){ const name = row.nearest_nt_site_name; if(isBrownseaSite(name)) return 'No competing NT site identified'; return fmtPlain(name); }
function competitorMetric(row, field){ if(isBrownseaSite(row.nearest_nt_site_name)) return null; return row[field]; }
function kv(label,value,kind='plain'){
  let shown;
  if(kind==='category') shown = fmtCategory(value);
  else if(kind==='number') shown = fmtNumber(value, 1);
  else if(kind==='rate') shown = fmtRate(value);
  else if(kind==='minutes') shown = fmtMinutes(value);
  else if(kind==='percent') shown = fmtPercent(value);
  else shown = fmtPlain(value);
  return `<div class="kv"><div class="label">${label}</div><div class="value">${shown}</div></div>`;
}
function section(title, items, note=''){
  const noteHtml = note ? `<div class="small note">${note}</div>` : '';
  return `<div class="card"><div class="section-title">${title}</div>${noteHtml}<div class="grid">${items.join('')}</div></div>`;
}
function isBrownseaDestinationPostcode(q){ return q === 'BH137EE'; }
function destinationPostcodeCard(){ return `<div class="card notfound"><h2 style="margin:0 0 8px;">Brownsea Island destination postcode</h2><p style="margin:0 0 10px;">BH13 7EE is Brownsea Island's destination postcode. This tool estimates visitor access from mainland origin postcodes.</p><div class="small">Please enter a visitor origin postcode instead, for example a BH, DT, or SP residential postcode.</div></div>`; }
function modelGap(row){ const explicit = numberValue(row.district_model_gap_per_1000); if(explicit !== null) return explicit; const predicted = numberValue(row.district_predicted_visit_rate); const observed = numberValue(row.district_visits_per_1000); if(predicted === null || observed === null) return null; return predicted - observed; }
function performanceAgainstExpectation(row){ const gap = modelGap(row); if(gap === null) return 'Not available'; const absGap = Math.abs(gap).toLocaleString(undefined, {minimumFractionDigits: 1, maximumFractionDigits: 1}); if(Math.abs(gap) < 0.05) return 'In line with expected'; if(gap > 0) return `${absGap} visits per 1,000 below expected`; return `${absGap} visits per 1,000 above expected`; }
function accessPosition(row){ if(isBrownseaSite(row.nearest_nt_site_name)) return 'No competing NT site identified'; const gap = numberValue(row.brownsea_vs_nearest_nt_gap_min); if(gap === null) return 'Not available'; const absGap = Math.abs(gap).toLocaleString(undefined, {minimumFractionDigits: 1, maximumFractionDigits: 1}); if(Math.abs(gap) < 0.05) return 'Brownsea journey is broadly similar to the nearest competing NT site'; if(gap > 0) return `Brownsea journey is ${absGap} minutes longer than the nearest competing NT site`; return `Brownsea journey is ${absGap} minutes shorter than the nearest competing NT site`; }
function boolValue(v){ if(v===true) return true; if(v===false || v===null || v===undefined) return false; return /^(true|1|yes)$/i.test(String(v)); }
function travelNote(row){ if(boolValue(row.chain_ferry_used)) { const allowance = fmtMinutes(row.chain_ferry_allowance_min); return `Includes a Sandbanks chain ferry allowance of ${allowance} minutes before the Brownsea ferry crossing. Live traffic and ferry waiting times are not included.`; } return 'Journey times are planning estimates and do not include live traffic or live ferry waiting times.'; }
function needLevel(row){ const decile = numberValue(row.imd_decile); if(decile === null) return 'Not available'; if(decile <= 3) return 'High local need'; if(decile <= 6) return 'Moderate local need'; return 'Lower local need'; }
function suggestedNextAction(row){ const zone = fmtCategory(row.priority_zone).toLowerCase(); const intervention = fmtCategory(row.intervention_type).toLowerCase(); if(zone.includes('urgent') || intervention.includes('crisis')) return 'Prioritise outreach and access support'; if(zone.includes('high') || intervention.includes('targeted')) return 'Use targeted outreach and monitor response'; if(zone.includes('growth')) return 'Test awareness and conversion activity'; if(zone.includes('monitor')) return 'Monitor engagement before intervention'; if(zone.includes('maintain') || intervention.includes('model')) return 'Maintain current engagement approach'; return 'Review alongside local operational context'; }
function assessmentReason(row){ const gap = modelGap(row); const need = needLevel(row).toLowerCase(); const accessGap = numberValue(row.brownsea_vs_nearest_nt_gap_min); if(gap !== null && gap > 0.25 && need.includes('high')) return 'Low observed engagement relative to expectation in a high-need district'; if(gap !== null && gap > 0.25) return 'Observed engagement is below model expectation'; if(gap !== null && gap < -0.25) return 'Observed engagement is above model expectation'; if(accessGap !== null && accessGap > 1) return 'Brownsea journey is slower than the nearest competing NT site'; return 'Observed engagement is broadly in line with model expectation'; }
function patternNote(row){ if(row.pattern_note && rawText(row.pattern_note) !== 'Not available') return rawText(row.pattern_note); const raw = rawText(row.shap_narrative); if(/fragility|model sensitivity|less typical/i.test(raw)) return 'Visitor pattern is less typical than similar districts'; return 'Not available'; }
function cleanNarrative(text){
  let raw = rawText(text);
  if(raw === 'Not available') return raw;
  raw = raw.replace(/^Narrative:\s*/i, '');
  raw = raw.replace(/\[[^\]]+\]/g, '').trim();
  raw = raw.replace(/(?<!Engagement )\bStatus\s*:/gi, 'Engagement status:');
  const replacements = [
    [/\bExceeding Target\b/gi, 'Above expected'], [/\bBelow Target\b/gi, 'Below expected'], [/\bOn Target\b/gi, 'In line with expected'],
    [/\bPrimary Barriers\s*:/gi, 'Main barriers:'], [/\bPositive Drivers\s*:/gi, 'Positive factors:'],
    [/\bDrive Time to Competitor NT Site\b/gi, 'drive time to nearest NT site'], [/\bDrive Time\b/gi, 'drive time'], [/\bTravel Time\b/gi, 'journey time'],
    [/\bFerry Duration\b/gi, 'ferry crossing time'], [/\bLocal Income Levels\b/gi, 'local income context'], [/\bOverall Deprivation\b/gi, 'deprivation level'],
    [/\bGeographic Isolation\b/gi, 'geographic access barriers'], [/\bSystemic Barriers\b/gi, 'wider access barriers'], [/\bLogistical Accessibility\b/gi, 'accessibility'],
    [/\bModel Sensitivity\b/gi, 'pattern note'], [/\bFragility\b/gi, 'visitor pattern']
  ];
  replacements.forEach(([pattern, replacement]) => { raw = raw.replace(pattern, replacement); });
  raw = raw.replace(/\b(?:Engagement\s+)+(?:Engagement\s+)*status\s*:/gi, 'Engagement status:');
  raw = raw.replace(/\s*\|\s*/g, '. ');
  raw = raw.replace(/\s+/g, ' ').replace(/\s+([.,;:])/g, '$1').replace(/[ .]+$/g, '').trim();
  return raw || 'Not available';
}
function findRow(records, q) { const exact = records.find(r => normalise(r.postcode_clean || r.postcode) === q); if (exact) return exact; return records.find(r => (normalise(r.postcode_clean || r.postcode) || '').startsWith(q)) || null; }
async function searchPostcode() {
  const q = normalise(document.getElementById('query').value);
  const out = document.getElementById('result');
  if (!q) { out.innerHTML = '<div class="card notfound">Enter a postcode to search.</div>'; return; }
  if (isBrownseaDestinationPostcode(q)) { out.innerHTML = destinationPostcodeCard(); return; }
  const key = shardKey(q);
  if (!key) { out.innerHTML = '<div class="card notfound">Enter a valid BH, DT, or SP postcode.</div>'; return; }
  let records;
  try { records = await loadShard(key); }
  catch (error) { out.innerHTML = `<div class="card notfound"><strong>No lookup data found for ${key}.</strong><div class="small">Check that artifacts/postcode_shards/${key}.json exists in the static app export.</div></div>`; setLookupStatus(`No lookup data found for ${key}`); return; }
  const row = findRow(records, q);
  if (!row) { out.innerHTML = `<div class="card notfound"><strong>No matching postcode found.</strong><div class="small">Try a full visitor-origin postcode in ${key}.</div></div>`; return; }
  const chips = `<div class="banner"><span class="tag">Priority: ${fmtCategory(row.priority_zone)}</span><span class="tag">Action type: ${fmtCategory(row.intervention_type)}</span><span class="tag">Deprivation: ${fmtCategory(row.deprivation_category)}</span></div>`;
  out.innerHTML = `
    <div class="card"><h2 style="margin:0 0 6px;">${fmtPlain(row.postcode)}</h2><div class="small">District ${fmtPlain(row.district)} &middot; Authority ${fmtPlain(row.authority_name)} &middot; Region ${fmtPlain(row.region_name)}</div>${chips}<div class="small" style="margin-top:12px;"><strong>Summary:</strong> ${cleanNarrative(row.shap_narrative)}</div><div class="small" style="margin-top:6px;"><strong>Pattern note:</strong> ${patternNote(row)}</div></div>
    ${section('Brownsea access', [kv('Departure terminal', row.brownsea_departure_terminal), kv('Journey to departure terminal (minutes)', row.drive_to_departure_terminal_min, 'minutes'), kv('Access route', row.access_route_mode), kv('Ferry crossing time (minutes)', row.brownsea_crossing_min, 'minutes'), kv('Total Brownsea journey (minutes)', row.total_brownsea_journey_min, 'minutes')], travelNote(row))}
    ${section('Nearest competing NT site', [kv('Nearest NT site', competitorName(row)), kv('Nearest NT site drive time (minutes)', competitorMetric(row, 'nearest_nt_site_drive_min'), 'minutes'), kv('Access position', accessPosition(row)), kv('Brownsea accessibility score', row.brownsea_accessibility_score, 'number')])}
    ${section('Local context', [kv('IMD decile', row.imd_decile, 'number'), kv('Need level', needLevel(row)), kv('Deprivation category', row.deprivation_category, 'category'), kv('Average FSM percentage', row['avg_fsm%'], 'percent'), kv('Observed district visits per 1000', row.district_visits_per_1000, 'rate')])}
    ${section('District-Level Assessment', [kv('Observed visits per 1000', row.district_visits_per_1000, 'rate'), kv('Model expected visits per 1000', row.district_predicted_visit_rate, 'rate'), kv('Performance against expectation', performanceAgainstExpectation(row)), kv('Priority zone', row.priority_zone, 'category'), kv('Suggested next action', suggestedNextAction(row)), kv('Assessment reason', assessmentReason(row)), kv('Action type', row.intervention_type, 'category'), kv('Alternative departure terminal', row.alternative_brownsea_departure_terminal)])}`;
}
function runHashSearch(){ if (location.hash.startsWith('#postcode=')) { const value = decodeURIComponent(location.hash.replace('#postcode=', '')); document.getElementById('query').value = value; searchPostcode(); } }
document.getElementById('query').addEventListener('keydown', e => { if (e.key === 'Enter') searchPostcode(); });
runHashSearch();
</script>
</body>
</html>'''
    return (
        template.replace("__TITLE__", title)
        .replace("__GENERATED_AT__", generated_at)
        .replace("__RECORDS_COUNT__", f"{records_count:,}")
        .replace("__SHARDS_BASE__", shards_base.rstrip("/"))
        .replace("__DOWNLOADS_PAGE__", downloads_page)
        .replace("__HELP_PAGE__", help_page)
        .replace("__REPORTS_INDEX__", reports_index)
        .replace("__DOWNLOAD_CSV__", download_csv)
        .replace("__DOWNLOAD_JSON__", download_json)
    )


def write_static_postcode_app_html(
    output_path: Path,
    *,
    title: str,
    generated_at: str,
    records_count: int,
    shards_base: str,
    downloads_page: str,
    help_page: str,
    reports_index: str,
    download_csv: str,
    download_json: str,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        static_postcode_app_html(
            title=title,
            generated_at=generated_at,
            records_count=records_count,
            shards_base=shards_base,
            downloads_page=downloads_page,
            help_page=help_page,
            reports_index=reports_index,
            download_csv=download_csv,
            download_json=download_json,
        ),
        encoding="utf-8",
    )
    return output_path


def write_deploy_readme(target: Path, release_name: str, shard_count: int = 0) -> Path:
    text = f"""# Brownsea staff static app

This folder is a static export of the Brownsea Visitor Access Tool for release `{release_name}`.

It can be published with GitHub Pages, Netlify, or any simple static web host. It does not need Flask, Python, Colab, or a server once exported.

## Main files

- `index.html` - staff postcode lookup app
- `downloads.html` - reports and download links
- `reports/` - released HTML reports and plots
- `artifacts/postcode_shards/` - small postcode-district lookup files used by the app
- `artifacts/` - released CSV/JSON files
- `reports.zip` - downloadable bundle of reports and key audit files

The app loads only the small postcode-district shard needed for the user's search, rather than loading the full postcode lookup file on page open.

Shard files exported: {shard_count}

## GitHub Pages

1. Commit this folder as `docs/` in the repository.
2. In GitHub, go to Settings > Pages.
3. Choose `Deploy from a branch`.
4. Select branch `main` and folder `/docs`.
5. Save and use the GitHub Pages URL.

Generated at: {datetime.now(timezone.utc).isoformat()}
"""
    path = target / "README_STAFF_APP.md"
    path.write_text(text, encoding="utf-8")
    return path


def export_static_staff_app(
    outputs_root: Path,
    release_name: str = "latest",
    target: Path = Path("docs"),
    clean: bool = True,
) -> list[Path]:
    release = release_dir(outputs_root, release_name)
    artifacts = release / "artifacts"
    reports = release / "reports"
    lookup_json = artifacts / "postcode_lookup.json"
    if not release.exists():
        raise FileNotFoundError(f"Release not found: {release}")
    if not lookup_json.exists():
        raise FileNotFoundError(f"Missing required lookup JSON: {lookup_json}")

    if clean:
        safe_clear_directory(target)
    else:
        target.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    if copy_tree_if_exists(reports, target / "reports"):
        written.append(target / "reports")

    (target / "artifacts").mkdir(parents=True, exist_ok=True)
    for name in SELECTED_ARTIFACTS:
        if copy_if_exists(artifacts / name, target / "artifacts" / name):
            written.append(target / "artifacts" / name)

    for name in SELECTED_ROOT_FILES:
        if copy_if_exists(release / name, target / name):
            written.append(target / name)

    records = json.loads(lookup_json.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError("postcode_lookup.json must contain a list of records")

    shards_dir, shard_index_path, shard_index = write_postcode_shards(records, target)
    written.extend([shards_dir, shard_index_path])

    written.append(
        write_static_postcode_app_html(
            target / "index.html",
            title="Brownsea Visitor Access Tool",
            generated_at=f"static export from release {release_name}",
            records_count=len(records),
            shards_base="artifacts/postcode_shards",
            downloads_page="downloads.html",
            help_page="help.html",
            reports_index="reports/index.html",
            download_csv="artifacts/postcode_lookup.csv",
            download_json="artifacts/postcode_lookup.json",
        )
    )

    (target / "reports").mkdir(parents=True, exist_ok=True)
    for name in ["postcode_app.html", "postcode_lookup.html"]:
        written.append(
            write_static_postcode_app_html(
                target / "reports" / name,
                title="Brownsea Visitor Access Tool",
                generated_at=f"From {release_name} release.",
                records_count=len(records),
                shards_base="../artifacts/postcode_shards",
                downloads_page="../downloads.html",
                help_page="../help.html",
                reports_index="index.html",
                download_csv="../artifacts/postcode_lookup.csv",
                download_json="../artifacts/postcode_lookup.json",
            )
        )

    zip_path = write_reports_zip(target)
    written.append(zip_path)
    written.append(write_static_downloads_page(target))
    written.append(write_help_html(target / "help.html", home_href="index.html", downloads_href="downloads.html", reports_href="reports/index.html"))
    written.append(write_deploy_readme(target, release_name, shard_count=len(shard_index)))
    (target / ".nojekyll").write_text("", encoding="utf-8")
    written.append(target / ".nojekyll")
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a completed release as a static staff web app for GitHub Pages")
    parser.add_argument("outputs_root", help="Outputs root containing releases/, for example /content/drive/MyDrive/brownsea/outputs")
    parser.add_argument("--release-name", default="latest")
    parser.add_argument("--target", default="docs", help="Target static site folder. Use docs for GitHub Pages.")
    parser.add_argument("--no-clean", action="store_true", help="Do not clear the target folder before export")
    args = parser.parse_args()

    try:
        written = export_static_staff_app(
            Path(args.outputs_root),
            release_name=args.release_name,
            target=Path(args.target),
            clean=not args.no_clean,
        )
    except Exception as exc:
        print("Static staff app export: FAIL")
        print(f"  reason: {exc}")
        return 1

    print("Static staff app export: PASS")
    print(f"  target: {Path(args.target).resolve()}")
    for path in written:
        print(f"  wrote: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
