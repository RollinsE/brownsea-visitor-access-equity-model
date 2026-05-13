# -*- coding: utf-8 -*-
"""Static web UI builders for postcode lookup artifacts."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict

import pandas as pd

from src.help_page import write_help_html


def _normalise_engagement_text(raw: str) -> str:
    """Return a concise, plain-language engagement narrative.

    The function is deliberately idempotent because refresh scripts may read
    already-sanitised release artifacts and write them back again.
    """
    raw = "" if raw is None else str(raw)
    if not raw:
        return "Not available"
    raw = re.sub(r"^Narrative:\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\[\s*(High|Medium|Low)\s+Fragility\s*:?\s*[^\]]*\]", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\[\s*(High|Medium|Low)\s+Model\s+Sensitivity\s*:?\s*[^\]]*\]", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\[\s*Less typical visitor pattern\s*\]", "", raw, flags=re.IGNORECASE).strip()

    replacements = [
        (r"(?<!Engagement )\bStatus\s*:", "Engagement status:"),
        (r"\bBelow Target\b", "Below expected"),
        (r"\bExceeding Target\b", "Above expected"),
        (r"\bOn Target\b", "In line with expected"),
        (r"\bPrimary Barriers\s*:", "Main barriers:"),
        (r"\bPositive Drivers\s*:", "Positive factors:"),
        (r"\bDrive Time to Competitor NT Site\b", "drive time to nearest NT site"),
        (r"\bBrownsea journey time\b", "Brownsea journey time"),
        (r"\bDrive Time\b", "drive time"),
        (r"\bTravel Time\b", "journey time"),
        (r"\bFerry Duration\b", "ferry crossing time"),
        (r"\bLocal Income Levels\b", "local income context"),
        (r"\bOverall Deprivation\b", "deprivation level"),
        (r"\bGeographic Isolation\b", "geographic access barriers"),
        (r"\bSystemic Barriers\b", "wider access barriers"),
        (r"\bLogistical Accessibility\b", "accessibility"),
        (r"\bHigh Deprivation\b", "high deprivation"),
        (r"\bModerate Deprivation\b", "moderate deprivation"),
        (r"\bAffluence levels\b", "lower deprivation"),
        (r"\bFSM/Poverty Rates\b", "FSM and poverty context"),
        (r"\bModel Sensitivity\b", "pattern note"),
        (r"\bFragility\b", "visitor pattern"),
    ]
    for pattern, replacement in replacements:
        raw = re.sub(pattern, replacement, raw, flags=re.IGNORECASE)

    raw = re.sub(r"\b(?:Engagement\s+)+(?:Engagement\s+)*status\s*:", "Engagement status:", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*\|\s*", ". ", raw)
    raw = re.sub(r"\s+", " ", raw)
    raw = re.sub(r"\s+([.,;:])", r"\1", raw)
    raw = raw.strip(" .")
    return raw or "Not available"


def _pattern_note_from_narrative(value: object) -> str:
    raw = "" if value is None else str(value)
    if re.search(r"Less typical visitor pattern", raw, flags=re.IGNORECASE):
        return "Visitor pattern is less typical than similar districts"
    match = re.search(r"\[\s*(High|Medium|Low)\s+(?:Fragility|Model\s+Sensitivity)\s*:?\s*[^\]]*\]", raw, flags=re.IGNORECASE)
    if not match:
        return "Not available"
    level = match.group(1).lower()
    if level == "high":
        return "Visitor pattern is less typical than similar districts"
    if level == "medium":
        return "Visitor pattern varies somewhat compared with similar districts"
    return "Visitor pattern is broadly typical of similar districts"


def _plain_narrative(value: object) -> str:
    return _normalise_engagement_text("" if value is None else str(value))

def _hide_brownsea_competitor_values(df: pd.DataFrame) -> pd.DataFrame:
    """Prevent Brownsea itself being displayed as a competing NT site."""
    if df is None or df.empty or "nearest_nt_site_name" not in df.columns:
        return df

    clean = df.copy()
    mask = clean["nearest_nt_site_name"].astype(str).str.contains("brownsea", case=False, na=False)
    if mask.any():
        clean.loc[mask, "nearest_nt_site_name"] = "No competing NT site identified"
        for col in ["nearest_nt_site_drive_min", "brownsea_vs_nearest_nt_gap_min"]:
            if col in clean.columns:
                clean.loc[mask, col] = pd.NA

    if "shap_narrative" in clean.columns:
        clean["pattern_note"] = clean["shap_narrative"].map(_pattern_note_from_narrative)
        clean["shap_narrative"] = clean["shap_narrative"].map(_plain_narrative)

    return clean


def _records_for_json(df: pd.DataFrame) -> list[dict]:
    # pandas.to_json converts NaN/NaT to JSON null and avoids invalid JavaScript NaN tokens.
    display_df = _hide_brownsea_competitor_values(df)
    return json.loads(display_df.to_json(orient="records", date_format="iso"))


def build_postcode_json(lookup_df: pd.DataFrame, output_path: Path) -> str:
    payload = _records_for_json(lookup_df)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    return str(output_path)


def build_postcode_app_html(lookup_df: pd.DataFrame, output_path: Path, metadata: Dict | None = None) -> str:
    metadata = metadata or {}
    records = _records_for_json(lookup_df)
    title = metadata.get("title", "Brownsea Visitor Opportunity Lookup")
    generated_at = metadata.get("generated_at", "")
    download_csv = metadata.get("download_csv", "../artifacts/postcode_lookup.csv")
    download_json = metadata.get("download_json", "../artifacts/postcode_lookup.json")
    reports_index = metadata.get("reports_index", "index.html")
    downloads_page = metadata.get("downloads_page", "downloads.html")
    help_page = metadata.get("help_page", "help.html")
    external_data = bool(metadata.get("external_data", False))
    data_url = metadata.get("data_url", download_json)

    if external_data:
        data_script = f'''let DATA = [];
let DATA_READY = false;
const DATA_URL = {json.dumps(data_url)};
function setLookupStatus(message) {{
  const el = document.getElementById('load-status');
  if (el) el.textContent = message;
}}
function setSearchEnabled(enabled) {{
  const button = document.getElementById('search-button');
  if (button) button.disabled = !enabled;
}}
setSearchEnabled(false);
fetch(DATA_URL)
  .then(response => {{
    if (!response.ok) throw new Error(`HTTP ${{response.status}}`);
    return response.json();
  }})
  .then(records => {{
    DATA = Array.isArray(records) ? records : [];
    DATA_READY = true;
    setSearchEnabled(true);
    setLookupStatus(`Lookup data loaded: ${{DATA.length.toLocaleString()}} records`);
    runHashSearch();
  }})
  .catch(error => {{
    DATA_READY = false;
    setSearchEnabled(false);
    setLookupStatus('Lookup data could not be loaded. Use the downloads page or refresh the page.');
    const out = document.getElementById('result');
    if (out) out.innerHTML = `<div class=\"card notfound\"><strong>Lookup data could not be loaded.</strong><div class=\"small\">The static app could not read ${{DATA_URL}}. Check that artifacts/postcode_lookup.json is present.</div></div>`;
    console.error('Failed to load postcode lookup data', error);
  }});'''
    else:
        data_script = "const DATA = " + json.dumps(records) + ";\nconst DATA_READY = true;\nfunction setLookupStatus(message) { const el = document.getElementById('load-status'); if (el) el.textContent = message; }\nfunction setSearchEnabled(enabled) { const button = document.getElementById('search-button'); if (button) button.disabled = !enabled; }\nsetLookupStatus(`Lookup data loaded: ${DATA.length.toLocaleString()} records`);"

    html = rf"""<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\" />
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
<title>{title}</title>
<style>
:root {{
  --bg: #f6f8fb; --card: #ffffff; --ink: #1f2937; --muted: #6b7280; --line: #e5e7eb;
  --brand: #0f4c5c; --brand2: #2c7a7b; --good: #0f766e; --warn: #b45309;
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; font-family: Arial, sans-serif; background: var(--bg); color: var(--ink); }}
header {{ background: linear-gradient(135deg, var(--brand), var(--brand2)); color: white; padding: 28px 24px; }}
main {{ max-width: 1100px; margin: 0 auto; padding: 24px; }}
.hero p {{ margin: 8px 0 0; max-width: 780px; opacity: 0.95; }}
.searchbar {{ display:flex; gap:10px; flex-wrap: wrap; margin-top: 18px; }}
input {{ flex: 1; min-width: 260px; padding: 14px 16px; font-size: 16px; border: 1px solid var(--line); border-radius: 12px; }}
button {{ padding: 14px 18px; font-size: 16px; border: 0; border-radius: 12px; background: #111827; color: white; cursor: pointer; }}
button:hover {{ opacity: .95; }}
.links {{ margin-top: 14px; display:flex; gap:16px; flex-wrap: wrap; }}
.links a {{ color: white; text-decoration: underline; }}
.card {{ background: var(--card); border: 1px solid var(--line); border-radius: 16px; padding: 18px; margin-top: 18px; box-shadow: 0 4px 12px rgba(0,0,0,.04); }}
.grid {{ display:grid; grid-template-columns: repeat(2, minmax(250px, 1fr)); gap: 14px 18px; }}
@media (max-width: 760px) {{ .grid {{ grid-template-columns: 1fr; }} }}
.section-title {{ margin: 0 0 12px; font-size: 15px; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }}
.kv {{ padding-bottom: 8px; border-bottom: 1px dashed var(--line); }}
.label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; margin-bottom: 4px; }}
.value {{ font-size: 16px; font-weight: 600; }}
.small {{ color: var(--muted); font-size: 13px; }}
.note {{ margin-bottom: 12px; line-height: 1.4; }}
.tag {{ display:inline-block; padding: 5px 10px; border-radius: 999px; background: #eef2ff; color: #3730a3; font-size: 12px; margin-right: 8px; margin-bottom: 8px; }}
.banner {{ display:flex; gap:10px; flex-wrap: wrap; margin-top: 10px; }}
.notfound {{ background:#fff7ed; border-color:#fed7aa; }}
.footer {{ margin-top: 24px; color: var(--muted); font-size: 12px; }}
</style>
</head>
<body>
<header>
  <div class=\"hero\">
    <h1 style=\"margin:0;\">{title}</h1>
    <p>Search a BH, DT, or SP postcode to see Brownsea access, competitor context, local context, and district-level assessment.</p>
    <div class=\"searchbar\">
      <input id=\"query\" placeholder=\"Enter postcode, e.g. BH13 7EE\" />
      <button id=\"search-button\" onclick=\"searchPostcode()\">Search</button>
    </div>
    <div class=\"links\">
      <a href=\"{downloads_page}\">Reports & Downloads</a>
      <a href=\"{help_page}\">Help</a>
      <a href=\"{reports_index}\">Reports index</a>
      <a href=\"{download_csv}\">Download postcode CSV</a>
      <a href=\"{download_json}\">Download postcode JSON</a>
    </div>
  </div>
</header>
<main>
  <div class=\"card small\">Generated: {generated_at} &middot; Records: {len(records):,}<br><span id=\"load-status\">Loading lookup data...</span></div>
  <div id=\"result\"></div>
  <div class=\"footer\">This lookup uses precomputed pipeline outputs. It is intended as a decision-support tool, not live journey planning.</div>
</main>
<script>
{data_script}
function normalise(v){{ return (v||'').toString().replace(/\s+/g,'').toUpperCase(); }}
function titleCaseText(v){{
  if(v===null || v===undefined || v==='') return 'Not available';
  return String(v).replace(/_/g,' ').replace(/\s+/g,' ').trim().replace(/\b\w/g, c => c.toUpperCase());
}}
function rawText(v){{ return (v===null||v===undefined||v==='') ? 'Not available' : String(v); }}
function numberValue(v){{
  if(v===null || v===undefined || v==='') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}}
function fmtNumber(v, digits=1){{
  const n = numberValue(v);
  if(n===null) return 'Not available';
  return n.toLocaleString(undefined, {{minimumFractionDigits: digits, maximumFractionDigits: digits}});
}}
function fmtMinutes(v){{ return fmtNumber(v, 1); }}
function fmtRate(v){{ return fmtNumber(v, 1); }}
function fmtPercent(v){{
  const n = numberValue(v);
  if(n===null) return 'Not available';
  return `${{n.toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}})}}%`;
}}
function fmtCategory(v){{ return titleCaseText(v); }}
function fmtPlain(v){{ return rawText(v); }}
function isBrownseaSite(v){{ return /brownsea/i.test(rawText(v)); }}
function competitorName(row){{
  const name = row.nearest_nt_site_name;
  if(isBrownseaSite(name)) return 'No competing NT site identified';
  return fmtPlain(name);
}}
function competitorMetric(row, field){{
  if(isBrownseaSite(row.nearest_nt_site_name)) return null;
  return row[field];
}}
function kv(label,value,kind='plain'){{
  let shown;
  if(kind==='category') shown = fmtCategory(value);
  else if(kind==='number') shown = fmtNumber(value, 1);
  else if(kind==='rate') shown = fmtRate(value);
  else if(kind==='minutes') shown = fmtMinutes(value);
  else if(kind==='percent') shown = fmtPercent(value);
  else shown = fmtPlain(value);
  return `<div class="kv"><div class="label">${{label}}</div><div class="value">${{shown}}</div></div>`;
}}
function section(title, items, note=''){{
  const noteHtml = note ? `<div class="small note">${{note}}</div>` : '';
  return `<div class="card"><div class="section-title">${{title}}</div>${{noteHtml}}<div class="grid">${{items.join('')}}</div></div>`;
}}
function isBrownseaDestinationPostcode(q){{ return q === 'BH137EE'; }}
function destinationPostcodeCard(){{
  return `<div class="card notfound"><h2 style="margin:0 0 8px;">Brownsea Island destination postcode</h2><p style="margin:0 0 10px;">BH13 7EE is Brownsea Island's destination postcode. This tool estimates visitor access from mainland origin postcodes.</p><div class="small">Please enter a visitor origin postcode instead, for example a BH, DT, or SP residential postcode.</div></div>`;
}}
function modelGap(row){{
  const explicit = numberValue(row.district_model_gap_per_1000);
  if(explicit !== null) return explicit;
  const predicted = numberValue(row.district_predicted_visit_rate);
  const observed = numberValue(row.district_visits_per_1000);
  if(predicted === null || observed === null) return null;
  return predicted - observed;
}}
function performanceAgainstExpectation(row){{
  const gap = modelGap(row);
  if(gap === null) return 'Not available';
  const absGap = Math.abs(gap).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}});
  if(Math.abs(gap) < 0.05) return 'In line with expected';
  if(gap > 0) return `${{absGap}} visits per 1,000 below expected`;
  return `${{absGap}} visits per 1,000 above expected`;
}}
function accessPosition(row){{
  if(isBrownseaSite(row.nearest_nt_site_name)) return 'No competing NT site identified';
  const gap = numberValue(row.brownsea_vs_nearest_nt_gap_min);
  if(gap === null) return 'Not available';
  const absGap = Math.abs(gap).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}});
  if(Math.abs(gap) < 0.05) return 'Brownsea journey is broadly similar to the nearest competing NT site';
  if(gap > 0) return `Brownsea journey is ${{absGap}} minutes longer than the nearest competing NT site`;
  return `Brownsea journey is ${{absGap}} minutes shorter than the nearest competing NT site`;
}}
function boolValue(v){{
  if(v===true) return true;
  if(v===false || v===null || v===undefined) return false;
  return /^(true|1|yes)$/i.test(String(v));
}}
function travelNote(row){{
  if(boolValue(row.chain_ferry_used)) {{
    const allowance = fmtMinutes(row.chain_ferry_allowance_min);
    return `Includes a Sandbanks chain ferry allowance of ${{allowance}} minutes before the Brownsea ferry crossing. Live traffic and ferry waiting times are not included.`;
  }}
  return 'Journey times are planning estimates and do not include live traffic or live ferry waiting times.';
}}
function needLevel(row){{
  const decile = numberValue(row.imd_decile);
  if(decile === null) return 'Not available';
  if(decile <= 3) return 'High local need';
  if(decile <= 6) return 'Moderate local need';
  return 'Lower local need';
}}
function suggestedNextAction(row){{
  const zone = fmtCategory(row.priority_zone).toLowerCase();
  const intervention = fmtCategory(row.intervention_type).toLowerCase();
  if(zone.includes('urgent') || intervention.includes('crisis')) return 'Prioritise outreach and access support';
  if(zone.includes('high') || intervention.includes('targeted')) return 'Use targeted outreach and monitor response';
  if(zone.includes('growth')) return 'Test awareness and conversion activity';
  if(zone.includes('monitor')) return 'Monitor engagement before intervention';
  if(zone.includes('maintain') || intervention.includes('model')) return 'Maintain current engagement approach';
  return 'Review alongside local operational context';
}}
function assessmentReason(row){{
  const gap = modelGap(row);
  const need = needLevel(row).toLowerCase();
  const accessGap = numberValue(row.brownsea_vs_nearest_nt_gap_min);
  if(gap !== null && gap > 0.25 && need.includes('high')) return 'Low observed engagement relative to expectation in a high-need district';
  if(gap !== null && gap > 0.25) return 'Observed engagement is below model expectation';
  if(gap !== null && gap < -0.25) return 'Observed engagement is above model expectation';
  if(accessGap !== null && accessGap > 1) return 'Brownsea journey is slower than the nearest competing NT site';
  return 'Observed engagement is broadly in line with model expectation';
}}
function patternNoteFromNarrative(text){{
  const raw = rawText(text);
  if(/Less typical visitor pattern/i.test(raw)) return 'Visitor pattern is less typical than similar districts';
  return 'Not available';
}}
function patternNote(row){{
  if(row && row.pattern_note) return fmtPlain(row.pattern_note);
  return patternNoteFromNarrative(row ? row.shap_narrative : null);
}}
function cleanNarrative(text){{
  let raw = rawText(text);
  if(raw === 'Not available') return raw;
  raw = raw.replace(/^Narrative:\s*/i, '');
  raw = raw.replace(/\[[^\]]+\]/g, '').trim();
  raw = raw.replace(/(?<!Engagement )\bStatus\s*:/gi, 'Engagement status:');
  const replacements = [
    [/\bExceeding Target\b/gi, 'Above expected'],
    [/\bBelow Target\b/gi, 'Below expected'],
    [/\bOn Target\b/gi, 'In line with expected'],
    [/\bPrimary Barriers\s*:/gi, 'Main barriers:'],
    [/\bPositive Drivers\s*:/gi, 'Positive factors:'],
    [/\bDrive Time to Competitor NT Site\b/gi, 'drive time to nearest NT site'],
    [/\bDrive Time\b/gi, 'drive time'],
    [/\bTravel Time\b/gi, 'journey time'],
    [/\bFerry Duration\b/gi, 'ferry crossing time'],
    [/\bLocal Income Levels\b/gi, 'local income context'],
    [/\bOverall Deprivation\b/gi, 'deprivation level'],
    [/\bGeographic Isolation\b/gi, 'geographic access barriers'],
    [/\bSystemic Barriers\b/gi, 'wider access barriers'],
    [/\bLogistical Accessibility\b/gi, 'accessibility'],
    [/\bModel Sensitivity\b/gi, 'pattern note'],
    [/\bFragility\b/gi, 'visitor pattern']
  ];
  replacements.forEach(([pattern, replacement]) => {{ raw = raw.replace(pattern, replacement); }});
  raw = raw.replace(/\b(?:Engagement\s+)+(?:Engagement\s+)*status\s*:/gi, 'Engagement status:');
  raw = raw.replace(/\s*\|\s*/g, '. ');
  raw = raw.replace(/\s+/g, ' ').replace(/\s+([.,;:])/g, '$1').replace(/[ .]+$/g, '').trim();
  return raw || 'Not available';
}}
function findRow(q) {{
  const exact = DATA.find(r => normalise(r.postcode_clean || r.postcode) === q);
  if (exact) return exact;
  const outward = DATA.find(r => (normalise(r.postcode_clean || r.postcode) || '').startsWith(q));
  return outward || null;
}}
function searchPostcode() {{
  const q = normalise(document.getElementById('query').value);
  const out = document.getElementById('result');
  if (!q) {{ out.innerHTML = '<div class="card notfound">Enter a postcode to search.</div>'; return; }}
  if (!DATA_READY) {{ out.innerHTML = '<div class="card notfound">Lookup data is still loading. Please try again in a moment.</div>'; return; }}
  if (isBrownseaDestinationPostcode(q)) {{ out.innerHTML = destinationPostcodeCard(); return; }}
  const row = findRow(q);
  if (!row) {{ out.innerHTML = '<div class="card notfound"><strong>No matching postcode found.</strong><div class="small">Try a full visitor-origin BH / DT / SP postcode.</div></div>'; return; }}

  const chips = `
    <div class=\"banner\">
      <span class=\"tag\">Priority: ${{fmtCategory(row.priority_zone)}}</span>
      <span class=\"tag\">Action type: ${{fmtCategory(row.intervention_type)}}</span>
      <span class=\"tag\">Deprivation: ${{fmtCategory(row.deprivation_category)}}</span>
    </div>`;

  out.innerHTML = `
    <div class=\"card\">
      <h2 style=\"margin:0 0 6px;\">${{fmtPlain(row.postcode)}}</h2>
      <div class=\"small\">District ${{fmtPlain(row.district)}} · Authority ${{fmtPlain(row.authority_name)}} · Region ${{fmtPlain(row.region_name)}}</div>
      ${{chips}}
      <div class=\"small\" style=\"margin-top:12px;\"><strong>Summary:</strong> ${{cleanNarrative(row.shap_narrative)}}</div>
      <div class=\"small\" style=\"margin-top:6px;\"><strong>Pattern note:</strong> ${{patternNote(row)}}</div>
    </div>
    ${{section('Brownsea access', [
      kv('Departure terminal', row.brownsea_departure_terminal),
      kv('Journey to departure terminal (minutes)', row.drive_to_departure_terminal_min, 'minutes'),
      kv('Access route', row.access_route_mode),
      kv('Ferry crossing time (minutes)', row.brownsea_crossing_min, 'minutes'),
      kv('Total Brownsea journey (minutes)', row.total_brownsea_journey_min, 'minutes')
    ], travelNote(row))}}
    ${{section('Nearest competing NT site', [
      kv('Nearest NT site', competitorName(row)),
      kv('Nearest NT site drive time (minutes)', competitorMetric(row, 'nearest_nt_site_drive_min'), 'minutes'),
      kv('Access position', accessPosition(row)),
      kv('Brownsea accessibility score', row.brownsea_accessibility_score, 'number')
    ])}}
    ${{section('Local context', [
      kv('IMD decile', row.imd_decile, 'number'),
      kv('Need level', needLevel(row)),
      kv('Deprivation category', row.deprivation_category, 'category'),
      kv('Average FSM percentage', row['avg_fsm%'], 'percent'),
      kv('Observed district visits per 1000', row.district_visits_per_1000, 'rate')
    ])}}
    ${{section('District-Level Assessment', [
      kv('Observed visits per 1000', row.district_visits_per_1000, 'rate'),
      kv('Model expected visits per 1000', row.district_predicted_visit_rate, 'rate'),
      kv('Performance against expectation', performanceAgainstExpectation(row)),
      kv('Priority zone', row.priority_zone, 'category'),
      kv('Suggested next action', suggestedNextAction(row)),
      kv('Assessment reason', assessmentReason(row)),
      kv('Action type', row.intervention_type, 'category'),
      kv('Alternative departure terminal', row.alternative_brownsea_departure_terminal)
    ])}}
  `;
}}
function runHashSearch() {{
  if (location.hash.startsWith('#postcode=')) {{
    const value = decodeURIComponent(location.hash.replace('#postcode=', ''));
    document.getElementById('query').value = value;
    searchPostcode();
  }}
}}
if (DATA_READY) {{ runHashSearch(); }}
document.getElementById('query').addEventListener('keydown', e => {{ if (e.key === 'Enter') searchPostcode(); }});
</script>
</body>
</html>"""
    output_path.write_text(html, encoding="utf-8")
    return str(output_path)



def build_downloads_html(release_dir: Path, output_path: Path) -> str:
    """Write a static reports/downloads page for a completed release.

    The live Flask app has richer dynamic downloads, including a ZIP bundle.
    This static page gives users clear links when they are browsing saved HTML
    reports directly from Drive or a release folder.
    """
    release_dir = Path(release_dir)
    reports_dir = release_dir / "reports"
    artifacts_dir = release_dir / "artifacts"

    items: list[tuple[str, str, str]] = []
    candidates = [
        ("Reports index", "Main report index.", "index.html", reports_dir / "index.html"),
        ("Postcode app report", "Static postcode lookup report.", "postcode_app.html", reports_dir / "postcode_app.html"),
        ("Postcode lookup CSV", "Spreadsheet-friendly postcode lookup results.", "../artifacts/postcode_lookup.csv", artifacts_dir / "postcode_lookup.csv"),
        ("Postcode lookup JSON", "Machine-readable postcode lookup results used by the app.", "../artifacts/postcode_lookup.json", artifacts_dir / "postcode_lookup.json"),
        ("Model performance CSV", "Model ranking and performance metrics.", "../artifacts/model_performance.csv", artifacts_dir / "model_performance.csv"),
        ("Model performance summary", "Short machine-readable model performance summary.", "../artifacts/model_performance_summary.json", artifacts_dir / "model_performance_summary.json"),
        ("Three-way analysis CSV", "District-level analysis table used by the postcode lookup.", "../artifacts/three_way_intersection_analysis_v2.csv", artifacts_dir / "three_way_intersection_analysis_v2.csv"),
        ("Release manifest", "Release file list and hashes.", "../release_manifest.json", release_dir / "release_manifest.json"),
        ("Run manifest", "Pipeline run metadata.", "../run_manifest.json", release_dir / "run_manifest.json"),
        ("Stage run manifest", "Stage execution metadata.", "../stage_run_manifest.json", release_dir / "stage_run_manifest.json"),
        ("Release lock", "Freeze record for the release candidate.", "../release_lock.json", release_dir / "release_lock.json"),
    ]
    for name, description, href, path in candidates:
        if path.exists():
            items.append((name, description, href))

    rows = "\n".join(
        f'<div class="item"><div class="name">{name}</div><div class="description">{description}</div><div><a class="button" href="{href}">Open / Download</a></div></div>'
        for name, description, href in items
    ) or '<div class="description">No downloadable files were found in this release.</div>'

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
.name {{ font-weight:700; }} .description {{ color:var(--muted); font-size:13px; }}
.button {{ display:inline-block; padding:8px 12px; border-radius:10px; background:#111827; color:#fff; text-decoration:none; white-space:nowrap; }}
@media (max-width:760px){{ .item {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<header>
  <h1 style="margin:0;">Reports & Downloads</h1>
  <p style="max-width:780px; margin:8px 0 0;">Open or download the released reports, app files, and audit outputs.</p>
  <div class="links"><a href="postcode_app.html">Back to postcode lookup</a><a href="help.html">Help</a><a href="index.html">Reports index</a></div>
</header>
<main><div class="card">{rows}</div></main>
</body>
</html>"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return str(output_path)
