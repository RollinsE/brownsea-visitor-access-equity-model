from __future__ import annotations

import argparse
import io
import json
import re
import zipfile
from pathlib import Path
from typing import Any

from src.release_manager import find_latest_release_lookup
from src.help_page import build_help_html


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

    raw = re.sub(r"\b(?:Engagement\s+)+status\s*:", "Engagement status:", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*\|\s*", ". ", raw)
    raw = re.sub(r"\s+", " ", raw)
    raw = re.sub(r"\s+([.,;:])", r"\1", raw)
    raw = raw.strip(" .")
    return raw or "Not available"


def _pattern_note_from_narrative(value: Any) -> str:
    raw = str(value or "")
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


def _plain_narrative(value: Any) -> str:
    return _normalise_engagement_text(str(value or ""))

BROWNSEA_DESTINATION_POSTCODES = {"BH137EE"}


def normalise_postcode(value: str) -> str:
    return (value or '').replace(' ', '').upper().strip()


def is_brownsea_destination_postcode(value: str) -> bool:
    return normalise_postcode(value) in BROWNSEA_DESTINATION_POSTCODES


def brownsea_destination_message() -> str:
    return (
        "BH13 7EE is Brownsea Island's destination postcode. "
        "This tool estimates visitor access from mainland origin postcodes. "
        "Please enter a visitor origin postcode instead."
    )


def _is_brownsea_competitor(value: Any) -> bool:
    return "brownsea" in str(value or "").lower()


def sanitize_lookup_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return an app-safe row for display/API responses.

    Brownsea is the destination, not a competing NT site, so it is hidden
    from competitor fields at the app boundary.
    """
    clean = dict(row)
    if _is_brownsea_competitor(clean.get("nearest_nt_site_name")):
        clean["nearest_nt_site_name"] = "No competing NT site identified"
        clean["nearest_nt_site_drive_min"] = None
        clean["brownsea_vs_nearest_nt_gap_min"] = None
    if "shap_narrative" in clean:
        clean["pattern_note"] = _pattern_note_from_narrative(clean.get("shap_narrative"))
        clean["shap_narrative"] = _plain_narrative(clean.get("shap_narrative"))
    return clean


def load_lookup(lookup_path: Path) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    data = json.loads(lookup_path.read_text(encoding='utf-8'))
    safe_data = [sanitize_lookup_row(row) for row in data]
    index: dict[str, dict[str, Any]] = {}
    for row in safe_data:
        key = normalise_postcode(row.get('postcode_clean') or row.get('postcode') or '')
        if key:
            index[key] = row
    return safe_data, index


def _release_base_for_lookup(lookup_file: Path) -> Path | None:
    parts = lookup_file.parts
    if 'releases' not in parts:
        return None
    idx = parts.index('releases')
    if len(parts) <= idx + 1:
        return None
    return Path(*parts[:idx + 2])



def _file_size_label(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    size = path.stat().st_size
    units = ["B", "KB", "MB", "GB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return ""


def _download_item(name: str, description: str, url: str, path: Path | None = None) -> dict[str, str]:
    return {
        "name": name,
        "description": description,
        "url": url,
        "size": _file_size_label(path) if path else "",
    }


def _collect_download_sections(release_base: Path | None, reports_dir: Path, artifacts_dir: Path) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    core_candidates = [
        ("Postcode lookup CSV", "Spreadsheet-friendly postcode lookup results.", "/artifacts/postcode_lookup.csv", artifacts_dir / "postcode_lookup.csv"),
        ("Postcode lookup JSON", "Machine-readable postcode lookup results used by the app.", "/artifacts/postcode_lookup.json", artifacts_dir / "postcode_lookup.json"),
    ]
    core = [_download_item(*item) for item in core_candidates if item[3].exists()]
    if core:
        sections.append({"title": "Core app files", "items": core})

    preferred_reports = [
        ("Reports index", "Open the main report index.", "/reports/index.html", reports_dir / "index.html"),
        ("Postcode app report", "Open the static postcode lookup report.", "/reports/postcode_app.html", reports_dir / "postcode_app.html"),
        ("Postcode lookup report", "Open the postcode lookup report.", "/reports/postcode_lookup.html", reports_dir / "postcode_lookup.html"),
        ("Model performance report", "Open the model performance report.", "/reports/model_performance.html", reports_dir / "model_performance.html"),
    ]
    report_items = [_download_item(*item) for item in preferred_reports if item[3].exists()]
    seen = {item["url"] for item in report_items}
    if reports_dir.exists():
        for path in sorted(reports_dir.rglob("*.html")):
            rel = path.relative_to(reports_dir).as_posix()
            url = f"/reports/{rel}"
            if url in seen or rel == "downloads.html":
                continue
            name = path.stem.replace("_", " ").replace("-", " ").title()
            report_items.append(_download_item(name, "Open report.", url, path))
            seen.add(url)
    if report_items:
        sections.append({"title": "Reports", "items": report_items})

    plot_items: list[dict[str, str]] = []
    if reports_dir.exists():
        for suffix in ("*.png", "*.svg", "*.jpg", "*.jpeg", "*.webp"):
            for path in sorted(reports_dir.rglob(suffix)):
                rel = path.relative_to(reports_dir).as_posix()
                name = path.stem.replace("_", " ").replace("-", " ").title()
                plot_items.append(_download_item(name, "View or download plot.", f"/reports/{rel}", path))
    if plot_items:
        sections.append({"title": "Plots", "items": plot_items})

    audit_candidates: list[tuple[str, str, str, Path]] = [
        ("Model performance CSV", "Model ranking and performance metrics.", "/artifacts/model_performance.csv", artifacts_dir / "model_performance.csv"),
        ("Model performance summary", "Short machine-readable model performance summary.", "/artifacts/model_performance_summary.json", artifacts_dir / "model_performance_summary.json"),
        ("Three-way analysis CSV", "District-level analysis table used by the postcode lookup.", "/artifacts/three_way_intersection_analysis_v2.csv", artifacts_dir / "three_way_intersection_analysis_v2.csv"),
    ]
    if release_base is not None:
        audit_candidates.extend([
            ("Release manifest", "Release file list and hashes.", "/release-file/release_manifest.json", release_base / "release_manifest.json"),
            ("Run manifest", "Pipeline run metadata.", "/release-file/run_manifest.json", release_base / "run_manifest.json"),
            ("Stage run manifest", "Stage execution metadata.", "/release-file/stage_run_manifest.json", release_base / "stage_run_manifest.json"),
            ("Release lock", "Freeze record for the release candidate.", "/release-file/release_lock.json", release_base / "release_lock.json"),
        ])
    audit = [_download_item(*item) for item in audit_candidates if item[3].exists()]
    if audit:
        sections.append({"title": "Audit and model files", "items": audit})

    sections.append({"title": "Bundle", "items": [
        _download_item("Download reports bundle", "ZIP containing reports plus key release artifacts and manifests.", "/downloads/reports.zip")
    ]})
    return sections


def _iter_bundle_files(release_base: Path | None, reports_dir: Path, artifacts_dir: Path):
    if reports_dir.exists():
        for path in sorted(p for p in reports_dir.rglob("*") if p.is_file()):
            yield path, Path("reports") / path.relative_to(reports_dir)
    selected_artifacts = [
        "postcode_lookup.csv",
        "postcode_lookup.json",
        "model_performance.csv",
        "model_performance_summary.json",
        "three_way_intersection_analysis_v2.csv",
    ]
    for name in selected_artifacts:
        path = artifacts_dir / name
        if path.exists() and path.is_file():
            yield path, Path("artifacts") / name
    if release_base is not None:
        for name in ["release_manifest.json", "run_manifest.json", "stage_run_manifest.json", "release_lock.json"]:
            path = release_base / name
            if path.exists() and path.is_file():
                yield path, Path(name)


def _reports_bundle_bytes(release_base: Path | None, reports_dir: Path, artifacts_dir: Path) -> io.BytesIO:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path, arcname in _iter_bundle_files(release_base, reports_dir, artifacts_dir):
            zf.write(path, arcname.as_posix())
    buffer.seek(0)
    return buffer

def create_app(lookup_path: str | None = None, outputs_root: str | None = None) -> Any:
    from flask import Flask, abort, jsonify, render_template, request, send_file, send_from_directory

    app = Flask(__name__, template_folder='templates', static_folder='static')
    outputs_dir = Path(outputs_root) if outputs_root else Path('outputs')
    if lookup_path:
        lookup_file = Path(lookup_path)
    else:
        resolved = find_latest_release_lookup(outputs_dir)
        if resolved is None:
            raise FileNotFoundError(f"Could not find postcode lookup artifact under {outputs_dir}")
        lookup_file = resolved

    release_base = _release_base_for_lookup(lookup_file)
    if release_base is not None:
        reports_dir = release_base / 'reports'
        artifacts_dir = release_base / 'artifacts'
    else:
        base_dir = lookup_file.parent.parent if lookup_path else outputs_dir
        reports_dir = base_dir / 'reports'
        artifacts_dir = base_dir / 'artifacts'

    rows, lookup_index = load_lookup(lookup_file)

    @app.get('/')
    def home():
        return render_template(
            'index.html',
            record_count=len(rows),
            reports_index='/reports/index.html',
            download_csv='/artifacts/postcode_lookup.csv',
            download_json='/artifacts/postcode_lookup.json',
            downloads_page='/downloads',
        )

    @app.get('/api/lookup')
    def api_lookup():
        query = normalise_postcode(request.args.get('postcode', ''))
        if not query:
            return jsonify({'error': 'postcode is required'}), 400
        if is_brownsea_destination_postcode(query):
            return jsonify({'match_type': 'destination', 'result': None, 'message': brownsea_destination_message()})

        exact = lookup_index.get(query)
        if exact:
            return jsonify({'match_type': 'exact', 'result': exact})

        prefix = next((row for key, row in lookup_index.items() if key.startswith(query)), None)
        if prefix:
            return jsonify({'match_type': 'prefix', 'result': prefix})

        return jsonify({'match_type': 'none', 'result': None}), 404

    @app.get('/help')
    def help_page():
        return build_help_html(home_href='/', downloads_href='/downloads', reports_href='/reports/index.html')

    @app.get('/health')
    def health():
        return jsonify({'status': 'ok', 'records': len(rows), 'lookup_path': str(lookup_file)})

    @app.get('/reports/<path:filename>')
    def reports(filename: str):
        target = reports_dir / filename
        if not target.exists():
            abort(404)
        return send_from_directory(reports_dir, filename)

    @app.get('/artifacts/<path:filename>')
    def artifacts(filename: str):
        target = artifacts_dir / filename
        if not target.exists():
            abort(404)
        return send_from_directory(artifacts_dir, filename)

    @app.get('/release-file/<path:filename>')
    def release_file(filename: str):
        if release_base is None or '/' in filename or '\\' in filename:
            abort(404)
        target = release_base / filename
        if not target.exists() or not target.is_file():
            abort(404)
        return send_from_directory(release_base, filename, as_attachment=True)

    @app.get('/downloads')
    def downloads():
        sections = _collect_download_sections(release_base, reports_dir, artifacts_dir)
        return render_template('downloads.html', sections=sections, release_base=str(release_base or artifacts_dir.parent))

    @app.get('/downloads/reports.zip')
    def reports_bundle():
        buffer = _reports_bundle_bytes(release_base, reports_dir, artifacts_dir)
        return send_file(buffer, mimetype='application/zip', as_attachment=True, download_name='brownsea_reports_bundle.zip')

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description='Run Brownsea postcode lookup app')
    parser.add_argument('--lookup', default=None, help='Path to postcode_lookup.json; defaults to outputs_root/releases/latest/artifacts/postcode_lookup.json when available')
    parser.add_argument('--outputs-root', default='outputs', help='Path to outputs root folder containing builds/ and releases/')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    app = create_app(args.lookup, args.outputs_root)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()
