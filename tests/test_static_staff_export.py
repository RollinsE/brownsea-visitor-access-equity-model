from __future__ import annotations

import json
from pathlib import Path

from scripts.export_static_staff_app import export_static_staff_app


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_export_static_staff_app_creates_github_pages_folder(tmp_path: Path):
    outputs = tmp_path / "outputs"
    release = outputs / "releases" / "latest"
    artifacts = release / "artifacts"
    reports = release / "reports"

    records = [
        {
            "postcode": "BH15 1ED",
            "postcode_clean": "BH151ED",
            "district": "BH15",
            "authority_name": "Test Authority",
            "region_name": "Test Region",
            "priority_zone": "maintain",
            "intervention_type": "model_district",
            "deprivation_category": "moderately_deprived",
            "brownsea_departure_terminal": "Poole Quay",
            "drive_to_departure_terminal_min": 10.0,
            "access_route_mode": "Road",
            "brownsea_crossing_min": 20.0,
            "total_brownsea_journey_min": 30.0,
            "nearest_nt_site_name": "Kingston Lacy",
            "nearest_nt_site_drive_min": 15.0,
            "brownsea_vs_nearest_nt_gap_min": 5.0,
            "brownsea_accessibility_score": 0.5,
            "imd_decile": 5,
            "avg_fsm%": 12.0,
            "district_visits_per_1000": 7.6,
            "district_predicted_visit_rate": 6.0,
            "alternative_brownsea_departure_terminal": "Sandbanks Jetty",
            "shap_narrative": "Status: Exceeding Target | Primary Barriers: Local Income Levels | Positive Drivers: Travel Time",
        }
    ]
    _write(artifacts / "postcode_lookup.json", json.dumps(records))
    _write(artifacts / "postcode_lookup.csv", "postcode,district\nBH15 1ED,BH15\n")
    _write(artifacts / "model_performance.csv", "model,mae\nA,1.0\n")
    _write(reports / "index.html", "<html>report index</html>")
    _write(release / "run_manifest.json", "{}")

    target = tmp_path / "docs"
    written = export_static_staff_app(outputs, release_name="latest", target=target)

    assert target.joinpath("index.html").exists()
    assert target.joinpath("downloads.html").exists()
    assert target.joinpath("help.html").exists()
    assert target.joinpath("reports.zip").exists()
    assert target.joinpath("reports", "index.html").exists()
    assert target.joinpath("artifacts", "postcode_lookup.json").exists()
    assert target.joinpath("artifacts", "postcode_shards", "BH15.json").exists()
    assert target.joinpath("artifacts", "postcode_shards_index.json").exists()
    assert target.joinpath("README_STAFF_APP.md").exists()
    assert any(path.name == "reports.zip" for path in written)

    html = target.joinpath("index.html").read_text(encoding="utf-8")
    assert "Brownsea Visitor Access Tool" in html
    assert "artifacts/postcode_lookup.csv" in html
    assert "downloads.html" in html
    assert "help.html" in html
    assert "artifacts/postcode_shards" in html
    assert "loadShard" in html
    assert "fetch(DATA_URL)" not in html
    assert "const DATA = [{" not in html
    assert target.joinpath("index.html").stat().st_size < 200_000

    help_html = target.joinpath("help.html").read_text(encoding="utf-8")
    assert "Help & Definitions" in help_html
    assert "Priority action matrix" in help_html
    assert "Strategic focus" not in help_html
    assert "Need criteria" not in help_html
    assert "Calculation" not in help_html


def test_export_static_staff_app_fails_without_lookup_json(tmp_path: Path):
    outputs = tmp_path / "outputs"
    outputs.joinpath("releases", "latest", "artifacts").mkdir(parents=True)
    target = tmp_path / "docs"

    try:
        export_static_staff_app(outputs, release_name="latest", target=target)
    except FileNotFoundError as exc:
        assert "postcode_lookup.json" in str(exc)
    else:
        raise AssertionError("Expected missing lookup JSON to fail")
