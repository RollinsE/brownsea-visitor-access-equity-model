import json
from pathlib import Path

import pytest

from app.server import _release_base_for_lookup


def test_release_base_is_inferred_from_lookup_path(tmp_path):
    lookup = tmp_path / "outputs" / "releases" / "latest" / "artifacts" / "postcode_lookup.json"
    assert _release_base_for_lookup(lookup) == tmp_path / "outputs" / "releases" / "latest"


def test_app_serves_latest_release_reports_and_artifacts(tmp_path):
    pytest.importorskip("flask")
    from app.server import create_app

    release = tmp_path / "outputs" / "releases" / "latest"
    artifacts = release / "artifacts"
    reports = release / "reports"
    artifacts.mkdir(parents=True)
    reports.mkdir(parents=True)

    lookup_payload = [
        {
            "postcode": "BH15 1ED",
            "postcode_clean": "BH151ED",
            "district": "BH15",
            "priority_zone": "Priority Action",
        }
    ]
    (artifacts / "postcode_lookup.json").write_text(json.dumps(lookup_payload), encoding="utf-8")
    (artifacts / "postcode_lookup.csv").write_text("postcode\nBH13 7EE\n", encoding="utf-8")
    (reports / "index.html").write_text("<html>release report</html>", encoding="utf-8")

    app = create_app(outputs_root=str(tmp_path / "outputs"))
    client = app.test_client()

    api = client.get("/api/lookup?postcode=BH15%201ED")
    assert api.status_code == 200
    assert api.get_json()["result"]["postcode_clean"] == "BH151ED"

    report = client.get("/reports/index.html")
    assert report.status_code == 200
    assert b"release report" in report.data

    csv = client.get("/artifacts/postcode_lookup.csv")
    assert csv.status_code == 200
    assert b"BH15 1ED" in csv.data

    downloads = client.get("/downloads")
    assert downloads.status_code == 200
    assert b"Reports & Downloads" in downloads.data

    help_page = client.get("/help")
    assert help_page.status_code == 200
    assert b"Help & Definitions" in help_page.data
    assert b"Accessibility score guide" in help_page.data

    bundle = client.get("/downloads/reports.zip")
    assert bundle.status_code == 200
    assert bundle.content_type == "application/zip"


def test_load_lookup_sanitizes_brownsea_as_competitor(tmp_path):
    from app.server import load_lookup

    lookup = tmp_path / "postcode_lookup.json"
    lookup.write_text(
        json.dumps([
            {
                "postcode": "BH15 1ED",
                "postcode_clean": "BH151ED",
                "nearest_nt_site_name": "Brownsea Island",
                "nearest_nt_site_drive_min": 3.4,
                "brownsea_vs_nearest_nt_gap_min": 1.2,
            }
        ]),
        encoding="utf-8",
    )

    rows, index = load_lookup(lookup)

    assert rows[0]["nearest_nt_site_name"] == "No competing NT site identified"
    assert rows[0]["nearest_nt_site_drive_min"] is None
    assert rows[0]["brownsea_vs_nearest_nt_gap_min"] is None
    assert index["BH151ED"]["nearest_nt_site_name"] == "No competing NT site identified"


def test_brownsea_destination_postcode_returns_guidance(tmp_path):
    pytest.importorskip("flask")
    from app.server import create_app

    release = tmp_path / "outputs" / "releases" / "latest"
    artifacts = release / "artifacts"
    reports = release / "reports"
    artifacts.mkdir(parents=True)
    reports.mkdir(parents=True)
    (artifacts / "postcode_lookup.json").write_text(
        json.dumps([{"postcode": "BH13 7EE", "postcode_clean": "BH137EE", "district": "BH13"}]),
        encoding="utf-8",
    )
    app = create_app(outputs_root=str(tmp_path / "outputs"))
    client = app.test_client()

    response = client.get("/api/lookup?postcode=BH13%207EE")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["match_type"] == "destination"
    assert payload["result"] is None
    assert "destination postcode" in payload["message"]
