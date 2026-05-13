import json
from pathlib import Path

import pandas as pd

from scripts.refresh_app_ui import refresh_app_ui
from src.web_ui import build_postcode_app_html, build_postcode_json


def _sample_lookup_df(nearest_site="Studland Bay"):
    return pd.DataFrame([
        {
            "postcode": "BH1 1AA",
            "postcode_clean": "BH11AA",
            "district": "BH1",
            "authority_name": "BCP",
            "region_name": "South West",
            "priority_zone": "Maintain",
            "intervention_type": "Model District",
            "deprivation_category": "moderately_deprived",
            "avg_fsm%": 12.3456,
            "district_visits_per_1000": 7.6045627376,
            "district_predicted_visit_rate": 6.8971633094,
            "district_model_gap_per_1000": -0.7073994282,
            "imd_decile": 6,
            "brownsea_departure_terminal": "Sandbanks Jetty",
            "drive_to_departure_terminal_min": 2.345,
            "brownsea_crossing_min": 5,
            "total_brownsea_journey_min": 7.345,
            "nearest_nt_site_name": nearest_site,
            "nearest_nt_site_drive_min": 4.612,
            "brownsea_vs_nearest_nt_gap_min": 2.733,
            "brownsea_accessibility_score": 93.9,
            "alternative_brownsea_departure_terminal": "Poole Quay",
            "shap_narrative": "Narrative: Status: Exceeding Target | Primary Barriers: Geographic Isolation, Systemic Barriers | Positive Drivers: Travel Time, Local Income Levels [High Fragility: 73% above peers]",
        }
    ])


def test_postcode_app_html_uses_assessment_labels_and_formatters(tmp_path):
    output = tmp_path / "postcode_app.html"
    build_postcode_app_html(_sample_lookup_df(), output, {})
    html = output.read_text(encoding="utf-8")

    assert "District-Level Assessment" in html
    assert "Model expected visits per 1000" in html
    assert "Performance against expectation" in html
    assert "Suggested next action" in html
    assert "Assessment reason" in html
    assert "Need level" in html
    assert "Access position" in html
    assert "<strong>Summary:</strong>" in html
    assert "Pattern note" in html
    assert "Strategic recommendation" not in html
    assert "Model predicted visits per 1000" not in html
    assert "recommendationSummary" not in html
    assert "fmt(row" not in html


def test_postcode_app_html_uses_plain_language_for_model_narrative(tmp_path):
    output = tmp_path / "postcode_app.html"
    build_postcode_app_html(_sample_lookup_df(), output, {})
    html = output.read_text(encoding="utf-8")

    assert "<strong>Summary:</strong>" in html
    assert "Pattern note" in html
    assert "Above expected" in html
    assert "Main barriers" in html
    assert "Positive factors" in html
    assert "geographic access barriers" in html
    assert "wider access barriers" in html
    assert "journey time" in html
    assert "local income context" in html
    assert "Visitor pattern is less typical than similar districts" in html
    assert "High Fragility: 73% above peers" not in html
    assert "High Model Sensitivity" not in html
    assert "above comparable districts" not in html
    assert "Model sensitivity" not in html
    assert "Primary Barriers: Geographic" not in html
    assert "Positive Drivers: Travel" not in html
    assert "Status: Exceeding Target" not in html


def test_brownsea_is_hidden_as_competitor_in_json_artifact(tmp_path):
    output = tmp_path / "postcode_lookup.json"
    build_postcode_json(_sample_lookup_df(nearest_site="Brownsea Island"), output)
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload[0]["nearest_nt_site_name"] == "No competing NT site identified"
    assert payload[0]["nearest_nt_site_drive_min"] is None
    assert payload[0]["brownsea_vs_nearest_nt_gap_min"] is None


def test_refresh_app_ui_rewrites_release_html_without_pipeline_run(tmp_path):
    release = tmp_path / "outputs" / "releases" / "latest"
    artifacts = release / "artifacts"
    reports = release / "reports"
    artifacts.mkdir(parents=True)
    reports.mkdir(parents=True)
    (artifacts / "postcode_lookup.json").write_text(
        json.dumps(_sample_lookup_df().to_dict(orient="records")),
        encoding="utf-8",
    )

    written = refresh_app_ui(tmp_path / "outputs", "latest")

    assert reports / "postcode_app.html" in written
    assert reports / "postcode_lookup.html" in written
    assert reports / "downloads.html" in written
    assert (reports / "postcode_app.html").exists()
    assert (reports / "downloads.html").exists()
    refreshed = (reports / "postcode_app.html").read_text(encoding="utf-8")
    assert "District-Level Assessment" in refreshed
    assert "Model expected visits per 1000" in refreshed
    assert "<strong>Summary:</strong>" in refreshed
    assert "Pattern note" in refreshed


def test_engagement_summary_is_idempotent_and_plain_language(tmp_path):
    from app.server import _plain_narrative

    raw = (
        "Narrative: Status: Exceeding Target | Primary Barriers: Local Income Levels, "
        "Drive Time to Competitor NT Site | Positive Drivers: Travel Time, Overall Deprivation "
        "[High Fragility: 73% above peers]"
    )
    once = _plain_narrative(raw)
    twice = _plain_narrative(once)
    thrice = _plain_narrative("Engagement Engagement Engagement status: Above expected")

    assert once == twice
    assert "Engagement Engagement" not in twice
    assert "Engagement status: Above expected" in thrice
    assert "drive time to nearest NT site" in twice
    assert "Drive Time to Competitor NT Site" not in twice
    assert "deprivation level" in twice


def test_performance_gap_includes_visits_unit(tmp_path):
    output = tmp_path / "postcode_app.html"
    build_postcode_app_html(_sample_lookup_df(), output, {})
    html = output.read_text(encoding="utf-8")

    assert "visits per 1,000 above expected" in html
    assert "visits per 1,000 below expected" in html


def test_sandbanks_chain_ferry_note_is_data_driven(tmp_path):
    df = _sample_lookup_df()
    df.loc[0, "district"] = "BH19"
    df.loc[0, "brownsea_departure_terminal"] = "Sandbanks Jetty"
    df.loc[0, "chain_ferry_used"] = True
    df.loc[0, "chain_ferry_allowance_min"] = 10
    df.loc[0, "access_route_mode"] = "Sandbanks via chain ferry"
    output = tmp_path / "postcode_app.html"
    build_postcode_app_html(df, output, {})
    html = output.read_text(encoding="utf-8")

    assert "Includes a Sandbanks chain ferry allowance" in html
    assert "Access route" in html


def test_postcode_metrics_model_chain_ferry_for_purbeck_side():
    from src.postcode_lookup import _build_brownsea_metrics

    postcodes = pd.DataFrame([
        {"district": "BH19", "lat": 50.609, "long": -1.958},
    ])
    metrics = _build_brownsea_metrics(postcodes)

    assert metrics.loc[0, "chain_ferry_used"] in [True, bool(True)]
    assert metrics.loc[0, "access_route_mode"] == "Sandbanks via chain ferry"
    assert metrics.loc[0, "chain_ferry_allowance_min"] == 10
    assert metrics.loc[0, "drive_to_departure_terminal_min"] > 10


def test_static_app_guards_brownsea_destination_postcode(tmp_path):
    output = tmp_path / "postcode_app.html"
    build_postcode_app_html(_sample_lookup_df(), output, {})
    html = output.read_text(encoding="utf-8")

    assert "isBrownseaDestinationPostcode" in html
    assert "Brownsea Island destination postcode" in html
    assert "Reports & Downloads" in html
    assert "downloads.html" in html
