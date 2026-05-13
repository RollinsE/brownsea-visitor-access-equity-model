import json
from pathlib import Path

import pandas as pd

from src.postcode_lookup import build_postcode_lookup_artifacts


def _config(tmp_path: Path) -> dict:
    artifacts = tmp_path / "artifacts"
    reports = tmp_path / "reports"
    return {
        "artifact_dir": str(artifacts),
        "report_dir": str(reports),
        "reference_paths": {"nt_sites": str(Path("data/reference/nt_sites.csv"))},
        "output_files": {
            "postcode_lookup_csv": str(artifacts / "postcode_lookup.csv"),
            "postcode_lookup_parquet": str(artifacts / "postcode_lookup.parquet"),
            "postcode_lookup_json": str(artifacts / "postcode_lookup.json"),
            "postcode_lookup_html": str(reports / "postcode_lookup.html"),
            "postcode_app_html": str(reports / "postcode_app.html"),
        },
    }


def test_build_postcode_lookup_artifacts_smoke(tmp_path, monkeypatch):
    monkeypatch.setenv("BROWNSEA_DISABLE_BALLTREE", "1")
    ons_df = pd.DataFrame(
        {
            "POSTCODE": ["BH13 7EE", "ZZ1 1ZZ"],
            "District": ["BH13", "ZZ1"],
            "lat": [50.7, 51.0],
            "long": [-1.95, -2.0],
            "lsoa21cd": ["LSOA001", "LSOA999"],
        }
    )
    lsoa_master_df = pd.DataFrame(
        {
            "lsoa21cd": ["LSOA001"],
            "imd_decile": [3],
            "deprivation_category": ["moderately_deprived"],
            "avg_fsm%": [22.5],
            "Authority_Name": ["BCP"],
            "Region_Name": ["South West"],
        }
    )
    analysis_df = pd.DataFrame(
        {
            "District": ["BH13"],
            "visits_per_1000": [12.3],
            "predicted_visit_rate": [16.8],
            "priority_zone": ["Priority Action"],
            "intervention_type": ["Transport Access"],
            "shap_narrative": ["Access is the strongest barrier."],
        }
    )

    lookup = build_postcode_lookup_artifacts(ons_df, lsoa_master_df, analysis_df, _config(tmp_path))

    assert len(lookup) == 1
    assert lookup.loc[0, "postcode_clean"] == "BH137EE"
    assert lookup.loc[0, "nearest_nt_site_name"] != "Unknown"
    assert (tmp_path / "artifacts" / "postcode_lookup.csv").exists()
    assert (tmp_path / "artifacts" / "postcode_lookup.json").exists()
    assert (tmp_path / "reports" / "postcode_lookup.html").exists()
    assert (tmp_path / "reports" / "postcode_app.html").exists()

    payload = json.loads((tmp_path / "artifacts" / "postcode_lookup.json").read_text())
    assert payload[0]["postcode_clean"] == "BH137EE"
    assert "NaN" not in (tmp_path / "artifacts" / "postcode_lookup.json").read_text()
