from __future__ import annotations

import json
from pathlib import Path

from src.project_doctor import diagnose_project, inspect_route_cache
from src.release_freeze import freeze_release
from src.release_manager import write_release_pointer


def _write_minimal_release_files(base: Path) -> None:
    (base / "artifacts").mkdir(parents=True, exist_ok=True)
    (base / "reports").mkdir(parents=True, exist_ok=True)
    (base / "checkpoints").mkdir(parents=True, exist_ok=True)
    (base / "run_manifest.json").write_text(json.dumps({"run_id": "run123", "runtime": "test"}), encoding="utf-8")
    (base / "stage_run_manifest.json").write_text(json.dumps({"stages": [1, 2, 3, 4, 5]}), encoding="utf-8")
    (base / "artifacts" / "postcode_lookup.json").write_text(json.dumps([{"postcode_clean": "BH151AA"}]), encoding="utf-8")
    (base / "artifacts" / "postcode_lookup.csv").write_text("postcode_clean\nBH151AA\n", encoding="utf-8")
    (base / "artifacts" / "three_way_intersection_analysis_v2.csv").write_text("District,value\nBH1,1\n", encoding="utf-8")
    (base / "artifacts" / "model_performance.csv").write_text("model,mae\nrf,1\n", encoding="utf-8")
    (base / "artifacts" / "model_performance_summary.json").write_text(json.dumps({"best_model": "rf"}), encoding="utf-8")
    (base / "reports" / "postcode_lookup.html").write_text("<html>lookup</html>", encoding="utf-8")
    (base / "reports" / "postcode_app.html").write_text("<html>app</html>", encoding="utf-8")
    (base / "reports" / "index.html").write_text("<html>index</html>", encoding="utf-8")
    (base / "reports" / "model_performance.html").write_text("<html>model</html>", encoding="utf-8")
    for name in ["model_bundle.joblib", "lsoa_master.parquet", "district_lsoa_map.parquet", "ons_clean.parquet"]:
        (base / "checkpoints" / name).write_bytes(b"placeholder")


def test_doctor_reports_pass_with_valid_release_and_pointer(tmp_path):
    outputs = tmp_path / "outputs"
    release = outputs / "releases" / "latest"
    _write_minimal_release_files(release)
    write_release_pointer(outputs, "latest", "run123")

    result = diagnose_project(outputs, release_name="latest")

    assert result.ok
    assert result.qa_status == "pass"
    assert result.freeze_status == "not_frozen"
    assert result.release_pointer["ok"] is True


def test_doctor_reports_freeze_pass_after_lock(tmp_path):
    outputs = tmp_path / "outputs"
    release = outputs / "releases" / "latest"
    _write_minimal_release_files(release)
    write_release_pointer(outputs, "latest", "run123")
    freeze_release(outputs, release_name="latest")

    result = diagnose_project(outputs, release_name="latest")

    assert result.ok
    assert result.freeze_status == "pass"


def test_inspect_route_cache_counts_routes(tmp_path):
    outputs = tmp_path / "outputs"
    cache = outputs / "cache" / "route_cache"
    cache.mkdir(parents=True)
    (cache / "brownsea_routes.json").write_text(json.dumps({"routes": {"a": {}, "b": {}}}), encoding="utf-8")
    (cache / "competitor_routes.json").write_text(json.dumps({"x": {}, "metadata": {}}), encoding="utf-8")

    result = inspect_route_cache(outputs)

    assert result["total_routes"] == 3
    assert result["brownsea_routes"]["routes"] == 2
    assert result["competitor_routes"]["routes"] == 1
