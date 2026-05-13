from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.release_freeze import check_release_lock, freeze_release
from src.release_manager import write_release_pointer


def _write_minimal_release_files(base: Path) -> None:
    (base / "artifacts").mkdir(parents=True, exist_ok=True)
    (base / "reports").mkdir(parents=True, exist_ok=True)
    (base / "checkpoints").mkdir(parents=True, exist_ok=True)
    (base / "run_manifest.json").write_text(
        json.dumps({"run_id": "run123", "runtime": "test", "output_root": str(base.parent.parent)}),
        encoding="utf-8",
    )
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


def test_freeze_release_writes_lock_and_check_passes(tmp_path):
    outputs = tmp_path / "outputs"
    release = outputs / "releases" / "latest"
    _write_minimal_release_files(release)
    write_release_pointer(outputs, "latest", "run123")

    lock_path = freeze_release(outputs, release_name="latest")
    assert lock_path == release / "release_lock.json"
    payload = json.loads(lock_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "release-freeze-v1"
    assert payload["run_id"] == "run123"
    assert any(item["path"] == "artifacts/postcode_lookup.json" for item in payload["files"])

    result = check_release_lock(outputs, release_name="latest")
    assert result.ok
    assert result.checked_files > 0
    assert result.drift == []


def test_freeze_check_detects_changed_file(tmp_path):
    outputs = tmp_path / "outputs"
    release = outputs / "releases" / "latest"
    _write_minimal_release_files(release)
    freeze_release(outputs, release_name="latest")

    (release / "reports" / "index.html").write_text("<html>changed</html>", encoding="utf-8")
    result = check_release_lock(outputs, release_name="latest")

    assert not result.ok
    assert any(item.path == "reports/index.html" and item.problem == "changed" for item in result.drift)


def test_freeze_check_reports_not_frozen(tmp_path):
    outputs = tmp_path / "outputs"
    release = outputs / "releases" / "latest"
    _write_minimal_release_files(release)

    result = check_release_lock(outputs, release_name="latest")

    assert result.status == "not_frozen"
    assert not result.ok


def test_freeze_refuses_to_overwrite_without_force(tmp_path):
    outputs = tmp_path / "outputs"
    release = outputs / "releases" / "latest"
    _write_minimal_release_files(release)
    freeze_release(outputs, release_name="latest")

    with pytest.raises(FileExistsError):
        freeze_release(outputs, release_name="latest")

    freeze_release(outputs, release_name="latest", force=True)
