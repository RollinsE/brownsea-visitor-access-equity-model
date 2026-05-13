import json
from pathlib import Path

from src.release_manager import (
    prepare_build_directory,
    promote_release,
    write_promoted_release_manifest,
    write_release_pointer,
)
from src.release_qa import validate_release, write_release_manifest


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


def test_validate_release_passes_for_minimal_release(tmp_path):
    release = tmp_path / "outputs" / "releases" / "latest"
    _write_minimal_release_files(release)
    write_release_pointer(tmp_path / "outputs", "latest", "run123")
    result = validate_release(tmp_path / "outputs", release_name="latest")
    assert result.ok
    assert result.summary["required_missing"] == 0
    assert result.summary["pointer"]["ok"] is True


def test_validate_release_fails_when_required_app_file_missing(tmp_path):
    release = tmp_path / "outputs" / "releases" / "latest"
    _write_minimal_release_files(release)
    (release / "artifacts" / "postcode_lookup.json").unlink()
    result = validate_release(release)
    assert not result.ok
    assert any(item.path == "artifacts/postcode_lookup.json" for item in result.required_missing)


def test_write_release_manifest_records_hashes_and_qa_status(tmp_path):
    release = tmp_path / "outputs" / "releases" / "latest"
    _write_minimal_release_files(release)
    manifest_path = write_release_manifest(release, release_name="latest", run_id="run123")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "release-qa-v1"
    assert payload["qa_status"] == "pass"
    assert any(item["path"] == "artifacts/postcode_lookup.json" and item.get("sha256") for item in payload["files"])


def test_promoted_release_manifest_is_written_for_released_build(tmp_path):
    output_root = tmp_path / "outputs"
    build = prepare_build_directory(output_root, "run123")
    _write_minimal_release_files(build)
    release = promote_release(build, output_root, "latest")
    manifest = write_promoted_release_manifest(
        release,
        output_root=output_root,
        release_name="latest",
        run_id="run123",
        source_build=build,
        route_cache_dir=output_root / "cache" / "route_cache",
    )
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["source_build"] == str(build.resolve())
    assert payload["route_cache_dir"] == str((output_root / "cache" / "route_cache").resolve())
    assert payload["qa_status"] == "pass"


def test_validate_release_can_repair_three_way_alias(tmp_path):
    release = tmp_path / "outputs" / "releases" / "latest"
    _write_minimal_release_files(release)
    canonical = release / "artifacts" / "three_way_intersection_analysis_v2.csv"
    canonical.unlink()
    alias_dir = release / "artifacts" / "tables"
    alias_dir.mkdir(parents=True, exist_ok=True)
    (alias_dir / "analysis_table.csv").write_text("District,value\nBH1,1\n", encoding="utf-8")

    result = validate_release(release, repair_aliases=True)

    assert result.ok
    assert canonical.exists()
    assert result.summary["repaired_aliases"] == [
        {"created": "artifacts/three_way_intersection_analysis_v2.csv", "from": "artifacts/tables/analysis_table.csv"}
    ]
