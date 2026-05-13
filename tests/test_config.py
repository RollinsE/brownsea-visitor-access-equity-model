from pathlib import Path

from src.config import apply_output_directory


def test_apply_output_directory_builds_expected_layout(tmp_path):
    config = {"output_files": {}}
    config = apply_output_directory(config, str(tmp_path / "outputs"))
    assert Path(config["artifact_dir"]).exists()
    assert Path(config["report_dir"]).exists()
    assert Path(config["checkpoint_dir"]).exists()
    assert Path(config["log_dir"]).exists()
    assert config["output_files"]["postcode_lookup_html"].endswith("reports/postcode_lookup.html")
