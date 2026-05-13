from pathlib import Path

from src.notebook_viewer import find_latest_build, list_saved_outputs, resolve_output_base


def test_notebook_viewer_finds_latest_build_and_outputs(tmp_path):
    outputs = tmp_path / "outputs"
    old = outputs / "builds" / "20260101_000000"
    new = outputs / "builds" / "20260102_000000"
    (old / "reports").mkdir(parents=True)
    (new / "reports").mkdir(parents=True)
    (new / "artifacts").mkdir(parents=True)
    (new / "reports" / "index.html").write_text("<h1>Index</h1>", encoding="utf-8")
    (new / "artifacts" / "postcode_lookup.csv").write_text("postcode\nBH1", encoding="utf-8")

    assert find_latest_build(outputs) == new
    assert resolve_output_base(outputs) == new
    files = list_saved_outputs(new)
    names = {p.name for p in files}
    assert {"index.html", "postcode_lookup.csv"}.issubset(names)
