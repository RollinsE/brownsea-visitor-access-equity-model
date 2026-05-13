from pathlib import Path

from src.config import apply_output_directory


def test_route_cache_is_shared_across_versioned_builds(tmp_path):
    output_root = tmp_path / "outputs"
    first = apply_output_directory({"output_files": {}}, str(output_root / "builds" / "run_a"))
    second = apply_output_directory({"output_files": {}}, str(output_root / "builds" / "run_b"))

    expected = output_root / "cache" / "route_cache"
    assert Path(first["route_cache_dir"]) == expected
    assert Path(second["route_cache_dir"]) == expected
    assert expected.exists()
    assert str(expected) not in str(output_root / "builds" / "run_a")
    assert str(expected) not in str(output_root / "builds" / "run_b")
