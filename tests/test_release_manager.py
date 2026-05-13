from pathlib import Path

from src.release_manager import prepare_build_directory, promote_release, write_release_pointer


def test_prepare_build_directory(tmp_path):
    build = prepare_build_directory(tmp_path / 'outputs', 'run123')
    assert build.exists()
    assert str(build).endswith('outputs/builds/run123')


def test_promote_release_and_pointer(tmp_path):
    build = prepare_build_directory(tmp_path / 'outputs', 'run123')
    (build / 'artifacts').mkdir(parents=True)
    (build / 'artifacts' / 'postcode_lookup.json').write_text('[]')
    release = promote_release(build, tmp_path / 'outputs', 'latest')
    assert (release / 'artifacts' / 'postcode_lookup.json').exists()
    pointer = write_release_pointer(tmp_path / 'outputs', 'latest', 'run123')
    assert pointer.exists()
