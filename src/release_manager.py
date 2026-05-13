# -*- coding: utf-8 -*-
"""Build and release management helpers."""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Optional


def prepare_build_directory(output_root: str | Path, run_id: str) -> Path:
    root = Path(output_root)
    build_dir = root / 'builds' / run_id
    build_dir.mkdir(parents=True, exist_ok=True)
    return build_dir


def promote_release(build_dir: str | Path, output_root: str | Path, release_name: str = 'latest') -> Path:
    build_dir = Path(build_dir)
    output_root = Path(output_root)
    release_dir = output_root / 'releases' / release_name
    if release_dir.exists():
        shutil.rmtree(release_dir)
    release_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(build_dir, release_dir)
    return release_dir


def write_release_pointer(output_root: str | Path, release_name: str, run_id: str) -> Path:
    output_root = Path(output_root)
    pointer_path = output_root / 'releases' / 'release_pointer.json'
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'release_name': release_name,
        'run_id': run_id,
        'path': str((output_root / 'releases' / release_name).resolve()),
    }
    pointer_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    return pointer_path


def find_latest_release_lookup(outputs_root: str | Path) -> Optional[Path]:
    outputs_root = Path(outputs_root)
    candidates = [
        outputs_root / 'releases' / 'latest' / 'artifacts' / 'postcode_lookup.json',
        outputs_root / 'releases' / 'latest' / 'artifacts' / 'postcode_lookup.parquet',
        outputs_root / 'artifacts' / 'postcode_lookup.json',
        outputs_root / 'artifacts' / 'postcode_lookup.parquet',
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def write_promoted_release_manifest(
    release_dir: str | Path,
    *,
    output_root: str | Path,
    release_name: str = 'latest',
    run_id: str | None = None,
    source_build: str | Path | None = None,
    route_cache_dir: str | Path | None = None,
) -> Path:
    """Write an auditable release manifest for a promoted build.

    Kept as a thin wrapper so release promotion remains simple and existing
    callers/tests do not need to change.
    """
    from src.release_qa import write_release_manifest

    return write_release_manifest(
        release_dir,
        release_name=release_name,
        run_id=run_id,
        source_build=source_build,
        output_root=output_root,
        route_cache_dir=route_cache_dir,
    )
