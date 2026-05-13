
# -*- coding: utf-8 -*-
"""Persistent route cache with simple versioned metadata."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

LOG = logging.getLogger("Brownsea_Equity_Analysis")


def file_sha256(path: str | Path | None) -> str:
    if not path:
        return ""
    p = Path(path)
    if not p.exists() or not p.is_file():
        return ""
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def expected_route_cache_metadata(*, scope: str, profile: str, cache_version: str, competitor_file: str | Path | None = None, shortlist_size: int | None = None) -> Dict:
    return {
        'scope': scope,
        'profile': profile,
        'cache_version': cache_version,
        'competitor_file_hash': file_sha256(competitor_file) if competitor_file else '',
        'competitor_shortlist_size': shortlist_size if shortlist_size is not None else None,
    }


ROUTE_CACHE_COMPATIBILITY_KEYS = ("scope", "profile", "cache_version")


def _compatible_metadata(existing_metadata: Dict, expected_metadata: Dict) -> bool:
    """Return True when an existing route cache can safely be reused.

    The cache itself is keyed by origin/destination coordinates and route scope.
    Competitor shortlist size and competitor reference-file hash affect which routes
    may be requested in a run, but they do not make already cached coordinate-level
    route durations invalid. Only the ORS profile, cache schema version, and scope
    should force invalidation.
    """
    for key in ROUTE_CACHE_COMPATIBILITY_KEYS:
        if existing_metadata.get(key) != expected_metadata.get(key):
            return False
    return True


def load_route_cache(cache_path: str | Path, metadata_path: str | Path, expected_metadata: Dict) -> Tuple[Dict, bool]:
    cache_path = Path(cache_path)
    metadata_path = Path(metadata_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    if not cache_path.exists() or not metadata_path.exists():
        return {}, False

    try:
        existing_metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
    except Exception as exc:
        LOG.warning(f"Could not read route cache metadata {metadata_path}: {exc}")
        return {}, False

    if not _compatible_metadata(existing_metadata, expected_metadata):
        changed = [
            key for key in ROUTE_CACHE_COMPATIBILITY_KEYS
            if existing_metadata.get(key) != expected_metadata.get(key)
        ]
        LOG.info(f"Invalidating route cache {cache_path.name} because compatibility metadata changed: {changed}")
        return {}, False

    try:
        cache = json.loads(cache_path.read_text(encoding='utf-8'))
        return cache, True
    except Exception as exc:
        LOG.warning(f"Could not read route cache {cache_path}: {exc}")
        return {}, False


def save_route_cache(cache: Dict, cache_path: str | Path, metadata_path: str | Path, metadata: Dict) -> None:
    cache_path = Path(cache_path)
    metadata_path = Path(metadata_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    cache_path.write_text(json.dumps(cache, indent=2), encoding='utf-8')
    metadata_to_write = dict(metadata)
    metadata_to_write['saved_at'] = datetime.now().isoformat()
    metadata_to_write['route_count'] = len(cache)
    metadata_path.write_text(json.dumps(metadata_to_write, indent=2), encoding='utf-8')


def cache_stats(cache: Dict) -> str:
    return f"{len(cache)} cached routes"
