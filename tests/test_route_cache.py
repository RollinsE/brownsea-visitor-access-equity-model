
from pathlib import Path

from src.route_cache import expected_route_cache_metadata, load_route_cache, save_route_cache


def test_route_cache_survives_shortlist_changes(tmp_path):
    cache_path = tmp_path / 'competitor.json'
    meta_path = tmp_path / 'competitor.meta.json'

    meta_v1 = expected_route_cache_metadata(scope='competitor', profile='driving-car', cache_version='v1', competitor_file=None, shortlist_size=12)
    save_route_cache({'a': {'duration': 1}}, cache_path, meta_path, meta_v1)

    cache_loaded, ok = load_route_cache(cache_path, meta_path, meta_v1)
    assert ok is True
    assert 'a' in cache_loaded

    # Route cache entries are keyed by coordinates. Shortlist size controls which
    # routes are requested, but it must not discard already cached route durations.
    meta_v2 = expected_route_cache_metadata(scope='competitor', profile='driving-car', cache_version='v1', competitor_file=None, shortlist_size=8)
    cache_loaded, ok = load_route_cache(cache_path, meta_path, meta_v2)
    assert ok is True
    assert 'a' in cache_loaded


def test_route_cache_invalidates_when_profile_changes(tmp_path):
    cache_path = tmp_path / 'competitor.json'
    meta_path = tmp_path / 'competitor.meta.json'

    meta_v1 = expected_route_cache_metadata(scope='competitor', profile='driving-car', cache_version='v1', competitor_file=None, shortlist_size=12)
    save_route_cache({'a': {'duration': 1}}, cache_path, meta_path, meta_v1)

    meta_v2 = expected_route_cache_metadata(scope='competitor', profile='foot-walking', cache_version='v1', competitor_file=None, shortlist_size=12)
    cache_loaded, ok = load_route_cache(cache_path, meta_path, meta_v2)
    assert ok is False
    assert cache_loaded == {}
