import pandas as pd

from src.routing_service import _candidate_competitors


def test_candidate_competitors_shortlists_and_excludes_brownsea():
    sites = pd.DataFrame([
        {"site_name": "Brownsea Island", "lat": 50.7, "lon": -1.9, "active": True},
        {"site_name": "A", "lat": 50.71, "lon": -1.91, "active": True},
        {"site_name": "B", "lat": 50.75, "lon": -1.95, "active": True},
        {"site_name": "C", "lat": 50.8, "lon": -2.0, "active": True},
    ])
    out = _candidate_competitors(50.70, -1.90, sites, 2)
    assert len(out) == 2
    assert 'Brownsea Island' not in out['site_name'].tolist()
