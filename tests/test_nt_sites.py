from pathlib import Path

from src.nt_sites import load_nt_sites


def test_load_nt_sites_returns_active_rows():
    path = Path(__file__).resolve().parents[1] / "data" / "reference" / "nt_sites.csv"
    df = load_nt_sites(path)
    assert not df.empty
    assert {"site_id", "site_name", "lat", "lon", "active"}.issubset(df.columns)


def test_load_nt_sites_excludes_brownsea_destination(tmp_path):
    path = tmp_path / "nt_sites.csv"
    path.write_text(
        "site_id,site_name,lat,lon,active\n"
        "1,Brownsea Island,50.689,-1.957,true\n"
        "2,Studland Bay,50.642,-1.943,true\n",
        encoding="utf-8",
    )
    df = load_nt_sites(path)
    assert "Brownsea Island" not in df["site_name"].tolist()
    assert "Studland Bay" in df["site_name"].tolist()
