from __future__ import annotations

import json
from pathlib import Path

from src.app_smoke import smoke_test_app


def _write_release(base: Path) -> Path:
    release = base / "releases" / "latest"
    (release / "artifacts").mkdir(parents=True)
    (release / "reports").mkdir(parents=True)
    (release / "artifacts" / "postcode_lookup.json").write_text(
        json.dumps([{"postcode_clean": "BH137AA", "postcode": "BH13 7AA"}]),
        encoding="utf-8",
    )
    (release / "artifacts" / "postcode_lookup.csv").write_text("postcode\nBH13 7AA\n", encoding="utf-8")
    (release / "reports" / "index.html").write_text("<html>index</html>", encoding="utf-8")
    (release / "reports" / "postcode_app.html").write_text("<html>postcode app</html>", encoding="utf-8")
    return release


class FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


class FakeClient:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, path: str):
        expected = {
            "/",
            "/health",
            "/api/lookup?postcode=BH137AA",
            "/artifacts/postcode_lookup.json",
            "/artifacts/postcode_lookup.csv",
            "/reports/index.html",
            "/reports/postcode_app.html",
            "/downloads",
            "/help",
            "/downloads/reports.zip",
        }
        return FakeResponse(200 if path in expected else 404)


class FakeApp:
    def test_client(self):
        return FakeClient()


def test_smoke_test_app_checks_key_routes(tmp_path, monkeypatch):
    outputs = tmp_path / "outputs"
    _write_release(outputs)

    import app.server

    monkeypatch.setattr(app.server, "create_app", lambda *args, **kwargs: FakeApp())

    result = smoke_test_app(outputs, release_name="latest")

    assert result.ok
    assert result.records == 1
    assert result.summary["endpoints_checked"] == 10
    assert result.summary["endpoints_failed"] == 0
    assert {check.name for check in result.endpoint_checks} == {
        "home page",
        "health",
        "postcode lookup",
        "postcode JSON artifact",
        "postcode CSV artifact",
        "reports index",
        "postcode app report",
        "downloads page",
        "help page",
        "reports bundle",
    }


def test_smoke_test_app_reports_endpoint_failures(tmp_path, monkeypatch):
    outputs = tmp_path / "outputs"
    _write_release(outputs)

    class PartlyBrokenClient(FakeClient):
        def get(self, path: str):
            if path == "/reports/postcode_app.html":
                return FakeResponse(404)
            return FakeResponse(200)

    class PartlyBrokenApp:
        def test_client(self):
            return PartlyBrokenClient()

    import app.server

    monkeypatch.setattr(app.server, "create_app", lambda *args, **kwargs: PartlyBrokenApp())

    result = smoke_test_app(outputs, release_name="latest")

    assert not result.ok
    assert result.summary["endpoints_failed"] == 1
    failed = [check for check in result.endpoint_checks if not check.ok]
    assert failed[0].name == "postcode app report"
