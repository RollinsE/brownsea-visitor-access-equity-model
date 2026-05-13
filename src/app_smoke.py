# -*- coding: utf-8 -*-
"""Postcode app smoke-test helpers.

This module does not run the pipeline. It validates that a completed release or
build can be loaded by the Flask postcode app and that the key app routes return
usable responses.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from src.release_qa import resolve_release_target


@dataclass(frozen=True)
class EndpointCheck:
    name: str
    path: str
    status_code: int | None
    ok: bool
    problem: str | None = None


@dataclass(frozen=True)
class AppSmokeResult:
    target: str
    lookup_path: str
    status: str
    records: int
    endpoint_checks: list[EndpointCheck]
    summary: dict[str, Any]

    @property
    def ok(self) -> bool:
        return self.status == "pass"


def _load_lookup_rows(lookup_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(lookup_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected postcode lookup JSON to contain a list of records: {lookup_path}")
    rows = [row for row in payload if isinstance(row, dict)]
    if not rows:
        raise ValueError(f"Postcode lookup JSON contains no usable records: {lookup_path}")
    return rows


def _sample_postcode(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        value = row.get("postcode_clean") or row.get("postcode")
        if value:
            return str(value)
    raise ValueError("Could not find a sample postcode in postcode lookup records")


def _endpoint(client: Any, name: str, path: str, expected: set[int] | None = None) -> EndpointCheck:
    expected = expected or {200}
    try:
        response = client.get(path)
        status_code = int(response.status_code)
        ok = status_code in expected
        return EndpointCheck(
            name=name,
            path=path,
            status_code=status_code,
            ok=ok,
            problem=None if ok else f"expected {sorted(expected)}, got {status_code}",
        )
    except Exception as exc:
        return EndpointCheck(name=name, path=path, status_code=None, ok=False, problem=str(exc))


def smoke_test_app(target: str | Path = "outputs", release_name: str = "latest") -> AppSmokeResult:
    """Smoke-test the app against an outputs root, build directory, or release directory."""
    base = resolve_release_target(target, release_name=release_name)
    lookup_path = base / "artifacts" / "postcode_lookup.json"
    if not lookup_path.exists():
        raise FileNotFoundError(f"Postcode lookup JSON not found: {lookup_path}")

    rows = _load_lookup_rows(lookup_path)
    postcode = _sample_postcode(rows)

    try:
        from app.server import create_app
    except Exception as exc:
        raise RuntimeError(f"Could not import Flask app. Install Flask dependencies first. Details: {exc}") from exc

    try:
        app = create_app(lookup_path=str(lookup_path), outputs_root=str(base.parent.parent if "releases" in base.parts else base))
    except Exception as exc:
        raise RuntimeError(f"Could not create postcode app for {base}. Details: {exc}") from exc

    checks: list[EndpointCheck] = []
    with app.test_client() as client:
        checks.append(_endpoint(client, "home page", "/"))
        checks.append(_endpoint(client, "health", "/health"))
        checks.append(_endpoint(client, "postcode lookup", f"/api/lookup?postcode={quote_plus(postcode)}"))
        checks.append(_endpoint(client, "postcode JSON artifact", "/artifacts/postcode_lookup.json"))
        checks.append(_endpoint(client, "postcode CSV artifact", "/artifacts/postcode_lookup.csv"))
        checks.append(_endpoint(client, "reports index", "/reports/index.html"))
        checks.append(_endpoint(client, "postcode app report", "/reports/postcode_app.html"))
        checks.append(_endpoint(client, "downloads page", "/downloads"))
        checks.append(_endpoint(client, "help page", "/help"))
        checks.append(_endpoint(client, "reports bundle", "/downloads/reports.zip"))

    failures = [check for check in checks if not check.ok]
    status = "pass" if not failures else "fail"
    return AppSmokeResult(
        target=str(base),
        lookup_path=str(lookup_path),
        status=status,
        records=len(rows),
        endpoint_checks=checks,
        summary={
            "release_name": release_name,
            "endpoints_checked": len(checks),
            "endpoints_failed": len(failures),
            "sample_postcode": postcode,
        },
    )


def result_to_dict(result: AppSmokeResult) -> dict[str, Any]:
    return {
        "target": result.target,
        "lookup_path": result.lookup_path,
        "status": result.status,
        "records": result.records,
        "summary": result.summary,
        "endpoint_checks": [asdict(check) for check in result.endpoint_checks],
    }


def print_smoke_result(result: AppSmokeResult) -> None:
    print(f"App smoke test: {result.status.upper()}")
    print(f"  target: {result.target}")
    print(f"  lookup path: {result.lookup_path}")
    print(f"  records: {result.records}")
    print(f"  endpoints checked: {len(result.endpoint_checks)}")
    print(f"  endpoints failed: {sum(1 for check in result.endpoint_checks if not check.ok)}")

    failures = [check for check in result.endpoint_checks if not check.ok]
    if failures:
        print("\nFailed endpoints:")
        for check in failures:
            print(f"  {check.name}: {check.path} ({check.problem})")
    else:
        print("  all key app routes are responding")
