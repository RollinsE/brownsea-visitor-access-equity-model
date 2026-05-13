# -*- coding: utf-8 -*-
"""Project status checks that do not rerun the Brownsea pipeline."""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.release_freeze import check_release_lock, result_to_dict as freeze_result_to_dict
from src.release_qa import validate_release, result_to_dict as qa_result_to_dict, resolve_release_target


@dataclass(frozen=True)
class DoctorResult:
    output_root: str
    release_name: str
    release_target: str | None
    status: str
    qa_status: str
    freeze_status: str
    route_cache: dict[str, Any]
    release_pointer: dict[str, Any]
    summary: dict[str, Any]

    @property
    def ok(self) -> bool:
        return self.status == "pass"


def _count_cache_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "routes": 0}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            routes = payload.get("routes")
            if isinstance(routes, dict):
                count = len(routes)
            else:
                # Older cache versions were plain route dictionaries.
                count = len([key for key in payload.keys() if key != "metadata"])
        else:
            count = 0
        return {"path": str(path), "exists": True, "routes": count, "size_bytes": path.stat().st_size}
    except Exception as exc:
        return {"path": str(path), "exists": True, "routes": 0, "problem": str(exc)}


def inspect_route_cache(output_root: str | Path) -> dict[str, Any]:
    root = Path(output_root).expanduser().resolve()
    cache_dir = root / "cache" / "route_cache"
    brownsea = _count_cache_file(cache_dir / "brownsea_routes.json")
    competitor = _count_cache_file(cache_dir / "competitor_routes.json")
    return {
        "cache_dir": str(cache_dir),
        "exists": cache_dir.exists(),
        "brownsea_routes": brownsea,
        "competitor_routes": competitor,
        "total_routes": int(brownsea.get("routes", 0)) + int(competitor.get("routes", 0)),
    }


def inspect_release_pointer(output_root: str | Path, release_name: str = "latest") -> dict[str, Any]:
    root = Path(output_root).expanduser().resolve()
    pointer_path = root / "releases" / "release_pointer.json"
    expected_target = root / "releases" / release_name
    if not pointer_path.exists():
        return {"path": str(pointer_path), "exists": False, "ok": False, "expected_release": str(expected_target)}
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"path": str(pointer_path), "exists": True, "ok": False, "problem": str(exc)}
    target_path = Path(pointer.get("path", "")) if pointer.get("path") else None
    ok = pointer.get("release_name") == release_name and target_path is not None and target_path.resolve() == expected_target.resolve()
    return {
        "path": str(pointer_path),
        "exists": True,
        "ok": ok,
        "release_name": pointer.get("release_name"),
        "run_id": pointer.get("run_id"),
        "target_path": pointer.get("path"),
        "expected_path": str(expected_target),
    }


def diagnose_project(output_root: str | Path = "outputs", *, release_name: str = "latest") -> DoctorResult:
    root = Path(output_root).expanduser().resolve()
    release_target: str | None = None
    qa_status = "error"
    qa_summary: dict[str, Any]
    try:
        target = resolve_release_target(root, release_name=release_name)
        release_target = str(target)
        qa_result = validate_release(root, release_name=release_name)
        qa_status = qa_result.status
        qa_summary = qa_result_to_dict(qa_result)
    except Exception as exc:
        qa_summary = {"status": "error", "error": str(exc)}

    try:
        freeze_result = check_release_lock(root, release_name=release_name)
        freeze_status = freeze_result.status
        freeze_summary = freeze_result_to_dict(freeze_result)
    except Exception as exc:
        freeze_status = "error"
        freeze_summary = {"status": "error", "error": str(exc)}

    route_cache = inspect_route_cache(root)
    pointer = inspect_release_pointer(root, release_name=release_name)

    hard_fail = qa_status != "pass" or pointer.get("exists") is False
    status = "fail" if hard_fail else "pass"
    summary = {
        "qa": qa_summary,
        "freeze": freeze_summary,
        "route_cache": route_cache,
        "release_pointer": pointer,
    }
    return DoctorResult(
        output_root=str(root),
        release_name=release_name,
        release_target=release_target,
        status=status,
        qa_status=qa_status,
        freeze_status=freeze_status,
        route_cache=route_cache,
        release_pointer=pointer,
        summary=summary,
    )


def result_to_dict(result: DoctorResult) -> dict[str, Any]:
    return {
        "output_root": result.output_root,
        "release_name": result.release_name,
        "release_target": result.release_target,
        "status": result.status,
        "qa_status": result.qa_status,
        "freeze_status": result.freeze_status,
        "route_cache": result.route_cache,
        "release_pointer": result.release_pointer,
        "summary": result.summary,
    }


def print_doctor_result(result: DoctorResult) -> None:
    print(f"Project doctor: {result.status.upper()}")
    print(f"  output root: {result.output_root}")
    print(f"  release name: {result.release_name}")
    print(f"  release target: {result.release_target or 'not found'}")
    print(f"  release QA: {result.qa_status.upper()}")
    print(f"  freeze check: {result.freeze_status.upper()}")

    cache = result.route_cache
    print("\nRoute cache")
    print(f"  directory: {cache.get('cache_dir')}")
    print(f"  total routes: {cache.get('total_routes')}")
    print(f"  brownsea routes: {cache.get('brownsea_routes', {}).get('routes', 0)}")
    print(f"  competitor routes: {cache.get('competitor_routes', {}).get('routes', 0)}")

    pointer = result.release_pointer
    print("\nRelease pointer")
    print(f"  exists: {pointer.get('exists')}")
    print(f"  ok: {pointer.get('ok')}")
    if pointer.get("run_id"):
        print(f"  run id: {pointer.get('run_id')}")

    if result.status != "pass":
        print("\nAction needed")
        if result.qa_status != "pass":
            print("  Run release QA and fix missing required files before freezing.")
        if not pointer.get("exists"):
            print("  Promote a release or recreate the release pointer.")
    elif result.freeze_status == "not_frozen":
        print("\nSuggested next command")
        print(f"  python scripts/freeze_release.py {result.output_root} --release-name {result.release_name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect Brownsea project state without rerunning the pipeline.")
    parser.add_argument("output_root", nargs="?", default="outputs", help="Outputs root containing builds, cache, and releases")
    parser.add_argument("--release-name", default="latest", help="Release name to inspect")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args(argv)

    result = diagnose_project(args.output_root, release_name=args.release_name)
    if args.json:
        print(json.dumps(result_to_dict(result), indent=2))
    else:
        print_doctor_result(result)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
