# -*- coding: utf-8 -*-
"""Freeze/check helpers for a promoted Brownsea release.

Freezing does not make Google Drive files physically read-only. It writes a
release_lock.json with file hashes so later checks can prove whether the release
candidate has drifted since it was frozen.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from src.release_qa import validate_release, resolve_release_target, write_release_manifest

LOCK_FILE = "release_lock.json"
SCHEMA_VERSION = "release-freeze-v1"


@dataclass(frozen=True)
class DriftItem:
    path: str
    problem: str
    expected_sha256: str | None = None
    actual_sha256: str | None = None


@dataclass(frozen=True)
class FreezeCheckResult:
    target: str
    status: str
    lock_path: str
    frozen_at: str | None
    release_name: str | None
    run_id: str | None
    checked_files: int
    drift: list[DriftItem]
    extra_files: list[str]
    summary: dict[str, Any]

    @property
    def ok(self) -> bool:
        return self.status == "pass"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _rel(path: Path, base: Path) -> str:
    return path.relative_to(base).as_posix()


def _iter_files(base: Path) -> Iterable[Path]:
    for path in sorted(p for p in base.rglob("*") if p.is_file()):
        rel = _rel(path, base)
        if rel == LOCK_FILE:
            continue
        yield path


def _fingerprints(base: Path) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for path in _iter_files(base):
        stat = path.stat()
        files.append(
            {
                "path": _rel(path, base),
                "size_bytes": stat.st_size,
                "sha256": _sha256(path),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
        )
    return files


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def freeze_release(
    target: str | Path = "outputs",
    *,
    release_name: str = "latest",
    force: bool = False,
    write_manifest_first: bool = True,
) -> Path:
    """Write release_lock.json after QA passes."""
    base = resolve_release_target(target, release_name=release_name)
    lock_path = base / LOCK_FILE
    if lock_path.exists() and not force:
        raise FileExistsError(f"Release is already frozen: {lock_path}. Use --force to replace the lock.")

    if write_manifest_first:
        write_release_manifest(base, release_name=release_name)

    qa_result = validate_release(base, release_name=release_name)
    if not qa_result.ok:
        raise RuntimeError(
            f"Release QA must pass before freezing. Required missing={len(qa_result.required_missing)}, "
            f"invalid files={len(qa_result.invalid_files)}"
        )

    run_manifest = _load_json(base / "run_manifest.json")
    release_manifest = _load_json(base / "release_manifest.json")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "frozen_at": datetime.now().isoformat(),
        "release_name": release_name,
        "release_dir": str(base),
        "run_id": run_manifest.get("run_id") or release_manifest.get("run_id"),
        "source_build": release_manifest.get("source_build"),
        "qa_status_at_freeze": qa_result.status,
        "file_count": len(list(_iter_files(base))),
        "files": _fingerprints(base),
        "note": "This lock records file hashes for drift detection. It does not make the filesystem read-only.",
    }
    lock_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return lock_path


def check_release_lock(target: str | Path = "outputs", *, release_name: str = "latest", strict_extra_files: bool = True) -> FreezeCheckResult:
    """Check whether a frozen release has drifted."""
    base = resolve_release_target(target, release_name=release_name)
    lock_path = base / LOCK_FILE
    if not lock_path.exists():
        return FreezeCheckResult(
            target=str(base),
            status="not_frozen",
            lock_path=str(lock_path),
            frozen_at=None,
            release_name=release_name,
            run_id=None,
            checked_files=0,
            drift=[],
            extra_files=[],
            summary={"lock_exists": False},
        )

    lock = _load_json(lock_path)
    expected_files = {item.get("path"): item for item in lock.get("files", []) if isinstance(item, dict) and item.get("path")}
    drift: list[DriftItem] = []

    for rel, expected in expected_files.items():
        path = base / rel
        expected_hash = expected.get("sha256")
        if not path.exists():
            drift.append(DriftItem(path=rel, problem="missing", expected_sha256=expected_hash))
            continue
        if not path.is_file():
            drift.append(DriftItem(path=rel, problem="not a file", expected_sha256=expected_hash))
            continue
        actual_hash = _sha256(path)
        if expected_hash != actual_hash:
            drift.append(
                DriftItem(
                    path=rel,
                    problem="changed",
                    expected_sha256=expected_hash,
                    actual_sha256=actual_hash,
                )
            )

    current_files = {_rel(path, base) for path in _iter_files(base)}
    extra_files = sorted(current_files - set(expected_files))
    status = "pass" if not drift and (not strict_extra_files or not extra_files) else "fail"
    return FreezeCheckResult(
        target=str(base),
        status=status,
        lock_path=str(lock_path),
        frozen_at=lock.get("frozen_at"),
        release_name=lock.get("release_name") or release_name,
        run_id=lock.get("run_id"),
        checked_files=len(expected_files),
        drift=drift,
        extra_files=extra_files if strict_extra_files else [],
        summary={
            "lock_exists": True,
            "strict_extra_files": strict_extra_files,
            "drift_count": len(drift),
            "extra_file_count": len(extra_files) if strict_extra_files else 0,
        },
    )


def result_to_dict(result: FreezeCheckResult) -> dict[str, Any]:
    return {
        "target": result.target,
        "status": result.status,
        "lock_path": result.lock_path,
        "frozen_at": result.frozen_at,
        "release_name": result.release_name,
        "run_id": result.run_id,
        "checked_files": result.checked_files,
        "drift": [asdict(item) for item in result.drift],
        "extra_files": result.extra_files,
        "summary": result.summary,
    }


def print_freeze_result(result: FreezeCheckResult) -> None:
    print(f"Release freeze check: {result.status.upper()}")
    print(f"  target: {result.target}")
    print(f"  lock: {result.lock_path}")
    if result.frozen_at:
        print(f"  frozen at: {result.frozen_at}")
    if result.run_id:
        print(f"  run id: {result.run_id}")
    print(f"  checked files: {result.checked_files}")
    print(f"  drift: {len(result.drift)}")
    print(f"  extra files: {len(result.extra_files)}")

    if result.drift:
        print("\nRelease drift detected:")
        for item in result.drift[:20]:
            print(f"  {item.path}: {item.problem}")
        if len(result.drift) > 20:
            print(f"  ... {len(result.drift) - 20} more")

    if result.extra_files:
        print("\nExtra files since freeze:")
        for path in result.extra_files[:20]:
            print(f"  {path}")
        if len(result.extra_files) > 20:
            print(f"  ... {len(result.extra_files) - 20} more")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze or check a Brownsea release candidate without rerunning the pipeline.")
    parser.add_argument("target", nargs="?", default="outputs", help="Outputs root, build directory, or release directory")
    parser.add_argument("--release-name", default="latest", help="Release name when target is an outputs root")
    parser.add_argument("--check", action="store_true", help="Check an existing release_lock.json instead of writing one")
    parser.add_argument("--force", action="store_true", help="Replace an existing release_lock.json")
    parser.add_argument("--no-manifest-refresh", action="store_true", help="Do not refresh release_manifest.json before freezing")
    parser.add_argument("--allow-extra-files", action="store_true", help="When checking, do not fail because new files were added after freeze")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args(argv)

    try:
        if args.check:
            result = check_release_lock(
                args.target,
                release_name=args.release_name,
                strict_extra_files=not args.allow_extra_files,
            )
            if args.json:
                print(json.dumps(result_to_dict(result), indent=2))
            else:
                print_freeze_result(result)
            return 0 if result.ok else 1

        lock_path = freeze_release(
            args.target,
            release_name=args.release_name,
            force=args.force,
            write_manifest_first=not args.no_manifest_refresh,
        )
        result = check_release_lock(args.target, release_name=args.release_name)
        if args.json:
            payload = result_to_dict(result)
            payload["written"] = str(lock_path)
            print(json.dumps(payload, indent=2))
        else:
            print(f"Release frozen: {lock_path}")
            print_freeze_result(result)
        return 0 if result.ok else 1
    except Exception as exc:
        if args.json:
            print(json.dumps({"status": "error", "error": str(exc)}, indent=2))
        else:
            print("Release freeze: ERROR")
            print(f"  {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
