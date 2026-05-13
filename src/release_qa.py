# -*- coding: utf-8 -*-
"""Release-candidate QA and manifest helpers.

This module deliberately does not run the pipeline. It inspects a completed
build or promoted release and reports whether the files needed by the app,
notebook viewer, stage reruns, and release audit trail are present and readable.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "release-qa-v1"

REQUIRED_FILES: tuple[tuple[str, str], ...] = (
    ("run manifest", "run_manifest.json"),
    ("postcode lookup JSON", "artifacts/postcode_lookup.json"),
    ("postcode lookup CSV", "artifacts/postcode_lookup.csv"),
    ("postcode lookup report", "reports/postcode_lookup.html"),
    ("postcode app report", "reports/postcode_app.html"),
    ("reports index", "reports/index.html"),
    ("three-way analysis", "artifacts/three_way_intersection_analysis_v2.csv"),
)

RECOMMENDED_FILES: tuple[tuple[str, str], ...] = (
    ("stage run manifest", "stage_run_manifest.json"),
    ("release manifest", "release_manifest.json"),
    ("model performance CSV", "artifacts/model_performance.csv"),
    ("model performance summary", "artifacts/model_performance_summary.json"),
    ("model performance report", "reports/model_performance.html"),
    ("model bundle checkpoint", "checkpoints/model_bundle.joblib"),
    ("LSOA master checkpoint", "checkpoints/lsoa_master.parquet"),
    ("district-LSOA map checkpoint", "checkpoints/district_lsoa_map.parquet"),
    ("ONS checkpoint", "checkpoints/ons_clean.parquet"),
)

JSON_FILES: tuple[str, ...] = (
    "run_manifest.json",
    "stage_run_manifest.json",
    "release_manifest.json",
    "artifacts/postcode_lookup.json",
    "artifacts/model_performance_summary.json",
)

ARTIFACT_ALIASES: dict[str, tuple[str, ...]] = {
    "artifacts/three_way_intersection_analysis_v2.csv": (
        "artifacts/tables/analysis_table.csv",
        "artifacts/tables/district_analysis_export.csv",
    ),
}


@dataclass(frozen=True)
class FileCheck:
    label: str
    path: str
    exists: bool
    required: bool
    size_bytes: int | None = None
    problem: str | None = None


@dataclass(frozen=True)
class QAResult:
    target: str
    status: str
    required_missing: list[FileCheck]
    recommended_missing: list[FileCheck]
    invalid_files: list[FileCheck]
    checked_files: list[FileCheck]
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
    try:
        return path.relative_to(base).as_posix()
    except ValueError:
        return str(path)


def resolve_release_target(path: str | Path, release_name: str = "latest") -> Path:
    """Resolve either an outputs root, build directory, or release directory."""
    candidate = Path(path).expanduser().resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"QA target does not exist: {candidate}")

    release_candidate = candidate / "releases" / release_name
    if release_candidate.exists() and release_candidate.is_dir():
        return release_candidate

    if (candidate / "artifacts").exists() or (candidate / "reports").exists():
        return candidate

    raise FileNotFoundError(
        f"Could not resolve QA target. Expected an outputs root containing releases/{release_name}, "
        f"or a build/release directory with artifacts/ or reports/: {candidate}"
    )


def _check_file(base: Path, label: str, relative_path: str, *, required: bool) -> FileCheck:
    path = base / relative_path
    if not path.exists():
        return FileCheck(label=label, path=relative_path, exists=False, required=required, problem="missing")
    if not path.is_file():
        return FileCheck(label=label, path=relative_path, exists=True, required=required, problem="not a file")
    size = path.stat().st_size
    if size <= 0:
        return FileCheck(label=label, path=relative_path, exists=True, required=required, size_bytes=size, problem="empty file")
    return FileCheck(label=label, path=relative_path, exists=True, required=required, size_bytes=size)


def _validate_json_files(base: Path) -> list[FileCheck]:
    invalid: list[FileCheck] = []
    for relative_path in JSON_FILES:
        path = base / relative_path
        if not path.exists() or not path.is_file():
            continue
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            invalid.append(
                FileCheck(
                    label="JSON validity",
                    path=relative_path,
                    exists=True,
                    required=relative_path in {p for _, p in REQUIRED_FILES},
                    size_bytes=path.stat().st_size,
                    problem=f"invalid JSON: {exc}",
                )
            )
    return invalid


def _load_run_manifest(base: Path) -> dict[str, Any]:
    manifest_path = base / "run_manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _pointer_check(base: Path) -> dict[str, Any]:
    """Return release pointer information when QA target is a promoted release."""
    parts = base.parts
    if "releases" not in parts:
        return {"checked": False, "reason": "target is not under a releases directory"}
    idx = parts.index("releases")
    if idx == 0:
        return {"checked": False, "reason": "could not infer output root"}
    output_root = Path(*parts[:idx])
    release_name = parts[idx + 1] if len(parts) > idx + 1 else base.name
    pointer_path = output_root / "releases" / "release_pointer.json"
    if not pointer_path.exists():
        return {"checked": True, "path": str(pointer_path), "exists": False, "ok": False}
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"checked": True, "path": str(pointer_path), "exists": True, "ok": False, "problem": str(exc)}
    expected = str(base.resolve())
    return {
        "checked": True,
        "path": str(pointer_path),
        "exists": True,
        "ok": pointer.get("release_name") == release_name and Path(pointer.get("path", "")).resolve() == base.resolve(),
        "release_name": pointer.get("release_name"),
        "run_id": pointer.get("run_id"),
        "target_path": pointer.get("path"),
        "expected_path": expected,
    }


def repair_artifact_aliases(base: Path) -> list[dict[str, str]]:
    """Create canonical files from recognised artifact aliases when available.

    This repair is intentionally narrow and non-destructive. It only copies from
    recognised aliases to missing canonical files, and it never overwrites an
    existing canonical artifact.
    """
    repaired: list[dict[str, str]] = []
    for canonical, aliases in ARTIFACT_ALIASES.items():
        destination = base / canonical
        if destination.exists():
            continue
        for alias in aliases:
            source = base / alias
            if source.exists() and source.is_file() and source.stat().st_size > 0:
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(source.read_bytes())
                repaired.append({"created": canonical, "from": alias})
                break
    return repaired


def validate_release(target: str | Path, release_name: str = "latest", *, repair_aliases: bool = False) -> QAResult:
    base = resolve_release_target(target, release_name=release_name)
    repaired_aliases = repair_artifact_aliases(base) if repair_aliases else []

    checks: list[FileCheck] = []
    for label, relative_path in REQUIRED_FILES:
        checks.append(_check_file(base, label, relative_path, required=True))
    for label, relative_path in RECOMMENDED_FILES:
        checks.append(_check_file(base, label, relative_path, required=False))

    invalid = _validate_json_files(base)
    required_missing = [c for c in checks if c.required and c.problem]
    recommended_missing = [c for c in checks if not c.required and c.problem]
    required_invalid = [c for c in invalid if c.required]

    manifest = _load_run_manifest(base)
    pointer = _pointer_check(base)
    status = "pass" if not required_missing and not required_invalid else "fail"

    summary = {
        "target": str(base),
        "run_id": manifest.get("run_id"),
        "runtime": manifest.get("runtime"),
        "stage_plan": manifest.get("stage_plan"),
        "required_files": len(REQUIRED_FILES),
        "required_missing": len(required_missing),
        "recommended_files": len(RECOMMENDED_FILES),
        "recommended_missing": len(recommended_missing),
        "invalid_files": len(invalid),
        "pointer": pointer,
        "repaired_aliases": repaired_aliases,
    }

    return QAResult(
        target=str(base),
        status=status,
        required_missing=required_missing,
        recommended_missing=recommended_missing,
        invalid_files=invalid,
        checked_files=checks,
        summary=summary,
    )


def iter_manifest_files(base: Path) -> Iterable[dict[str, Any]]:
    for path in sorted(p for p in base.rglob("*") if p.is_file()):
        rel = _rel(path, base)
        # Avoid recursively changing the manifest hash every time it is rewritten.
        if rel == "release_manifest.json":
            continue
        try:
            stat = path.stat()
            yield {
                "path": rel,
                "size_bytes": stat.st_size,
                "sha256": _sha256(path),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
        except Exception as exc:
            yield {"path": rel, "problem": str(exc)}


def write_release_manifest(
    release_dir: str | Path,
    *,
    release_name: str = "latest",
    run_id: str | None = None,
    source_build: str | Path | None = None,
    output_root: str | Path | None = None,
    route_cache_dir: str | Path | None = None,
) -> Path:
    """Write an auditable manifest for a promoted release or completed build."""
    base = Path(release_dir).expanduser().resolve()
    base.mkdir(parents=True, exist_ok=True)
    run_manifest = _load_run_manifest(base)
    result = validate_release(base, release_name=release_name)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(),
        "release_name": release_name,
        "run_id": run_id or run_manifest.get("run_id"),
        "source_build": str(Path(source_build).expanduser().resolve()) if source_build else None,
        "release_dir": str(base),
        "output_root": str(Path(output_root).expanduser().resolve()) if output_root else run_manifest.get("output_root"),
        "route_cache_dir": str(Path(route_cache_dir).expanduser().resolve()) if route_cache_dir else run_manifest.get("route_cache_dir"),
        "qa_status": result.status,
        "qa_summary": result.summary,
        "files": list(iter_manifest_files(base)),
    }
    manifest_path = base / "release_manifest.json"
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return manifest_path


def result_to_dict(result: QAResult) -> dict[str, Any]:
    return {
        "target": result.target,
        "status": result.status,
        "summary": result.summary,
        "required_missing": [asdict(c) for c in result.required_missing],
        "recommended_missing": [asdict(c) for c in result.recommended_missing],
        "invalid_files": [asdict(c) for c in result.invalid_files],
        "checked_files": [asdict(c) for c in result.checked_files],
    }


def print_qa_result(result: QAResult) -> None:
    print(f"Release QA: {result.status.upper()}")
    print(f"  target: {result.target}")
    print(f"  required missing: {len(result.required_missing)}")
    print(f"  recommended missing: {len(result.recommended_missing)}")
    print(f"  invalid files: {len(result.invalid_files)}")

    if result.required_missing:
        print("\nMissing/invalid required files:")
        for item in result.required_missing:
            print(f"  {item.label}: {item.path} ({item.problem})")

    required_invalid = [item for item in result.invalid_files if item.required]
    if required_invalid:
        print("\nInvalid required files:")
        for item in required_invalid:
            print(f"  {item.path}: {item.problem}")

    if result.recommended_missing:
        print("\nRecommended files not found:")
        for item in result.recommended_missing:
            print(f"  {item.label}: {item.path} ({item.problem})")

    repaired = result.summary.get("repaired_aliases") or []
    if repaired:
        print("\nLegacy aliases repaired:")
        for item in repaired:
            print(f"  created {item.get('created')} from {item.get('from')}")

    pointer = result.summary.get("pointer", {})
    if pointer.get("checked") and not pointer.get("ok"):
        print("\nRelease pointer warning:")
        print(f"  {pointer}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a Brownsea build/release without rerunning the pipeline.")
    parser.add_argument("target", nargs="?", default="outputs", help="Outputs root, build directory, or release directory")
    parser.add_argument("--release-name", default="latest", help="Release name when target is an outputs root")
    parser.add_argument("--json", action="store_true", help="Print full QA result as JSON")
    parser.add_argument("--write-manifest", action="store_true", help="Write/update release_manifest.json for the target")
    parser.add_argument("--repair-aliases", action="store_true", help="Create missing canonical artifacts from recognised artifact aliases")
    args = parser.parse_args(argv)

    try:
        result = validate_release(args.target, release_name=args.release_name, repair_aliases=args.repair_aliases)
        if args.write_manifest:
            manifest_path = write_release_manifest(result.target, release_name=args.release_name)
            result = validate_release(args.target, release_name=args.release_name, repair_aliases=args.repair_aliases)
            if not args.json:
                print(f"Release manifest written: {manifest_path}")

        if args.json:
            print(json.dumps(result_to_dict(result), indent=2))
        else:
            print_qa_result(result)

        return 0 if result.ok else 1
    except Exception as exc:
        if args.json:
            print(json.dumps({"status": "error", "error": str(exc)}, indent=2))
        else:
            print(f"Release QA: ERROR")
            print(f"  {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
