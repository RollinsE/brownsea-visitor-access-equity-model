# -*- coding: utf-8 -*-
"""Notebook-only helpers for viewing saved Brownsea pipeline outputs.

The pipeline remains file-first. Import these helpers in Colab/Jupyter after a
run to display already-saved reports, PNGs, and HTML files without forcing
notebook rendering through ``!python cli.py``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable


def find_latest_build(output_root: str | Path = "outputs") -> Path:
    """Return the newest build directory under outputs/builds."""
    builds_dir = Path(output_root) / "builds"
    builds = sorted([p for p in builds_dir.glob("*") if p.is_dir()])
    if not builds:
        raise FileNotFoundError(f"No builds found under {builds_dir}")
    return builds[-1]


def find_release(output_root: str | Path = "outputs", release_name: str = "latest") -> Path:
    """Return a promoted release directory."""
    release_dir = Path(output_root) / "releases" / release_name
    if not release_dir.exists():
        pointer = Path(output_root) / "releases" / "release_pointer.json"
        if pointer.exists():
            try:
                payload = json.loads(pointer.read_text(encoding="utf-8"))
                pointed_name = payload.get("release_name")
                if pointed_name:
                    release_dir = Path(output_root) / "releases" / pointed_name
            except Exception:
                pass
    if not release_dir.exists():
        raise FileNotFoundError(f"Release not found: {release_dir}")
    return release_dir


def resolve_output_base(
    output_root: str | Path = "outputs",
    *,
    build_dir: str | Path | None = None,
    release_name: str | None = None,
) -> Path:
    """Resolve the build/release folder to view."""
    if build_dir:
        base = Path(build_dir)
    elif release_name:
        base = find_release(output_root, release_name)
    else:
        base = find_latest_build(output_root)
    if not base.exists():
        raise FileNotFoundError(f"Output folder not found: {base}")
    return base


def list_saved_outputs(base: str | Path, *, max_items: int = 200) -> list[Path]:
    """List saved report/artifact files in a build or release directory."""
    base = Path(base)
    candidates: list[Path] = []
    for subdir in (base / "reports", base / "artifacts"):
        if subdir.exists():
            candidates.extend(
                p for p in subdir.rglob("*")
                if p.is_file() and p.suffix.lower() in {".html", ".png", ".csv", ".json", ".parquet", ".xlsx"}
            )
    return sorted(candidates)[:max_items]


def print_saved_outputs(base: str | Path, *, max_items: int = 80) -> None:
    """Print a compact list of saved outputs. Safe outside notebooks."""
    base = Path(base)
    print(f"Saved Brownsea outputs: {base}")
    files = list_saved_outputs(base, max_items=max_items)
    if not files:
        print("  No saved report/artifact files found yet.")
        return
    for path in files:
        print(f"  - {path.relative_to(base)}")


def display_saved_outputs(
    output_root: str | Path = "outputs",
    *,
    build_dir: str | Path | None = None,
    release_name: str | None = None,
    show_html: bool = True,
    show_png: bool = True,
    max_items: int = 20,
) -> Path:
    """Display saved outputs in a notebook and return the viewed base folder.

    This function imports IPython lazily so the CLI pipeline never depends on
    notebook display behaviour.
    """
    base = resolve_output_base(output_root, build_dir=build_dir, release_name=release_name)
    print_saved_outputs(base, max_items=max_items)

    try:
        from IPython.display import HTML, Image, display
    except Exception as exc:
        print(f"Notebook display is unavailable in this environment: {exc}")
        return base

    report_dir = base / "reports"
    preferred = [
        report_dir / "index.html",
        report_dir / "executive_summary.html",
        report_dir / "model_performance.html",
        report_dir / "postcode_lookup.html",
        report_dir / "figures" / "priority_action_matrix.png",
        report_dir / "figures" / "growth_opportunity_matrix.png",
        report_dir / "figures" / "safe_zone_analysis.png",
        report_dir / "figures" / "shap_summary.png",
    ]

    shown = 0
    for path in preferred:
        if shown >= max_items or not path.exists():
            continue
        if path.suffix.lower() == ".png" and show_png:
            display(Image(filename=str(path)))
            shown += 1
        elif path.suffix.lower() == ".html" and show_html:
            html = path.read_text(encoding="utf-8", errors="replace")
            display(HTML(html))
            shown += 1

    if shown == 0:
        print("No preferred HTML/PNG outputs were available to display yet.")
    return base


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="View saved Brownsea pipeline outputs without rerunning the pipeline.")
    parser.add_argument("--output-root", default="outputs")
    parser.add_argument("--build-dir", default=None)
    parser.add_argument("--release-name", default=None)
    parser.add_argument("--max-items", type=int, default=80)
    args = parser.parse_args(list(argv) if argv is not None else None)

    base = resolve_output_base(args.output_root, build_dir=args.build_dir, release_name=args.release_name)
    print_saved_outputs(base, max_items=args.max_items)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
