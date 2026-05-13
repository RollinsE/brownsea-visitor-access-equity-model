#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Launch the postcode app with Colab-friendly defaults.

This is most useful when called from a notebook with:

    from src.colab_app import launch_postcode_app
    launch_postcode_app(outputs_root="/content/drive/MyDrive/brownsea/outputs")

The CLI entry point is provided for completeness, but browser opening works best
from a notebook cell because Colab owns the proxy UI there.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.colab_app import launch_postcode_app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Launch the Brownsea postcode app in Colab.")
    parser.add_argument("--outputs-root", default="/content/drive/MyDrive/brownsea/outputs")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--release-name", default="latest")
    parser.add_argument("--open-mode", choices=("window", "iframe", "none"), default="window")
    parser.add_argument("--keep-existing", action="store_true", help="Do not stop an existing app process for this port first")
    args = parser.parse_args(argv)

    open_mode = args.open_mode if args.open_mode != "none" else "window"
    launch_postcode_app(
        outputs_root=args.outputs_root,
        port=args.port,
        release_name=args.release_name,
        open_mode=open_mode,
        stop_existing=not args.keep_existing,
    )
    if args.open_mode == "none":
        print("Open the Colab proxy from a notebook cell:")
        print("  from google.colab import output")
        print(f"  output.serve_kernel_port_as_window({args.port})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
