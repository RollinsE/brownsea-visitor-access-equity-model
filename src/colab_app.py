# -*- coding: utf-8 -*-
"""Colab helpers for launching the postcode app.

Use from a notebook cell:

    from src.colab_app import launch_postcode_app
    launch_postcode_app(outputs_root="/content/drive/MyDrive/brownsea/outputs")
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def stop_postcode_app(port: int = 8000) -> None:
    """Stop an existing app process for the requested port when running in Colab/Linux."""
    pattern = f"run_postcode_app.py.*--port {port}"
    try:
        subprocess.run(["pkill", "-f", pattern], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def _wait_for_health(port: int, timeout_seconds: int = 15) -> bool:
    deadline = time.time() + timeout_seconds
    url = f"http://127.0.0.1:{port}/health"
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2) as response:
                if 200 <= int(response.status) < 300:
                    return True
        except Exception:
            time.sleep(0.5)
    return False


def launch_postcode_app(
    outputs_root: str = "/content/drive/MyDrive/brownsea/outputs",
    *,
    port: int = 8000,
    release_name: str = "latest",
    open_mode: str = "window",
    stop_existing: bool = True,
) -> str:
    """Launch the postcode app in Colab and open the Colab proxy.

    Returns the local health URL. The visible browser URL is opened through
    google.colab.output because 127.0.0.1 links are internal to the runtime.
    """
    root = _project_root()
    log_path = Path(f"/tmp/brownsea_app_{port}.log")

    if stop_existing:
        stop_postcode_app(port=port)
        time.sleep(1)

    cmd = [
        sys.executable,
        "run_postcode_app.py",
        "--outputs-root",
        outputs_root,
        "--port",
        str(port),
    ]
    env = os.environ.copy()
    env.setdefault("BROWSEA_RELEASE_NAME", release_name)

    log_handle = log_path.open("w", encoding="utf-8")
    subprocess.Popen(cmd, cwd=str(root), stdout=log_handle, stderr=subprocess.STDOUT, env=env)

    health_url = f"http://127.0.0.1:{port}/health"
    if not _wait_for_health(port):
        print("Postcode app did not become ready.")
        print(f"Log file: {log_path}")
        try:
            print(log_path.read_text(encoding="utf-8")[-2000:])
        except Exception:
            pass
        return health_url

    print("Postcode app is running")
    print(f"  local health check: {health_url}")
    print(f"  log file: {log_path}")

    try:
        from google.colab import output  # type: ignore

        if open_mode == "iframe":
            output.serve_kernel_port_as_iframe(port, height=900)
        else:
            output.serve_kernel_port_as_window(port)
    except Exception as exc:
        print("Could not open Colab proxy automatically.")
        print(f"  {exc}")
        print("In a Colab cell, run:")
        print("  from google.colab import output")
        print(f"  output.serve_kernel_port_as_window({port})")

    return health_url
