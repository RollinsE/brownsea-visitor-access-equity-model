"""Gunicorn entry point for the Brownsea postcode web app.

Environment variables:
- BROWNSEA_OUTPUTS_ROOT: path to outputs root containing releases/latest
- BROWNSEA_LOOKUP_PATH: optional direct path to postcode_lookup.json
"""

from __future__ import annotations

import os

from app.server import create_app

outputs_root = os.getenv("BROWNSEA_OUTPUTS_ROOT", "outputs")
lookup_path = os.getenv("BROWNSEA_LOOKUP_PATH") or None

app = create_app(lookup_path=lookup_path, outputs_root=outputs_root)
application = app
