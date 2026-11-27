# -*- coding: utf-8 -*-
"""Visit-rate targets and thresholds.

Kept free of third-party imports so the lightweight Flask app (which installs
only Flask and gunicorn) can use them.
"""

LOW_VISIT_RATE_THRESHOLD = 4
MEDIUM_VISIT_RATE_THRESHOLD = 7
TARGET_VISIT_RATE = 10
