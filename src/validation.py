# -*- coding: utf-8 -*-
"""Validation helpers for strict pipeline contracts."""

import pandas as pd


def validate_selected_features(dataset: pd.DataFrame, feature_cols) -> None:
    missing = [col for col in feature_cols if col not in dataset.columns]
    if missing:
        raise ValueError(
            "Stage 2 input is missing required selected features: " + ", ".join(missing)
        )
