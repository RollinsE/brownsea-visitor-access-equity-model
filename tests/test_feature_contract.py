import pandas as pd
import pytest

from src.validation import validate_selected_features


def test_validate_selected_features_passes_when_all_present():
    df = pd.DataFrame({"a": [1], "b": [2]})
    validate_selected_features(df, ["a", "b"])


def test_validate_selected_features_fails_when_missing():
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError):
        validate_selected_features(df, ["a", "b"])
