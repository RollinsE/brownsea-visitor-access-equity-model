import pandas as pd

from src.business_scoring import calculate_early_warnings, calculate_safe_zone_benchmarks, identify_quick_wins


def _districts():
    return pd.DataFrame({
        "District": ["A1", "A2", "A3", "A4"],
        "Post_Town": ["T"] * 4,
        "Authority_Name": ["LA"] * 4,
        "Population": [20000] * 4,
        "composite_need_score": [10.0, 10.0, 10.0, 10.0],
        "visits_per_1000": [1.0, 2.5, 9.0, 3.0],
        "predicted_visit_rate": [4.0, 3.0, 5.0, 2.0],
    }).assign(performance_gap=lambda d: d["predicted_visit_rate"] - d["visits_per_1000"])


def test_under_performance_flag_marks_districts_below_expected():
    data = calculate_safe_zone_benchmarks(_districts(), model_rmse=1.5)
    flagged = calculate_early_warnings(data)
    has_flag = flagged["risk_flags"].str.contains("UnderperformingPrediction")
    # A1 is 3.0 below expected (outside the 1.5 band). A2 is 0.5 below (inside it).
    # A3 visits far more than expected and must not be flagged.
    assert list(flagged.loc[has_flag, "District"]) == ["A1"]


def test_quick_wins_keep_their_rule_and_say_when_the_gap_is_within_model_error():
    data = calculate_safe_zone_benchmarks(_districts(), model_rmse=1.5)
    wins = identify_quick_wins(data).set_index("District")
    assert set(wins.index) == {"A1", "A2"}
    assert bool(wins.loc["A1", "gap_beyond_model_error"]) is True
    assert bool(wins.loc["A2", "gap_beyond_model_error"]) is False
