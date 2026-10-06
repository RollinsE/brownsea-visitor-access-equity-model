from __future__ import annotations

import functools

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")

from src.business_scoring import calculate_safe_zone_benchmarks  # noqa: E402
from src.constants import GeographicConstants, ModelConstants  # noqa: E402
from src.model_evaluation import (  # noqa: E402
    ENSEMBLE_NAME,
    JOURNEY_BASELINE,
    MEAN_BASELINE,
    area_mask,
    areas_without_real_features,
    clean_features,
    score_predictions,
    study_area_mask,
    train_and_evaluate,
)

FEATURES = ["total_journey_min", "imd_decile_mean", "avg_fsm%"]
PARAMS = {"optuna_trials": 2, "repeated_cv_repeats": 1, "n_splits_cv": 4, "random_state": 0}


def _districts(signal: bool = True, n_outside: int = 300, seed: int = 0) -> pd.DataFrame:
    """Made-up districts: 40 in the study area with real features, the rest placeholders with ~zero visits."""
    rng = np.random.default_rng(seed)
    n = 40
    names = [f"BH{i}" for i in range(1, 21)] + [f"DT{i}" for i in range(1, 11)] + [f"SP{i}" for i in range(1, 11)]
    journey = rng.uniform(10, 100, n)
    imd = rng.uniform(2, 9, n)
    inside = pd.DataFrame({
        "District": names,
        "Authority_Name": ["A"] * 14 + ["B"] * 12 + ["C"] * 8 + ["D"] * 6,
        "total_journey_min": journey,
        "imd_decile_mean": imd,
        "avg_fsm%": 45 - imd * 4 + rng.normal(0, 3, n),
        "Population": rng.integers(8000, 50000, n).astype(float),
    })
    true_rate = np.exp(2.6 - 0.025 * journey + 0.1 * (imd - 5)) if signal else np.full(n, 4.0)
    inside["Visits"] = rng.poisson(true_rate * np.exp(rng.normal(0, 0.15, n)) * inside["Population"] / 1000).astype(float)

    outside = pd.DataFrame({
        "District": [f"ZZ{i}" for i in range(n_outside)],
        "Authority_Name": [f"LA{i % 20}" for i in range(n_outside)],
        "total_journey_min": 120.0,
        "imd_decile_mean": rng.uniform(2, 9, n_outside),
        "avg_fsm%": rng.uniform(5, 40, n_outside),
        "Population": rng.integers(8000, 50000, n_outside).astype(float),
        "Visits": 0.0,
    })
    return pd.concat([inside, outside], ignore_index=True)


def _run(data: pd.DataFrame, **overrides):
    return train_and_evaluate(
        data[FEATURES], data["Visits"], data["Population"], {**PARAMS, **overrides},
        data["Authority_Name"], data["District"],
    )


@functools.lru_cache(maxsize=None)
def _standard_run():
    """One tuned run on the standard made-up data, shared by the tests that only read its results."""
    data = _districts()
    return (data, *_run(data))


def test_study_area_mask_matches_bh_dt_sp_only():
    districts = pd.Series(["BH1", "dt11", " SP9", "SO14", "B1", "TA1"])
    assert study_area_mask(districts).tolist() == [True, True, True, False, False, False]


def test_clean_features_treats_impossible_zeros_as_missing_and_reports_it():
    X = pd.DataFrame({
        "total_journey_min": [20.0, 0.0, 40.0, 0.0],
        "avg_fsm%": [10.0, 0.0, np.nan, 0.0],
    })
    rows = pd.Series([True, True, True, False])
    cleaned, report = clean_features(X, rows)

    assert cleaned["total_journey_min"].tolist() == [20.0, 30.0, 40.0, 0.0]  # median of 20 and 40; outside row untouched
    assert cleaned["avg_fsm%"].tolist()[:3] == [10.0, 0.0, 5.0]              # a zero FSM rate is a real value
    assert report == {"placeholder_zeros": {"total_journey_min": 1}, "missing_filled": {"total_journey_min": 1, "avg_fsm%": 1}}


def test_score_predictions_is_in_visits_per_thousand():
    scores = score_predictions(np.array([2.0, 4.0]), np.array([3.0, 4.0]), np.array([1000.0, 3000.0]))
    assert scores["mae"] == 0.5
    assert scores["weighted_mae"] == 0.25
    assert round(scores["rmse"], 4) == round(np.sqrt(0.5), 4)


def test_only_study_area_districts_are_used_and_scored():
    data, results, models, diagnostics = _standard_run()

    assert diagnostics["districts_in_dataset"] == 340
    assert diagnostics["districts_used"] == 40
    assert "grouped by local authority" in diagnostics["cross_validation"]
    assert diagnostics["group_sizes"] == {"A": 14, "B": 12, "C": 8, "D": 6}
    for info in models.values():
        assert len(info["oof_predictions"]) == 40
        assert info["oof_predictions"].notna().all()
        assert set(info["oof_predictions"].index) == set(data.index[:40])


def test_real_signal_is_found_and_beats_both_baselines():
    _, results, models, diagnostics = _standard_run()

    assert {MEAN_BASELINE, JOURNEY_BASELINE} <= set(results.index)
    assert MEAN_BASELINE not in models and JOURNEY_BASELINE not in models  # baselines are never selectable
    assert diagnostics["best_mae"] < 0.6 * diagnostics["baseline_mae"]
    assert diagnostics["skill_vs_baseline"] > 0.4
    assert results.loc[MEAN_BASELINE, "Skill vs baseline"] == 0
    assert not any("No model predicts better" in note for note in diagnostics["notes"])


def test_no_signal_is_reported_honestly():
    results, models, diagnostics = _run(_districts(signal=False), tune=False)
    assert diagnostics["skill_vs_baseline"] < 0.15
    assert results.loc[diagnostics["best_model"], "Mean R2"] < 0.2


def test_every_row_is_scored_the_same_way():
    data, results, models, _ = _standard_run()
    rate = (data["Visits"] / data["Population"] * 1000).iloc[:40]

    for name, info in models.items():
        predicted = info["oof_predictions"]
        expected = score_predictions(rate.values, predicted.values, data["Population"].iloc[:40].values)
        assert results.loc[name, "Mean MAE"] == pytest.approx(expected["mae"])
        assert results.loc[name, "Mean RMSE"] == pytest.approx(expected["rmse"])
        assert results.loc[name, "Mean R2"] == pytest.approx(expected["r2"])
        assert info["mae"] == pytest.approx(expected["mae"])


def test_ensemble_uses_distinct_model_families():
    _, results, models, diagnostics = _standard_run()
    members = diagnostics["ensemble_members"]

    assert models[ENSEMBLE_NAME]["base_models"] == members
    families = [name.replace("_Tuned", "") for name in members]
    assert len(members) == 3 and len(set(families)) == 3
    average = np.mean([models[name]["oof_predictions"].values for name in members], axis=0)
    assert np.allclose(models[ENSEMBLE_NAME]["oof_predictions"].values, average)


def test_fitted_models_work_with_the_existing_prediction_code():
    pytest.importorskip("tqdm")
    pytest.importorskip("optuna")
    from src.model_training import get_explanation_model, predict_rates, select_best_model

    data, results, models, diagnostics = _standard_run()
    name, info, _ = select_best_model(results, models)

    assert name == diagnostics["best_model"]
    predictions = predict_rates(info, data[FEATURES].iloc[:40], data["Population"].iloc[:40])
    assert predictions.shape == (40,) and (predictions >= 0).all()
    explain = get_explanation_model(info)
    assert {"scaler", "model"} <= set(explain["pipeline"].named_steps)


def test_tuning_can_be_switched_off():
    results, models, diagnostics = _run(_districts(), tune=False)
    assert diagnostics["tuning_trials"] == 0
    assert not any(name.endswith("_Tuned") for name in models)


def test_too_few_districts_is_an_error():
    data = _districts().iloc[:6]
    with pytest.raises(ValueError):
        _run(data)


def test_safe_zone_bands_use_model_rmse_when_available():
    data = pd.DataFrame({"predicted_visit_rate": [4.0, 4.0, 4.0], "visits_per_1000": [3.5, 2.5, 0.5]})

    with_model = calculate_safe_zone_benchmarks(data, model_rmse=1.2)
    assert with_model["safe_zone_lower_1rmse"].iloc[0] == pytest.approx(2.8)
    assert with_model["safe_zone_lower_2rmse"].iloc[0] == pytest.approx(1.6)
    assert with_model["safe_zone_band_width"].iloc[0] == pytest.approx(1.2)
    assert with_model["needs_intervention"].tolist() == [False, False, True]

    fixed = calculate_safe_zone_benchmarks(data)  # no RMSE supplied: fixed buffer
    half = ModelConstants.SAFE_ZONE_BUFFER / 2
    assert fixed["safe_zone_lower_1rmse"].iloc[0] == pytest.approx(4.0 - half)
    assert fixed["needs_intervention"].tolist() == [False, False, True]
    assert fixed["safe_zone_status"].tolist() == ["Within Safe Zone", "Moderate Underperformance", "Severe Underperformance"]


# ------------------------------------------------------- extra training areas
def _with_neighbours(same_pattern: bool = True, routed: bool = True) -> pd.DataFrame:
    """The standard made-up data plus 30 'SO' districts, some sharing local authority B with the study area."""
    data = _districts()
    rng = np.random.default_rng(7)
    n = 30
    journey = rng.uniform(40, 110, n)
    imd = rng.uniform(2, 9, n)
    true_rate = np.exp(2.6 - 0.025 * journey + 0.1 * (imd - 5)) if same_pattern else np.exp(0.2 + 0.02 * journey)
    neighbours = pd.DataFrame({
        "District": [f"SO{i}" for i in range(1, n + 1)],
        "Authority_Name": ["B"] * 10 + ["E"] * 20,
        "total_journey_min": journey if routed else 120.0,
        "imd_decile_mean": imd,
        "avg_fsm%": 45 - imd * 4 + rng.normal(0, 3, n),
        "Population": rng.integers(8000, 50000, n).astype(float),
    })
    neighbours["Visits"] = rng.poisson(true_rate * neighbours["Population"] / 1000).astype(float)
    return pd.concat([data, neighbours], ignore_index=True)


def test_area_mask_matches_whole_area_codes_only():
    districts = pd.Series(["SO14", "so51", "S1", "SP1", "BA2", "B1", "TA1", None])
    assert area_mask(districts, ["SO", "BA"]).tolist() == [True, True, False, False, True, False, False, False]
    assert not area_mask(districts, []).any()


def test_extra_training_areas_are_off_by_default():
    assert GeographicConstants.BCP_DORSET_POSTCODES == ["BH", "DT", "SP"]
    assert GeographicConstants.EXTRA_TRAINING_AREAS == []


def test_unrouted_areas_are_detected():
    routed = _with_neighbours(routed=True)
    assert areas_without_real_features(routed, routed["District"], ["SO"]) == []
    placeholder = _with_neighbours(routed=False)
    assert areas_without_real_features(placeholder, placeholder["District"], ["SO"]) == ["SO"]
    assert areas_without_real_features(routed, routed["District"], ["TA"]) == ["TA"]  # no such districts at all


def test_extra_areas_are_trained_on_but_never_scored():
    data = _with_neighbours()
    base_results, _, base = _run(_districts(), tune=False)
    results, models, diagnostics = _run(data, tune=False, extra_training_areas=["SO"])

    assert diagnostics["districts_used"] == 70 and diagnostics["districts_scored"] == 40
    assert diagnostics["extra_training_areas"] == ["SO"]
    assert diagnostics["group_sizes"] == base["group_sizes"]            # folds are built on the study area only
    for info in models.values():
        assert set(info["oof_predictions"].index) == set(data.index[:40])
        assert info["oof_predictions"].notna().all()
    # Baselines never see the extra rows, so they are identical with and without them.
    for name in (MEAN_BASELINE, JOURNEY_BASELINE):
        assert results.loc[name, "Mean MAE"] == pytest.approx(base_results.loc[name, "Mean MAE"])
    assert diagnostics["baseline_mae"] == pytest.approx(base["baseline_mae"])
    # The fitted model has seen the wider journey range.
    ridge = models["Ridge Regression"]["pipeline"]
    assert ridge.named_steps["scaler"].n_samples_seen_ == 70


def test_neighbours_that_behave_differently_show_up_as_worse_scores():
    _, _, alone = _run(_districts(), tune=False)
    _, _, helped = _run(_with_neighbours(same_pattern=True), tune=False, extra_training_areas=["SO"])
    _, _, hurt = _run(_with_neighbours(same_pattern=False), tune=False, extra_training_areas=["SO"])
    assert hurt["best_mae"] > alone["best_mae"]
    assert helped["best_mae"] < hurt["best_mae"]


def test_extra_rows_from_a_held_out_authority_are_not_trained_on():
    """SO districts in authority B must be left out whenever authority B is the test fold."""
    data = _with_neighbours()
    b_neighbours = (data["Authority_Name"] == "B") & data["District"].str.startswith("SO")
    assert b_neighbours.sum() == 10

    # Give those neighbours two different, absurd visit counts. They are held out together with
    # authority B, so the predictions for B's study districts must not change at all.
    predictions = []
    for multiplier in (5, 50):
        altered = data.copy()
        altered.loc[b_neighbours, "Visits"] = altered.loc[b_neighbours, "Population"] * multiplier
        _, models, _ = _run(altered, tune=False, extra_training_areas=["SO"])
        predictions.append(models["Ridge Regression"]["oof_predictions"])

    in_b = (data["Authority_Name"].iloc[:40] == "B").values
    assert np.allclose(predictions[0][in_b].values, predictions[1][in_b].values)
    # Sanity check that the test can fail: other authorities do train on those rows.
    assert not np.allclose(predictions[0][~in_b].values, predictions[1][~in_b].values)
