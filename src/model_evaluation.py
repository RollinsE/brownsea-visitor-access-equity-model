# -*- coding: utf-8 -*-
"""Model training and evaluation.

How the models are trained and scored:

1. Training scope. Models are trained and scored on the study area only (the
   BH, DT and SP postcode areas). Districts elsewhere have no routed journey
   times and no reliable visit counts, so they are left out.
2. One scoring method. Every model, tuned or not, single or ensemble, is scored
   the same way: on its pooled out-of-fold predictions, in visits per 1,000
   residents.
3. Baselines. A "same rate everywhere" baseline and a "journey time only"
   baseline are scored alongside, so an error figure has something to be
   compared with.
4. Nested tuning. Hyperparameters are tuned inside each training fold, so the
   score of a tuned model is not flattered by tuning on its own test data.
5. Ensemble make-up. The ensemble averages the best model from each of the top
   three model families, so its members are different kinds of model.
"""
from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import PoissonRegressor, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GroupKFold, KFold, RepeatedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.constants import GeographicConstants

LOG = logging.getLogger("Brownsea_Equity_Analysis")

STUDY_PREFIXES = tuple(GeographicConstants.BCP_DORSET_POSTCODES)

# Features that cannot genuinely be zero. A zero here means "not calculated".
POSITIVE_FEATURES = (
    "total_journey_min",
    "nearest_competitor_drive_min",
    "imd_decile_mean",
    "geo_barriers_decile",
    "wider_barriers_decile",
    "income_decile",
)

MEAN_BASELINE = "Baseline: same rate everywhere"
JOURNEY_BASELINE = "Baseline: journey time only"
ENSEMBLE_NAME = "HybridEnsemble"


# ----------------------------------------------------------------------- data
def study_area_mask(districts: pd.Series) -> pd.Series:
    """True for districts in the BH, DT and SP postcode areas."""
    return districts.astype(str).str.upper().str.strip().str.startswith(STUDY_PREFIXES)


def postcode_area(districts: pd.Series) -> pd.Series:
    """The letters at the start of a district code: 'SO14' -> 'SO', 'B1' -> 'B'."""
    return districts.astype(str).str.upper().str.strip().str.extract(r"^([A-Z]+)", expand=False).fillna("")


def area_mask(districts: pd.Series, areas: list[str] | tuple[str, ...]) -> pd.Series:
    """True for districts whose postcode area is one of the given areas."""
    wanted = {str(area).strip().upper() for area in areas or [] if str(area).strip()}
    return postcode_area(districts).isin(wanted)


def areas_without_real_features(data: pd.DataFrame, districts: pd.Series, areas: list[str]) -> list[str]:
    """Areas whose districts still carry placeholder journey features (not routed yet).

    Call this on the raw modelling dataset, before clean_features.
    """
    missing = []
    for area in areas or []:
        rows = data.loc[area_mask(districts, [area])]
        if rows.empty:
            missing.append(area)
            continue
        routed = pd.Series(True, index=rows.index)
        if "nearest_competitor_drive_min" in rows:
            routed &= pd.to_numeric(rows["nearest_competitor_drive_min"], errors="coerce").fillna(0) > 0
        if "total_journey_min" in rows and len(rows) > 1 and rows["total_journey_min"].nunique() <= 1:
            routed &= False
        if routed.mean() < 0.8:
            missing.append(area)
    return missing


def clean_features(X: pd.DataFrame, rows: pd.Series) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Replace placeholder and missing feature values, for the given rows only.

    Zeros in features that cannot be zero are treated as missing. Missing values
    are filled with the median of the same rows. Returns the cleaned frame and a
    count of what was changed, so nothing is altered silently.
    """
    X = X.copy()
    rows = rows.reindex(X.index).fillna(False).astype(bool)
    report: dict[str, Any] = {"placeholder_zeros": {}, "missing_filled": {}}

    for column in X.columns:
        values = pd.to_numeric(X.loc[rows, column], errors="coerce").astype(float)
        if column in POSITIVE_FEATURES:
            placeholders = int((values <= 0).sum())
            if placeholders:
                values = values.where(values > 0)
                report["placeholder_zeros"][column] = placeholders
        missing = int(values.isna().sum())
        if missing:
            median = values.median()
            values = values.fillna(0.0 if pd.isna(median) else median)
            report["missing_filled"][column] = missing
        X[column] = pd.to_numeric(X[column], errors="coerce").astype(float)
        X.loc[rows, column] = values
    return X, report


@dataclass
class Targets:
    """The different forms of the target the models are fitted to."""

    rate: np.ndarray          # visits per 1,000 residents (what is reported)
    per_person: np.ndarray    # visits per resident (Poisson models)
    log_rate: np.ndarray      # log(1 + rate) (rate models)
    population: np.ndarray
    weights: np.ndarray       # population relative to the mean


def build_targets(y_visits: pd.Series, population: pd.Series) -> Targets:
    visits = np.asarray(y_visits, dtype=float)
    pop = np.asarray(population, dtype=float)
    per_person = visits / pop
    rate = per_person * 1000.0
    return Targets(rate, per_person, np.log1p(rate), pop, pop / pop.mean())


# --------------------------------------------------------------------- models
class _SameRateEverywhere(BaseEstimator, RegressorMixin):
    """Baseline: predicts the overall visits-per-resident of the training rows."""

    def fit(self, X, y, sample_weight=None):
        self.rate_ = float(np.average(y, weights=sample_weight))
        return self

    def predict(self, X):
        return np.full(len(X), self.rate_)


@dataclass(frozen=True)
class ModelSpec:
    name: str
    family: str
    kind: str                                   # "rate" or "poisson"
    build: Callable[[dict[str, Any]], Any]      # params -> unfitted estimator
    defaults: dict[str, Any] = field(default_factory=dict)
    space: dict[str, tuple] | None = None       # name -> ("int"|"float", low, high, log)
    role: str = "candidate"                     # or "baseline"
    columns: tuple[str, ...] | None = None      # restrict to these features


def candidate_specs(seed: int = 42, feature_names: list[str] | None = None) -> list[ModelSpec]:
    """Baselines plus every model family whose library is installed."""
    specs = [
        ModelSpec(MEAN_BASELINE, "baseline", "poisson", lambda p: _SameRateEverywhere(), role="baseline"),
    ]
    if feature_names and "total_journey_min" in feature_names:
        specs.append(ModelSpec(
            JOURNEY_BASELINE, "baseline", "rate", lambda p: Ridge(alpha=1.0),
            role="baseline", columns=("total_journey_min",),
        ))

    specs += [
        ModelSpec(
            "Ridge Regression", "ridge", "rate",
            lambda p: Ridge(random_state=seed, **p),
            defaults={"alpha": 1.0},
            space={"alpha": ("float", 0.01, 100.0, True)},
        ),
        ModelSpec(
            "Poisson GLM", "poisson_glm", "poisson",
            lambda p: PoissonRegressor(max_iter=2000, **p),
            # The target is visits per resident, a very small number, so the penalty must be small too.
            defaults={"alpha": 1e-4},
            space={"alpha": ("float", 1e-7, 1e-1, True)},
        ),
        ModelSpec(
            "Random Forest", "random_forest", "rate",
            lambda p: RandomForestRegressor(n_estimators=300, random_state=seed, n_jobs=1, **p),
            defaults={"min_samples_leaf": 3, "max_features": 0.5},
            space={
                "max_depth": ("int", 2, 8, False),
                "min_samples_leaf": ("int", 1, 8, False),
                "max_features": ("float", 0.3, 1.0, False),
            },
        ),
        ModelSpec(
            "Gradient Boosting", "gradient_boosting", "rate",
            lambda p: GradientBoostingRegressor(random_state=seed, subsample=0.8, **p),
            defaults={"n_estimators": 150, "max_depth": 2, "learning_rate": 0.05},
            space={
                "n_estimators": ("int", 50, 300, False),
                "max_depth": ("int", 1, 4, False),
                "learning_rate": ("float", 0.01, 0.2, True),
                "min_samples_leaf": ("int", 1, 8, False),
            },
        ),
    ]

    try:
        import lightgbm as lgb

        specs.append(ModelSpec(
            "LightGBM", "lightgbm", "poisson",
            lambda p: lgb.LGBMRegressor(objective="poisson", random_state=seed, n_jobs=1, verbosity=-1, **p),
            defaults={"n_estimators": 150, "learning_rate": 0.05, "num_leaves": 7, "min_child_samples": 5},
            space={
                "n_estimators": ("int", 50, 300, False),
                "learning_rate": ("float", 0.01, 0.2, True),
                "num_leaves": ("int", 3, 15, False),
                "min_child_samples": ("int", 2, 10, False),
            },
        ))
    except ImportError:
        LOG.info("lightgbm not installed; skipping LightGBM")

    try:
        import xgboost as xgb

        specs.append(ModelSpec(
            "XGBoost", "xgboost", "poisson",
            lambda p: xgb.XGBRegressor(objective="count:poisson", random_state=seed, n_jobs=1, **p),
            defaults={"n_estimators": 150, "learning_rate": 0.05, "max_depth": 2},
            space={
                "n_estimators": ("int", 50, 300, False),
                "learning_rate": ("float", 0.01, 0.2, True),
                "max_depth": ("int", 1, 4, False),
            },
        ))
    except ImportError:
        LOG.info("xgboost not installed; skipping XGBoost")

    try:
        import catboost as cb

        specs.append(ModelSpec(
            "CatBoost", "catboost", "rate",
            lambda p: cb.CatBoostRegressor(verbose=0, random_seed=seed, thread_count=1, **p),
            defaults={"iterations": 200, "depth": 3, "learning_rate": 0.05},
            space={
                "iterations": ("int", 50, 300, False),
                "depth": ("int", 2, 5, False),
                "learning_rate": ("float", 0.01, 0.2, True),
            },
        ))
    except ImportError:
        LOG.info("catboost not installed; skipping CatBoost")

    return specs


def make_pipeline(spec: ModelSpec, params: dict[str, Any] | None = None) -> Pipeline:
    """Scaler followed by the model. Step names match what the SHAP code expects."""
    scaler: Any = StandardScaler()
    if spec.columns:
        scaler = ColumnTransformer([("selected", StandardScaler(), list(spec.columns))])
    return Pipeline([("scaler", scaler), ("model", spec.build({**spec.defaults, **(params or {})}))])


def fit_model(spec: ModelSpec, params: dict[str, Any] | None, X: pd.DataFrame, targets: Targets, rows: np.ndarray) -> Pipeline:
    pipeline = make_pipeline(spec, params)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if spec.kind == "poisson":
            pipeline.fit(X.iloc[rows], targets.per_person[rows], model__sample_weight=targets.population[rows])
        else:
            # Weights are scaled within the training rows, so a fit does not depend on
            # the population of rows it has not seen.
            weights = targets.population[rows] / targets.population[rows].mean()
            pipeline.fit(X.iloc[rows], targets.log_rate[rows], model__sample_weight=weights)
    return pipeline


def predict_rate(pipeline: Pipeline, kind: str, X: pd.DataFrame) -> np.ndarray:
    """Predicted visits per 1,000 residents."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        raw = np.asarray(pipeline.predict(X), dtype=float)
    rate = raw * 1000.0 if kind == "poisson" else np.expm1(raw)
    return np.maximum(0.0, np.where(np.isfinite(rate), rate, 0.0))


# -------------------------------------------------------------------- scoring
def score_predictions(actual: np.ndarray, predicted: np.ndarray, population: np.ndarray) -> dict[str, float]:
    """Error measures in visits per 1,000 residents."""
    return {
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(np.sqrt(mean_squared_error(actual, predicted))),
        "r2": float(r2_score(actual, predicted)) if len(actual) > 1 else float("nan"),
        "weighted_mae": float(np.average(np.abs(actual - predicted), weights=population)),
    }


def make_splits(n_rows: int, groups: pd.Series | None, n_splits: int, seed: int) -> tuple[list, str, bool]:
    """Grouped folds when there are groups to hold out, plain folds otherwise."""
    n_splits = max(2, min(n_splits, n_rows))
    if groups is not None and groups.nunique() >= 2:
        folds = min(n_splits, int(groups.nunique()))
        splits = list(GroupKFold(n_splits=folds).split(np.zeros(n_rows), groups=groups.values))
        return splits, f"{folds}-fold cross-validation grouped by local authority", True
    splits = list(KFold(n_splits=n_splits, shuffle=True, random_state=seed).split(np.zeros(n_rows)))
    return splits, f"{n_splits}-fold cross-validation", False


def out_of_fold(spec: ModelSpec, X: pd.DataFrame, targets: Targets, splits: list,
                params_for_fold: Callable[[int, np.ndarray], dict[str, Any] | None]) -> np.ndarray:
    """Predict every row from a model that was not trained on it."""
    predictions = np.full(len(X), np.nan)
    for fold, (train_rows, test_rows) in enumerate(splits):
        pipeline = fit_model(spec, params_for_fold(fold, train_rows), X, targets, train_rows)
        predictions[test_rows] = predict_rate(pipeline, spec.kind, X.iloc[test_rows])
    return predictions


def fold_mae_std(actual: np.ndarray, predicted: np.ndarray, splits: list) -> float:
    return float(np.std([mean_absolute_error(actual[test], predicted[test]) for _, test in splits]))


def repeated_cv_mae(spec: ModelSpec, params: dict[str, Any] | None, X: pd.DataFrame, targets: Targets,
                    scored_rows: np.ndarray, extra_rows: np.ndarray, n_splits: int, repeats: int, seed: int) -> float:
    """Second opinion on the error: ordinary shuffled folds, repeated, with fixed parameters.

    Only the scored rows are held out. Extra training rows are always trained on.
    """
    if repeats <= 0:
        return float("nan")
    n_splits = max(2, min(n_splits, len(scored_rows)))
    all_splits = list(RepeatedKFold(n_splits=n_splits, n_repeats=repeats, random_state=seed).split(scored_rows))
    maes = []
    for start in range(0, len(all_splits), n_splits):
        splits = [(np.concatenate([scored_rows[train], extra_rows]).astype(int), scored_rows[test])
                  for train, test in all_splits[start:start + n_splits]]
        predictions = out_of_fold(spec, X, targets, splits, lambda fold, rows: params)
        maes.append(mean_absolute_error(targets.rate[scored_rows], predictions[scored_rows]))
    return float(np.mean(maes))


# --------------------------------------------------------------------- tuning
def _sample(space: dict[str, tuple], rng: np.random.Generator) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for name, (kind, low, high, log) in space.items():
        if kind == "int":
            params[name] = int(rng.integers(low, high + 1))
        elif log:
            params[name] = float(np.exp(rng.uniform(np.log(low), np.log(high))))
        else:
            params[name] = float(rng.uniform(low, high))
    return params


def tune_params(spec: ModelSpec, X: pd.DataFrame, targets: Targets, rows: np.ndarray,
                n_trials: int, seed: int, inner_splits: int = 4, scored: np.ndarray | None = None) -> dict[str, Any]:
    """Choose parameters using only the given rows.

    The rows are split again into inner folds and each trial is scored on its
    inner out-of-fold error. The defaults are always one of the trials, so a
    tuned model cannot be chosen that looked worse than the defaults.

    `scored` marks the rows the model is judged on (the study area). Only those
    are held out in the inner folds; any other rows are always trained on.
    """
    rows = np.asarray(rows, dtype=int)
    judged = rows if scored is None else rows[scored[rows]]
    always_train = rows[:0] if scored is None else rows[~scored[rows]]
    if not spec.space or n_trials <= 0 or len(judged) < 2 * inner_splits:
        return dict(spec.defaults)

    inner = [(np.concatenate([judged[train], always_train]).astype(int), judged[test]) for train, test in
             KFold(n_splits=inner_splits, shuffle=True, random_state=seed).split(judged)]

    def inner_error(params: dict[str, Any]) -> float:
        predictions = np.full(len(X), np.nan)
        for train_rows, test_rows in inner:
            pipeline = fit_model(spec, params, X, targets, train_rows)
            predictions[test_rows] = predict_rate(pipeline, spec.kind, X.iloc[test_rows])
        return float(mean_absolute_error(targets.rate[judged], predictions[judged]))

    defaults = {name: spec.defaults[name] for name in spec.space if name in spec.defaults}

    try:
        import optuna
    except ImportError:
        optuna = None

    if optuna is not None:
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=seed))
        if len(defaults) == len(spec.space):
            study.enqueue_trial(defaults)

        def objective(trial):
            params = {}
            for name, (kind, low, high, log) in spec.space.items():
                if kind == "int":
                    params[name] = trial.suggest_int(name, low, high)
                else:
                    params[name] = trial.suggest_float(name, low, high, log=log)
            return inner_error(params)

        study.optimize(objective, n_trials=n_trials)
        return {**spec.defaults, **study.best_params}

    # Fallback when optuna is not installed: plain random search over the same ranges.
    rng = np.random.default_rng(seed)
    trials = [dict(spec.defaults)] + [{**spec.defaults, **_sample(spec.space, rng)} for _ in range(max(0, n_trials - 1))]
    errors = [inner_error(params) for params in trials]
    return trials[int(np.argmin(errors))]


# ----------------------------------------------------------------------- main
def train_and_evaluate(
    X: pd.DataFrame,
    y_visits: pd.Series,
    population: pd.Series,
    params: dict[str, Any],
    groups: pd.Series | None = None,
    districts: pd.Series | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Train, tune and score the candidate models.

    Returns (results table, fitted models keyed by name, diagnostics). Each
    fitted-model entry holds the pipeline, its out-of-fold predictions and its
    scores, which is what the analysis stage and the model bundle read.
    """
    seed = int(params.get("random_state", 42))
    n_splits = int(params.get("n_splits_cv", 5))
    n_trials = int(params.get("optuna_trials", 30)) if params.get("tune", True) else 0
    repeats = int(params.get("repeated_cv_repeats", 5))
    scope = str(params.get("training_scope", "study_area"))

    extra_areas = [str(a).strip().upper() for a in params.get("extra_training_areas") or [] if str(a).strip()]

    population_num = pd.to_numeric(population, errors="coerce")
    usable = population_num.notna() & (population_num > 0)
    study = pd.Series(True, index=X.index)
    extra = pd.Series(False, index=X.index)
    if scope == "study_area":
        if districts is None:
            raise ValueError("training_scope='study_area' needs the District column")
        study = study_area_mask(districts).reindex(X.index).fillna(False)
        extra = area_mask(districts, extra_areas).reindex(X.index).fillna(False) & ~study
    keep = (usable & (study | extra)).values
    if int((usable & study).sum()) < 10:
        raise ValueError(f"Only {int((usable & study).sum())} districts available to score; need at least 10")

    X_fit = X.loc[keep]
    visits = pd.to_numeric(y_visits.loc[keep], errors="coerce").fillna(0.0)
    targets = build_targets(visits, population_num.loc[keep])
    groups_fit = groups.loc[keep].fillna("Unknown").astype(str) if groups is not None else None

    # The model is always judged on the study-area rows. Extra training rows are
    # never test rows; they are left out of training only when their local
    # authority is the one being held out, so nothing leaks across a fold.
    scored = study.loc[keep].values.astype(bool)
    scored_rows = np.flatnonzero(scored)
    extra_rows = np.flatnonzero(~scored)
    groups_scored = groups_fit.iloc[scored_rows] if groups_fit is not None else None
    base_splits, cv_description, grouped = make_splits(len(scored_rows), groups_scored, n_splits, seed)
    splits = []
    for train, test in base_splits:
        held_out = set(groups_scored.iloc[test]) if grouped else set()
        extra_train = np.array([row for row in extra_rows if groups_fit.iloc[row] not in held_out], dtype=int) \
            if grouped else extra_rows
        splits.append((np.concatenate([scored_rows[train], extra_train]).astype(int), scored_rows[test]))
    study_only_splits = [(scored_rows[train], scored_rows[test]) for train, test in base_splits]
    no_rows = extra_rows[:0]

    LOG.info("Modelling: %s districts trained on, %s scored, %s", len(X_fit), len(scored_rows), cv_description)

    rows_all = np.arange(len(X_fit))
    actual = targets.rate[scored_rows]
    table: list[dict[str, Any]] = []
    fitted: dict[str, Any] = {}
    oof: dict[str, np.ndarray] = {}
    family_of: dict[str, str] = {}

    def record(name: str, model_type: str, role: str, predictions: np.ndarray, repeated: float) -> dict[str, float]:
        scores = score_predictions(actual, predictions[scored_rows], targets.population[scored_rows])
        table.append({
            "Model": name, "Type": model_type, "Role": role,
            "Mean MAE": scores["mae"], "Std MAE": fold_mae_std(targets.rate, predictions, splits),
            "Mean R2": scores["r2"], "Std R2": np.nan, "Mean RMSE": scores["rmse"],
            "Weighted MAE": scores["weighted_mae"], "Repeated CV MAE": repeated,
        })
        oof[name] = predictions
        LOG.info("%s - out-of-fold MAE: %.4f", name, scores["mae"])
        return scores

    def keep_model(name: str, spec: ModelSpec, model_params: dict[str, Any], scores: dict[str, float]) -> None:
        fitted[name] = {
            "pipeline": fit_model(spec, model_params, X_fit, targets, rows_all),
            "type": spec.kind,
            "mae": scores["mae"], "rmse": scores["rmse"], "r2": scores["r2"],
            "features": X_fit.columns.tolist(),
            "best_params": dict(model_params),
            "oof_predictions": pd.Series(oof[name][scored_rows], index=X_fit.index[scored_rows]),
        }
        family_of[name] = spec.family

    for spec in candidate_specs(seed, X_fit.columns.tolist()):
        try:
            # Baselines describe the study area itself, so they never train on the extra rows.
            is_baseline = spec.role == "baseline"
            spec_splits = study_only_splits if is_baseline else splits
            spec_extra = no_rows if is_baseline else extra_rows
            predictions = out_of_fold(spec, X_fit, targets, spec_splits, lambda fold, rows: None)
            repeated = repeated_cv_mae(spec, None, X_fit, targets, scored_rows, spec_extra, n_splits, repeats, seed)
            label = "Baseline" if spec.role == "baseline" else spec.kind.capitalize()
            scores = record(spec.name, label, spec.role, predictions, repeated)
            if spec.role == "baseline":
                continue
            keep_model(spec.name, spec, dict(spec.defaults), scores)

            if spec.space and n_trials > 0:
                tuned_name = f"{spec.name}_Tuned"
                predictions = out_of_fold(
                    spec, X_fit, targets, splits,
                    lambda fold, rows, spec=spec: tune_params(spec, X_fit, targets, rows, n_trials, seed + fold, scored=scored),
                )
                final_params = tune_params(spec, X_fit, targets, rows_all, n_trials, seed, scored=scored)
                repeated = repeated_cv_mae(spec, final_params, X_fit, targets, scored_rows, extra_rows, n_splits, repeats, seed)
                scores = record(tuned_name, f"Tuned {spec.kind}", "candidate", predictions, repeated)
                keep_model(tuned_name, spec, final_params, scores)
        except Exception as exc:  # one failing library should not stop the run
            LOG.warning("%s failed and was skipped: %s", spec.name, exc)

    if not fitted:
        raise RuntimeError("No candidate model could be trained")

    # Ensemble: the best model from each of the three best families.
    best_per_family: dict[str, str] = {}
    for name in sorted(fitted, key=lambda n: fitted[n]["mae"]):
        best_per_family.setdefault(family_of[name], name)
    members = list(best_per_family.values())[:3]
    if len(members) >= 2:
        predictions = np.mean([oof[name] for name in members], axis=0)
        scores = record(ENSEMBLE_NAME, "Ensemble", "candidate", predictions, float("nan"))
        fitted[ENSEMBLE_NAME] = {
            "type": "ensemble",
            "base_models": members,
            "members": [fitted[name] for name in members],
            "mae": scores["mae"], "rmse": scores["rmse"], "r2": scores["r2"],
            "oof_predictions": pd.Series(predictions[scored_rows], index=X_fit.index[scored_rows]),
        }

    results = pd.DataFrame(table).set_index("Model")
    baseline_mae = float(results.loc[MEAN_BASELINE, "Mean MAE"])
    results["Skill vs baseline"] = 1.0 - results["Mean MAE"] / baseline_mae if baseline_mae > 0 else np.nan
    results = results.sort_values("Mean MAE")

    best_name = min(fitted, key=lambda name: fitted[name]["mae"])
    best_mae = float(fitted[best_name]["mae"])
    notes = []
    if best_mae >= baseline_mae:
        notes.append(
            "No model predicts better than assuming the same visit rate everywhere. "
            "Treat the expected visit rates and performance gaps with caution."
        )
    if len(scored_rows) < 100:
        notes.append(
            f"The model is scored on only {len(scored_rows)} districts, so the error figures are themselves uncertain."
        )
    notes.append(
        "The final model and the ensemble members are chosen on the same folds they are scored on, "
        "so the winning score is slightly optimistic."
    )

    diagnostics = {
        "training_scope": scope,
        "districts_in_dataset": int(len(X)),
        "extra_training_areas": extra_areas,
        "districts_used": int(len(X_fit)),
        "districts_scored": int(len(scored_rows)),
        "districts_with_zero_visits": int((targets.rate == 0).sum()),
        "districts_dropped_no_population": int(((study | extra) & ~usable).sum()),
        "cross_validation": cv_description,
        "group_sizes": groups_scored.value_counts().to_dict() if groups_scored is not None else {},
        "tuning_trials": n_trials,
        "mean_observed_rate": float(actual.mean()),
        "baseline_mae": baseline_mae,
        "best_model": best_name,
        "best_mae": best_mae,
        "best_rmse": float(fitted[best_name]["rmse"]),
        "best_r2": float(fitted[best_name]["r2"]),
        "skill_vs_baseline": float(1.0 - best_mae / baseline_mae) if baseline_mae > 0 else None,
        "ensemble_members": members if len(members) >= 2 else [],
        "error_unit": "visits per 1,000 residents, on out-of-fold predictions",
        "notes": notes,
    }
    for note in notes[:-1]:
        LOG.warning(note)
    return results, fitted, diagnostics
