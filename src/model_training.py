# -*- coding: utf-8 -*-
"""Machine-learning model training and evaluation utilities."""

import logging
import json
from pathlib import Path
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, Tuple
from sklearn.model_selection import KFold, GroupKFold
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from tqdm import tqdm
import os
import sys
import optuna
from optuna.pruners import MedianPruner

LOG = logging.getLogger("Brownsea_Equity_Analysis")


from src.validation import validate_selected_features
from src.reporting import save_dataframe_bundle, save_text_report


def validate_data(X, y_visits, population, groups=None):
    """Validate data for NaN values and log a compact summary."""
    LOG.info("Validating data before training")

    if not isinstance(X, pd.DataFrame):
        X = pd.DataFrame(X)

    summary = []

    if X.isnull().any().any():
        nan_cols = X.columns[X.isnull().any()].tolist()
        for col in nan_cols:
            if X[col].dtype in ['float64', 'int64']:
                X[col] = X[col].fillna(0)
            else:
                X[col] = X[col].fillna('Unknown')
        summary.append(f"features: filled NaN in {len(nan_cols)} columns")

    if y_visits.isnull().any():
        n = int(y_visits.isnull().sum())
        y_visits = y_visits.fillna(0)
        summary.append(f"target: filled {n} missing values with 0")

    if population.isnull().any():
        n = int(population.isnull().sum())
        pop_median = population.median()
        if pd.isna(pop_median):
            pop_median = 1
        population = population.fillna(pop_median)
        summary.append(f"population: filled {n} missing values with median={float(pop_median):.2f}")

    if (population <= 0).any():
        n = int((population <= 0).sum())
        min_valid = population[population > 0].min()
        if pd.isna(min_valid):
            min_valid = 1
        population = population.clip(lower=min_valid)
        summary.append(f"population: clipped {n} non-positive values to {float(min_valid):.2f}")

    if groups is not None and groups.isnull().any():
        n = int(groups.isnull().sum())
        groups = groups.fillna('Unknown')
        summary.append(f"groups: filled {n} missing values with 'Unknown'")

    if summary:
        LOG.warning("Validation summary - " + "; ".join(summary))
    else:
        LOG.info("Validation summary - no missing-value corrections applied")

    return X, y_visits, population, groups


def create_model_pipelines() -> dict:
    """Create model pipelines with appropriate objectives."""
    import lightgbm as lgb
    import xgboost as xgb
    import catboost as cb
    
    from src.constants import ModelConstants
    
    return {
        "Random Forest": RandomForestRegressor(
            random_state=ModelConstants.RANDOM_STATE, n_jobs=-1
        ),
        "LightGBM": lgb.LGBMRegressor(
            random_state=ModelConstants.RANDOM_STATE, objective='poisson',
            n_jobs=-1, min_child_samples=5, min_split_gain=0.0,
            min_child_weight=0.001, verbosity=-1
        ),
        "XGBoost": xgb.XGBRegressor(
            random_state=ModelConstants.RANDOM_STATE, objective='count:poisson',
            n_jobs=-1
        ),
        "CatBoost": cb.CatBoostRegressor(
            random_state=ModelConstants.RANDOM_STATE,
            verbose=0
        ),
        "Gradient Boosting": GradientBoostingRegressor(
            random_state=ModelConstants.RANDOM_STATE, loss='squared_error'
        ),
        "Ridge Regression": Ridge(
            random_state=ModelConstants.RANDOM_STATE
        )
    }


def create_error_result(name, model_type):
    """Create error result entry for failed models."""
    return {
        "Model": name, "Type": model_type,
        "Mean MAE": np.nan, "Std MAE": np.nan,
        "Mean R2": np.nan, "Std R2": np.nan, "Mean RMSE": np.nan
    }


def evaluate_poisson_model(name: str, model: any, X: pd.DataFrame,
                          y_rate_per_person: pd.Series, exposure: pd.Series, cv_splits) -> dict:
    """Evaluate Poisson models using Rate + Weights approach."""
    import lightgbm as lgb
    import xgboost as xgb
    import catboost as cb
    
    try:
        mae_scores, r2_scores, rmse_scores = [], [], []
        valid_folds = 0
        LOG.info(f"Evaluating {name} with {len(cv_splits)} folds")

        for fold, (train_idx, val_idx) in enumerate(cv_splits):
            try:
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_train = y_rate_per_person.iloc[train_idx]
                y_val = y_rate_per_person.iloc[val_idx]
                w_train = exposure.iloc[train_idx]

                y_train_np = y_train.values.astype(np.float64)
                w_train_np = w_train.values.astype(np.float64)

                if name == "LightGBM":
                    train_data = lgb.Dataset(X_train, label=y_train_np, weight=w_train_np, params={'verbose': -1})
                    params = {'objective': 'poisson', 'verbosity': -1, 'num_leaves': 31, 'learning_rate': 0.05, 'n_estimators': 100, 'min_child_samples': 5}
                    model_trained = lgb.train(params, train_data)
                    pred_per_person = model_trained.predict(X_val)

                elif name == "XGBoost":
                    dtrain = xgb.DMatrix(X_train, label=y_train_np, weight=w_train_np)
                    dval = xgb.DMatrix(X_val)
                    params = {'objective': 'count:poisson', 'max_depth': 4, 'eta': 0.05, 'eval_metric': 'poisson-nloglik'}
                    model_trained = xgb.train(params, dtrain, num_boost_round=100)
                    pred_per_person = model_trained.predict(dval)

                elif name == "CatBoost":
                    from catboost import Pool
                    train_pool = Pool(X_train, label=y_train_np, weight=w_train_np)
                    model_trained = cb.CatBoostRegressor(loss_function='Poisson', iterations=100, learning_rate=0.05, depth=4, verbose=0, random_seed=42)
                    model_trained.fit(train_pool)
                    pred_per_person = model_trained.predict(X_val)
                else:
                    continue

                pred_rates = pred_per_person * 1000
                actual_rates = y_val.values * 1000

                pred_rates = np.maximum(0, np.where(np.isfinite(pred_rates), pred_rates, 0))

                if len(actual_rates) > 0:
                    mae = mean_absolute_error(actual_rates, pred_rates)
                    if not np.isnan(mae):
                        mae_scores.append(mae)
                        r2_scores.append(r2_score(actual_rates, pred_rates))
                        rmse_scores.append(np.sqrt(mean_squared_error(actual_rates, pred_rates)))
                        valid_folds += 1

            except Exception as e:
                LOG.warning(f"{name} fold {fold} failed: {e}")
                continue

        if valid_folds == 0:
            return create_error_result(name, "Poisson")

        LOG.info(f"{name} - MAE: {np.mean(mae_scores):.4f}")
        return {
            "Model": name, "Type": "Poisson",
            "Mean MAE": np.mean(mae_scores), "Std MAE": np.std(mae_scores),
            "Mean R2": np.mean(r2_scores), "Std R2": np.std(r2_scores), "Mean RMSE": np.mean(rmse_scores)
        }
    except Exception as e:
        LOG.error(f"Error in {name}: {e}")
        return create_error_result(name, "Poisson")


def evaluate_rate_model(name: str, model: any, X: pd.DataFrame,
                       y_rate_log: pd.Series, weights: pd.Series, cv_splits) -> dict:
    """Evaluate rate models with sample weights."""
    try:
        if y_rate_log.isnull().any() or weights.isnull().any():
            LOG.error(f"{name}: contains NaN")
            return create_error_result(name, "Rate")

        pipeline = Pipeline([('scaler', StandardScaler()), ('model', model)])
        mae_scores = []
        r2_scores = []
        rmse_scores = []
        valid_folds = 0

        for fold, (train_idx, val_idx) in enumerate(cv_splits):
            try:
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_train = y_rate_log.iloc[train_idx]
                y_val = y_rate_log.iloc[val_idx]
                w_train = weights.iloc[train_idx]

                y_train_np = y_train.values
                w_train_np = w_train.values
                y_val_np = y_val.values

                if np.isnan(y_train_np).any() or np.isnan(w_train_np).any():
                    LOG.warning(f"{name} fold {fold}: skipping NaN")
                    continue

                pipeline.fit(X_train, y_train_np, model__sample_weight=w_train_np)

                pred_log = pipeline.predict(X_val)
                pred_rates = np.expm1(pred_log)
                pred_rates = np.maximum(0, pred_rates)
                actual_rates = np.expm1(y_val_np)

                if len(actual_rates) > 0:
                    mae = mean_absolute_error(actual_rates, pred_rates)
                    if not np.isnan(mae):
                        mae_scores.append(mae)
                        r2_scores.append(r2_score(actual_rates, pred_rates))
                        rmse_scores.append(np.sqrt(mean_squared_error(actual_rates, pred_rates)))
                        valid_folds += 1

            except Exception as e:
                LOG.warning(f"{name} fold {fold} failed: {e}")
                continue

        if valid_folds == 0:
            LOG.error(f"{name}: no valid folds")
            return create_error_result(name, "Rate")

        return {
            "Model": name, "Type": "Rate",
            "Mean MAE": np.mean(mae_scores), "Std MAE": np.std(mae_scores),
            "Mean R2": np.mean(r2_scores), "Std R2": np.std(r2_scores),
            "Mean RMSE": np.mean(rmse_scores)
        }
    except Exception as e:
        LOG.error(f"Error in {name}: {e}")
        return create_error_result(name, "Rate")


def train_and_evaluate(X: pd.DataFrame, y_visits: pd.Series, population: pd.Series,
                       params: dict, groups: pd.Series = None):
    """Train and evaluate models"""
    RS = params.get('random_state', 42)
    N_SPLITS = params.get('n_splits_cv', 5)

    X, y_visits, population, groups = validate_data(X, y_visits, population, groups)

    with np.errstate(divide='ignore', invalid='ignore'):
        y_rate_per_person_val = y_visits / population
        y_rate_per_person_val = np.where(np.isfinite(y_rate_per_person_val), y_rate_per_person_val, 0)
        y_rate_per_person = pd.Series(y_rate_per_person_val, index=y_visits.index)

        y_rate_val = y_rate_per_person_val * 1000
        y_rate = pd.Series(y_rate_val, index=y_visits.index)

    y_rate_log = pd.Series(np.log1p(y_rate), index=y_visits.index)
    sample_weights = pd.Series(np.maximum(population / population.mean(), 0.001), index=population.index)
    exposure = pd.Series(np.maximum(population, 0.001), index=population.index)

    if groups is not None:
        n_splits_actual = max(2, min(N_SPLITS, groups.nunique()))
        cv_splits = list(GroupKFold(n_splits=n_splits_actual).split(X, y_visits, groups))
    else:
        cv_splits = list(KFold(n_splits=N_SPLITS, shuffle=True, random_state=RS).split(X, y_visits))

    models = create_model_pipelines()
    results_list = []
    trained_pipelines = {}

    progress_setting = os.environ.get('BROWSEA_PROGRESS', '').strip().lower()
    show_progress = progress_setting in {'force', 'always'} or (progress_setting in {'1', 'true', 'yes', 'y'} and sys.stdout.isatty())
    for name, model in tqdm(models.items(), desc="Training base models", disable=not show_progress, leave=False):
        LOG.info(f"Evaluating {name} with {N_SPLITS} folds")

        is_poisson = name in ["LightGBM", "XGBoost"]
        pipeline = Pipeline([('scaler', StandardScaler()), ('model', model)])

        oof_preds = np.zeros(len(X))
        mae_scores, r2_scores, rmse_scores = [], [], []
        valid_folds = 0

        for fold, (train_idx, val_idx) in enumerate(cv_splits):
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]

            try:
                if is_poisson:
                    pipeline.fit(X_train, y_rate_per_person.iloc[train_idx], model__sample_weight=exposure.iloc[train_idx].values)
                    pred_rates = np.maximum(0, pipeline.predict(X_val) * 1000)
                else:
                    pipeline.fit(X_train, y_rate_log.iloc[train_idx], model__sample_weight=sample_weights.iloc[train_idx].values)
                    pred_rates = np.maximum(0, np.expm1(pipeline.predict(X_val)))

                oof_preds[val_idx] = pred_rates
                actual_rates = y_rate.iloc[val_idx].values

                if len(actual_rates) > 0:
                    mae_scores.append(mean_absolute_error(actual_rates, pred_rates))
                    r2_scores.append(r2_score(actual_rates, pred_rates))
                    rmse_scores.append(np.sqrt(mean_squared_error(actual_rates, pred_rates)))
                    valid_folds += 1
            except Exception as e:
                LOG.warning(f"{name} fold {fold} failed: {e}")

        if valid_folds > 0:
            mean_mae = np.mean(mae_scores)
            LOG.info(f"{name} - MAE: {mean_mae:.4f}")

            results_list.append({
                "Model": name, "Type": "Poisson" if is_poisson else "Rate",
                "Mean MAE": mean_mae, "Std MAE": np.std(mae_scores),
                "Mean R2": np.mean(r2_scores), "Std R2": np.std(r2_scores),
                "Mean RMSE": np.mean(rmse_scores)
            })

            try:
                if is_poisson:
                    pipeline.fit(X, y_rate_per_person, model__sample_weight=exposure.values)
                else:
                    pipeline.fit(X, y_rate_log, model__sample_weight=sample_weights.values)

                trained_pipelines[name] = {
                    'pipeline': pipeline,
                    'type': 'poisson' if is_poisson else 'rate',
                    'mae': mean_mae,
                    'features': X.columns.tolist(),
                    'oof_predictions': pd.Series(oof_preds, index=X.index)
                }
            except Exception as e:
                LOG.warning(f"Final fit failed for {name}: {e}")
                trained_pipelines[name] = None

    results_df = pd.DataFrame(results_list)
    tuned_results = {}
    if not results_df.empty:
        results_df = results_df.set_index('Model').sort_values('Mean MAE', ascending=True)
        tuned_results = tune_models_hybrid(
            X, y_rate_per_person, y_rate_log, population, sample_weights,
            results_df, trained_pipelines, groups, params
        )

        all_pipelines = {**trained_pipelines, **tuned_results}

        for name, info in tuned_results.items():
            tuned_r2 = r2_score(y_rate.values, info['oof_predictions'].values)
            results_df.loc[name] = {
                "Type": "Tuned " + info['type'], "Mean MAE": info['mae'], "Std MAE": 0,
                "Mean R2": tuned_r2, "Std R2": 0, "Mean RMSE": np.nan
            }

        results_df = results_df.sort_values('Mean MAE', ascending=True)
        ensemble_results = create_hybrid_ensembles(results_df, all_pipelines, y_rate)

        for name, info in ensemble_results.items():
            results_df.loc[name] = {
                "Type": "Ensemble", "Mean MAE": info['mae'], "Std MAE": 0,
                "Mean R2": info.get('r2', np.nan), "Std R2": 0, "Mean RMSE": np.nan
            }

    results_df = results_df.sort_values('Mean MAE', ascending=True)
    all_results = {**{k: v for k, v in trained_pipelines.items() if v is not None}, **tuned_results, **ensemble_results}

    return results_df, all_results


def tune_models_hybrid(X, y_rate_per_person, y_rate_log, population, sample_weights,
                      results_df, trained_pipelines, groups, params):
    """Tune top performing models with Optuna"""
    LOG.info("Tuning top models")
    
    import lightgbm as lgb
    import xgboost as xgb
    import catboost as cb

    RS = params.get('random_state', 42)
    N_SPLITS = params.get('n_splits_cv', 5)
    N_TRIALS = params.get('optuna_trials', 30)

    valid_models = [m for m in results_df.index if m in trained_pipelines and trained_pipelines[m] is not None]
    top_models = valid_models[:2]
    tuned_results = {}

    if groups is not None:
        cv_splits = list(GroupKFold(n_splits=min(N_SPLITS, groups.nunique())).split(X, y_rate_per_person, groups))
    else:
        cv_splits = list(KFold(n_splits=N_SPLITS, shuffle=True, random_state=RS).split(X, y_rate_per_person))

    for model_name in top_models:
        base_info = trained_pipelines[model_name]
        is_poisson = base_info['type'] == 'poisson'

        study = optuna.create_study(direction='minimize', study_name=f"{model_name}_tuning", pruner=MedianPruner())

        def objective(trial):
            if model_name == "LightGBM":
                model = lgb.LGBMRegressor(
                    objective='poisson',
                    n_estimators=trial.suggest_int('n_estimators', 50, 300),
                    learning_rate=trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
                    max_depth=trial.suggest_int('max_depth', 3, 10),
                    num_leaves=trial.suggest_int('num_leaves', 10, 50),
                    min_child_samples=trial.suggest_int('min_child_samples', 2, 15),
                    random_state=RS,
                    verbosity=-1
                )
            elif model_name == "XGBoost":
                model = xgb.XGBRegressor(
                    objective='count:poisson',
                    n_estimators=trial.suggest_int('n_estimators', 50, 300),
                    learning_rate=trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
                    max_depth=trial.suggest_int('max_depth', 3, 10),
                    random_state=RS
                )
            elif model_name == "CatBoost":
                model = cb.CatBoostRegressor(
                    iterations=trial.suggest_int('iterations', 50, 300),
                    depth=trial.suggest_int('depth', 3, 8),
                    learning_rate=trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
                    verbose=0,
                    random_seed=RS
                )
            elif model_name == "Random Forest":
                model = RandomForestRegressor(
                    n_estimators=trial.suggest_int('n_estimators', 50, 300),
                    max_depth=trial.suggest_int('max_depth', 3, 15),
                    min_samples_split=trial.suggest_int('min_samples_split', 2, 10),
                    random_state=RS
                )
            elif model_name == "Gradient Boosting":
                model = GradientBoostingRegressor(
                    n_estimators=trial.suggest_int('n_estimators', 50, 300),
                    max_depth=trial.suggest_int('max_depth', 3, 8),
                    learning_rate=trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
                    random_state=RS
                )
            else:
                model = Ridge(
                    alpha=trial.suggest_float('alpha', 0.01, 10.0, log=True),
                    random_state=RS
                )

            pipeline = Pipeline([('scaler', StandardScaler()), ('model', model)])
            mae_scores = []

            for train_idx, val_idx in cv_splits:
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]

                if is_poisson:
                    y_train, y_val = y_rate_per_person.iloc[train_idx], y_rate_per_person.iloc[val_idx]
                    pipeline.fit(X_train, y_train, model__sample_weight=population.iloc[train_idx].values)
                    pred_rates = np.maximum(0, pipeline.predict(X_val) * 1000)
                    actual_rates = y_val.values * 1000
                else:
                    y_train, y_val = y_rate_log.iloc[train_idx], y_rate_log.iloc[val_idx]
                    pipeline.fit(X_train, y_train, model__sample_weight=sample_weights.iloc[train_idx].values)
                    pred_rates = np.maximum(0, np.expm1(pipeline.predict(X_val)))
                    actual_rates = np.expm1(y_val.values)

                mae_scores.append(mean_absolute_error(actual_rates, pred_rates))

            return np.mean(mae_scores)

        try:
            study.optimize(objective, n_trials=N_TRIALS)
        except KeyboardInterrupt:
            LOG.warning(f"Tuning interrupted for {model_name}")
            continue

        best_params = study.best_params

        if model_name == "LightGBM":
            best_model = lgb.LGBMRegressor(objective='poisson', random_state=RS, verbosity=-1, **best_params)
        elif model_name == "XGBoost":
            best_model = xgb.XGBRegressor(objective='count:poisson', random_state=RS, **best_params)
        elif model_name == "CatBoost":
            best_model = cb.CatBoostRegressor(verbose=0, random_seed=RS, **best_params)
        elif model_name == "Random Forest":
            best_model = RandomForestRegressor(random_state=RS, **best_params)
        elif model_name == "Gradient Boosting":
            best_model = GradientBoostingRegressor(random_state=RS, **best_params)
        else:
            best_model = Ridge(random_state=RS, **best_params)

        pipeline = Pipeline([('scaler', StandardScaler()), ('model', best_model)])

        oof_preds = np.zeros(len(X))
        for train_idx, val_idx in cv_splits:
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            if is_poisson:
                pipeline.fit(X_train, y_rate_per_person.iloc[train_idx], model__sample_weight=population.iloc[train_idx].values)
                oof_preds[val_idx] = np.maximum(0, pipeline.predict(X_val) * 1000)
            else:
                pipeline.fit(X_train, y_rate_log.iloc[train_idx], model__sample_weight=sample_weights.iloc[train_idx].values)
                oof_preds[val_idx] = np.maximum(0, np.expm1(pipeline.predict(X_val)))

        if is_poisson:
            pipeline.fit(X, y_rate_per_person, model__sample_weight=population.values)
        else:
            pipeline.fit(X, y_rate_log, model__sample_weight=sample_weights.values)

        tuned_results[f"{model_name}_Tuned"] = {
            'pipeline': pipeline,
            'type': 'poisson' if is_poisson else 'rate',
            'features': X.columns.tolist(),
            'best_params': best_params,
            'mae': study.best_value,
            'oof_predictions': pd.Series(oof_preds, index=X.index)
        }

    return tuned_results


def create_hybrid_ensembles(results_df, trained_pipelines, y_rate):
    """Create ensemble"""
    ensemble_results = {}

    valid_models = [m for m in results_df.index if m in trained_pipelines and 'oof_predictions' in trained_pipelines[m]]
    top_models = valid_models[:3]

    if len(top_models) < 2:
        return ensemble_results

    members = [trained_pipelines[name] for name in top_models]
    oof_preds_list = [trained_pipelines[name]['oof_predictions'] for name in top_models]
    ensemble_oof = pd.concat(oof_preds_list, axis=1).mean(axis=1)

    mae = mean_absolute_error(y_rate.values, ensemble_oof.values)
    r2 = r2_score(y_rate.values, ensemble_oof.values)

    ensemble_results["HybridEnsemble"] = {
        'type': 'ensemble',
        'base_models': top_models,
        'members': members,
        'mae': mae,
        'r2': r2,
        'oof_predictions': ensemble_oof
    }

    LOG.info(f"Hybrid ensemble OOF MAE: {mae:.4f}")
    return ensemble_results


def predict_rates(model_info: dict, X: pd.DataFrame, population: pd.Series) -> np.ndarray:
    """Predict visit rates using the specified model."""
    if model_info is None:
        return np.zeros(len(X))

    if model_info.get('type') == 'ensemble':
        if 'members' not in model_info or not model_info['members']:
            return np.zeros(len(X))

        predictions = []
        for member_info in model_info['members']:
            pred = predict_rates(member_info, X, population)
            predictions.append(pred)

        if not predictions:
            return np.zeros(len(X))
        return np.mean(predictions, axis=0)

    if 'pipeline' not in model_info:
        return np.zeros(len(X))

    pipeline = model_info['pipeline']
    model_type = model_info['type']
    X = X.copy()

    if 'features' in model_info:
        required_features = model_info['features']
        missing = [c for c in required_features if c not in X.columns]
        if missing:
            raise ValueError(
                "Prediction input is missing required features: " + ", ".join(missing)
            )
        X = X[required_features]

    X = X.fillna(0)

    if model_type == 'poisson':
        pred_per_person = pipeline.predict(X)
        pred_rates = pred_per_person * 1000
    elif model_type == 'rate':
        pred_log = pipeline.predict(X)
        pred_rates = np.expm1(pred_log)
    else:
        raise ValueError(f"Unknown model type: {model_type}")

    return np.maximum(0, pred_rates)


def select_best_model(results_df: pd.DataFrame, model_dict: dict) -> tuple:
    """Select best model based on MAE."""
    valid_models = []
    for name, info in model_dict.items():
        if info is not None and isinstance(info, dict) and 'mae' in info:
            if not np.isnan(info['mae']) and info['mae'] != float('inf'):
                valid_models.append((name, info))

    if not valid_models:
        LOG.error("No valid models found")
        for name, info in model_dict.items():
            if info is not None:
                return name, info, (info.get('type') == 'rate')
        return None, None, False

    valid_models.sort(key=lambda x: x[1]['mae'])
    best_name, best_info = valid_models[0]
    used_log_transform = best_info.get('type') == 'rate'

    LOG.info(f"Selected model: {best_name}")
    LOG.info(f"Model Type: {'Poisson' if not used_log_transform else 'Rate'}")
    LOG.info(f"MAE: {best_info.get('mae', 0):.4f} visits/1000")

    return best_name, best_info, used_log_transform


def get_explanation_model(model_info: dict):
    """Extract SHAP-compatible model from ensemble."""
    if model_info is None:
        return None

    if model_info.get('type') == 'ensemble':
        members = model_info.get('members', [])
        if not members:
            return None
        return min(members, key=lambda m: m.get('mae', float('inf')))
    return model_info



def format_model_ranking_table(results_df: pd.DataFrame, max_rows: int | None = None) -> str:
    """Return a compact plain-text model ranking table for CLI output."""
    if results_df is None or results_df.empty:
        return ""

    display_df = results_df.reset_index().copy()
    display_df = display_df.rename(columns={'index': 'Model'})
    if 'Mean MAE' not in display_df.columns:
        return ""

    display_df = display_df[~display_df['Mean MAE'].isna()].copy()
    if display_df.empty:
        return ""

    display_df = display_df.sort_values('Mean MAE', ascending=True).reset_index(drop=True)
    if max_rows is not None and max_rows > 0:
        display_df = display_df.head(max_rows)

    rows = []
    for idx, row in display_df.iterrows():
        mae = row.get('Mean MAE')
        r2 = row.get('Mean R2')
        rmse = row.get('Mean RMSE')
        rows.append({
            'Rank': str(idx + 1),
            'Model': str(row.get('Model', '')),
            'Type': str(row.get('Type', '')),
            'Mean MAE': '' if pd.isna(mae) else f"{float(mae):.4f}",
            'Mean R2': '' if pd.isna(r2) else f"{float(r2):.4f}",
            'Mean RMSE': '' if pd.isna(rmse) else f"{float(rmse):.4f}",
        })

    headers = ['Rank', 'Model', 'Type', 'Mean MAE', 'Mean R2', 'Mean RMSE']
    widths = {
        header: max(len(header), *(len(row[header]) for row in rows))
        for header in headers
    }

    def fmt(values: dict[str, str]) -> str:
        return '  '.join(values[header].ljust(widths[header]) for header in headers).rstrip()

    header_row = fmt({header: header for header in headers})
    separator = '  '.join('-' * widths[header] for header in headers).rstrip()
    body = [fmt(row) for row in rows]
    return '\n'.join(['Model ranking by Mean MAE', header_row, separator, *body])

def save_model_performance_outputs(results_df: pd.DataFrame, config: dict, best_model_name: str | None = None) -> dict:
    """Persist model performance outputs and return the key file paths."""
    if results_df is None or results_df.empty:
        return {}

    display_df = results_df.reset_index().copy()
    display_df = display_df.rename(columns={'index': 'Model'})
    display_df = display_df[~display_df['Mean MAE'].isna()]
    if display_df.empty:
        return {}

    paths = save_dataframe_bundle(
        display_df.round(4),
        'model_performance',
        config,
        title='Model Performance',
        index=False,
        section='',
    )

    artifact_dir = Path(config.get('artifact_dir', config.get('output_dir', 'outputs')))
    artifact_dir.mkdir(parents=True, exist_ok=True)
    summary_path = artifact_dir / 'model_performance_summary.json'
    best_row = display_df.iloc[0].to_dict()
    summary = {
        'best_model': best_model_name or str(best_row.get('Model')),
        'best_mae': None if pd.isna(best_row.get('Mean MAE')) else float(best_row.get('Mean MAE')),
        'best_r2': None if pd.isna(best_row.get('Mean R2')) else float(best_row.get('Mean R2')),
        'models_evaluated': int(len(display_df)),
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')

    report_html = '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8" /><title>Model Performance</title></head><body><h1>Model Performance</h1>'
    report_html += f"<p>Best model: <strong>{summary['best_model']}</strong>; MAE: {summary['best_mae']}</p>"
    report_html += display_df.round(4).to_html(index=False, border=0)
    report_html += '</body></html>'
    report_path = save_text_report(report_html, 'model_performance.html', config)

    paths['summary_json'] = str(summary_path)
    paths['report'] = report_path
    return paths


def save_model_bundle(X: pd.DataFrame, best_model_info: dict, model_dict: dict, used_log_transform: bool, population: pd.Series, config: dict) -> str | None:
    """Persist the fitted model bundle needed for Stage 4+ reruns."""
    checkpoint_dir = Path(config.get('checkpoint_dir', config.get('output_dir', 'outputs')))
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = checkpoint_dir / 'model_bundle.joblib'
    bundle = {
        'X': X,
        'best_model_info': best_model_info,
        'model_dict': model_dict,
        'used_log_transform': bool(used_log_transform),
        'population': population,
        'selected_features': list(X.columns),
    }
    try:
        import joblib

        joblib.dump(bundle, bundle_path)
        LOG.info("Saved model bundle checkpoint: %s", bundle_path)
        return str(bundle_path)
    except Exception as exc:
        LOG.warning("Could not save model bundle checkpoint: %s", exc)
        return None


def execute_modeling_pipeline(ml_dataset: pd.DataFrame, config: dict) -> tuple:
    """Execute modeling pipeline with hybrid Poisson/Rate approach."""
    LOG.info("Preparing data for modeling")
    feature_cols = config['selected_features'].copy()

    validate_selected_features(ml_dataset, feature_cols)

    X = ml_dataset[feature_cols].copy()
    y_visits = ml_dataset['Visits'].copy()
    population = ml_dataset['Population'].copy()

    for col in feature_cols:
        if X[col].dtype in ['float64', 'int64']:
            X[col] = X[col].fillna(0)
        else:
            X[col] = X[col].fillna('Unknown')

    groups = ml_dataset['Authority_Name'].copy().fillna('Unknown') if 'Authority_Name' in ml_dataset.columns else None

    results_df, model_dict = train_and_evaluate(X, y_visits, population, config['model_params'], groups)

    best_model_name, best_model_info, used_log_transform = select_best_model(results_df, model_dict)

    bundle_path = save_model_bundle(X, best_model_info, model_dict, used_log_transform, population, config)

    ranking_table = format_model_ranking_table(results_df)
    if ranking_table:
        print(ranking_table)

    paths = save_model_performance_outputs(results_df, config, best_model_name)
    if paths:
        print("Model performance outputs:")
        print(f"  - table: {paths.get('csv') or paths.get('html')}")
        if paths.get('summary_json'):
            print(f"  - summary: {paths['summary_json']}")
        if bundle_path:
            print(f"  - model bundle: {bundle_path}")
        if best_model_name:
            mae = best_model_info.get('mae') if isinstance(best_model_info, dict) else None
            mae_text = f"; MAE={mae:.4f}" if isinstance(mae, (int, float)) else ""
            print(f"  - selected model: {best_model_name}{mae_text}")
    else:
        print("Model performance outputs: no valid models found")

    return X, best_model_info, model_dict, used_log_transform, population