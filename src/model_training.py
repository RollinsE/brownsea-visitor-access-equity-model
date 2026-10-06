# -*- coding: utf-8 -*-
"""Modelling stage: train the models, pick the best one and save the outputs.

The training and scoring itself lives in src/model_evaluation.py.
"""

import logging
import json
from pathlib import Path
import numpy as np
import pandas as pd

LOG = logging.getLogger("Brownsea_Equity_Analysis")


from src.validation import validate_selected_features
from src.reporting import save_dataframe_bundle, save_text_report
from src.model_evaluation import (
    area_mask, areas_without_real_features, clean_features, study_area_mask, train_and_evaluate,
)


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

def _diagnostics_html(diagnostics: dict | None) -> str:
    """Plain-language notes on how the figures in the performance report were produced."""
    if not diagnostics:
        return ''
    lines = [
        f"Trained on {diagnostics.get('districts_used')} districts and scored on the "
        f"{diagnostics.get('districts_scored')} in the study area, out of {diagnostics.get('districts_in_dataset')} in the dataset.",
        f"Method: {diagnostics.get('cross_validation')}. Errors are in {diagnostics.get('error_unit')}.",
        f"Average observed rate: {diagnostics.get('mean_observed_rate', float('nan')):.2f}. "
        f"Assuming the same rate everywhere gives a mean absolute error of {diagnostics.get('baseline_mae', float('nan')):.2f}; "
        f"the selected model gives {diagnostics.get('best_mae', float('nan')):.2f}.",
    ]
    lines += list(diagnostics.get('notes', []))
    return '<h2>How to read this</h2><ul>' + ''.join(f'<li>{line}</li>' for line in lines) + '</ul>'


def save_model_performance_outputs(results_df: pd.DataFrame, config: dict, best_model_name: str | None = None,
                                   diagnostics: dict | None = None) -> dict:
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
    selected = display_df[display_df['Model'] == best_model_name] if best_model_name else display_df.iloc[0:0]
    best_row = (selected if not selected.empty else display_df).iloc[0].to_dict()
    summary = {
        'best_model': best_model_name or str(best_row.get('Model')),
        'best_mae': None if pd.isna(best_row.get('Mean MAE')) else float(best_row.get('Mean MAE')),
        'best_r2': None if pd.isna(best_row.get('Mean R2')) else float(best_row.get('Mean R2')),
        'models_evaluated': int(len(display_df)),
    }
    if diagnostics:
        summary.update({
            'districts_used': diagnostics.get('districts_used'),
            'baseline_mae': diagnostics.get('baseline_mae'),
            'best_rmse': diagnostics.get('best_rmse'),
            'skill_vs_baseline': diagnostics.get('skill_vs_baseline'),
            'error_unit': diagnostics.get('error_unit'),
        })
        (artifact_dir / 'model_diagnostics.json').write_text(json.dumps(diagnostics, indent=2, default=str), encoding='utf-8')
    summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')

    report_html = '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8" /><title>Model Performance</title></head><body><h1>Model Performance</h1>'
    report_html += f"<p>Best model: <strong>{summary['best_model']}</strong>; MAE: {summary['best_mae']}</p>"
    report_html += display_df.round(4).to_html(index=False, border=0)
    report_html += _diagnostics_html(diagnostics)
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
    """Train and score the models on the study area, then save the best one and its reports."""
    LOG.info("Preparing data for modeling")
    feature_cols = config['selected_features'].copy()

    validate_selected_features(ml_dataset, feature_cols)

    y_visits = ml_dataset['Visits'].copy()
    population = ml_dataset['Population'].copy()
    groups = ml_dataset['Authority_Name'].copy().fillna('Unknown') if 'Authority_Name' in ml_dataset.columns else None

    districts = ml_dataset['District']
    extra_areas = list(config['model_params'].get('extra_training_areas') or [])
    not_routed = areas_without_real_features(ml_dataset, districts, extra_areas)
    if not_routed:
        raise ValueError(
            f"Extra training areas {not_routed} have no real journey features in this dataset. "
            "Rerun stage 1 with them in BROWNSEA_EXTRA_TRAINING_AREAS, or remove them from extra_training_areas."
        )
    # Only training rows are cleaned: elsewhere the zeros are placeholders, not gaps to fill.
    X, cleaning = clean_features(ml_dataset[feature_cols], study_area_mask(districts) | area_mask(districts, extra_areas))
    X = X.fillna(0)
    results_df, model_dict, diagnostics = train_and_evaluate(
        X, y_visits, population, config['model_params'], groups, districts
    )
    diagnostics['feature_cleaning'] = cleaning

    best_model_name, best_model_info, used_log_transform = select_best_model(results_df, model_dict)

    bundle_path = save_model_bundle(X, best_model_info, model_dict, used_log_transform, population, config)

    ranking_table = format_model_ranking_table(results_df)
    if ranking_table:
        print(ranking_table)

    paths = save_model_performance_outputs(results_df, config, best_model_name, diagnostics)
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