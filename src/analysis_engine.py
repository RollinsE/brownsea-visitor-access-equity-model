# -*- coding: utf-8 -*-
"""Priority and intervention analysis services."""

import logging
import os
from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px
from typing import Optional

from src.constants import (
    DeprivationConstants, TierLabels, PriorityZones, InterventionTypes,
    FeatureConstants, ModelConstants, GeographicConstants
)
from src.business_scoring import (
    calculate_fragility_score, calculate_growth_potential_scores,
    calculate_early_warnings, identify_quick_wins, calculate_safe_zone_benchmarks
)
from src.utils import get_deprivation_tier, get_timestamp
from src.reporting import save_dataframe_bundle, save_plotly_bundle, save_text_report

LOG = logging.getLogger("Brownsea_Equity_Analysis")


def _print_saved(label: str, paths: dict | str | None) -> None:
    """Print a compact, file-first CLI message for saved outputs."""
    if not paths:
        return
    if isinstance(paths, dict):
        preferred = paths.get('html') or paths.get('csv') or paths.get('png') or paths.get('json') or paths.get('xlsx')
    else:
        preferred = paths
    if preferred:
        print(f"  - {label}: {preferred}")


# =============================================================================
# PRIORITY & INTERVENTION SERVICES
# =============================================================================

class PriorityZoneService:
    """Service for determining Priority Action Matrix Zones using composite need scoring."""

    @staticmethod
    def calculate_composite_need_score(fsm_percentage, deprivation_percentage):
        if pd.isna(fsm_percentage) or pd.isna(deprivation_percentage):
            return 0
        return (fsm_percentage * DeprivationConstants.FSM_WEIGHT +
                deprivation_percentage * DeprivationConstants.DEPRIVATION_WEIGHT)

    @staticmethod
    def calculate_need_tier(row):
        fsm = row['avg_fsm%']
        deprivation = row['pop%_most_deprived']
        composite_score = PriorityZoneService.calculate_composite_need_score(fsm, deprivation)

        if composite_score >= DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD:
            return TierLabels.HIGH_NEED
        elif composite_score >= DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD:
            return TierLabels.MEDIUM_NEED
        else:
            return TierLabels.LOW_NEED

    @staticmethod
    def calculate_visit_rate_tier(visit_rate):
        if pd.isna(visit_rate):
            return 'Unknown'
        if visit_rate < DeprivationConstants.LOW_VISIT_RATE_THRESHOLD:
            return TierLabels.LOW_VISIT_RATE
        elif visit_rate < DeprivationConstants.MEDIUM_VISIT_RATE_THRESHOLD:
            return TierLabels.MEDIUM_VISIT_RATE
        else:
            return TierLabels.HIGH_VISIT_RATE

    @staticmethod
    def assign_priority_zone(row):
        need_tier = PriorityZoneService.calculate_need_tier(row)
        visit_tier = PriorityZoneService.calculate_visit_rate_tier(row['visits_per_1000'])

        zone_map = {
            (TierLabels.HIGH_NEED, TierLabels.LOW_VISIT_RATE): PriorityZones.URGENT_ACTION,
            (TierLabels.HIGH_NEED, TierLabels.MEDIUM_VISIT_RATE): PriorityZones.MONITOR,
            (TierLabels.HIGH_NEED, TierLabels.HIGH_VISIT_RATE): PriorityZones.MAINTAIN,
            (TierLabels.MEDIUM_NEED, TierLabels.LOW_VISIT_RATE): PriorityZones.HIGH_PRIORITY,
            (TierLabels.MEDIUM_NEED, TierLabels.MEDIUM_VISIT_RATE): PriorityZones.MONITOR,
            (TierLabels.MEDIUM_NEED, TierLabels.HIGH_VISIT_RATE): PriorityZones.MAINTAIN,
            (TierLabels.LOW_NEED, TierLabels.LOW_VISIT_RATE): PriorityZones.GROWTH_OPPORTUNITY,
            (TierLabels.LOW_NEED, TierLabels.MEDIUM_VISIT_RATE): PriorityZones.MONITOR,
            (TierLabels.LOW_NEED, TierLabels.HIGH_VISIT_RATE): PriorityZones.MAINTAIN
        }

        return zone_map.get((need_tier, visit_tier), PriorityZones.MONITOR)

    @staticmethod
    def add_zone_data(data):
        data['composite_need_score'] = data.apply(
            lambda row: PriorityZoneService.calculate_composite_need_score(
                row['avg_fsm%'], row['pop%_most_deprived']
            ), axis=1
        )

        if 'impact_score' not in data.columns:
            data['impact_score'] = (
                data['composite_need_score'] * 0.7 +
                (100 - data['visits_per_1000'] * 10) * 0.3
            )

        data['need_tier'] = data.apply(PriorityZoneService.calculate_need_tier, axis=1)
        data['visit_tier'] = data['visits_per_1000'].apply(PriorityZoneService.calculate_visit_rate_tier)
        data['priority_zone'] = data.apply(PriorityZoneService.assign_priority_zone, axis=1)

        return data


def diagnose_intervention_type(row):
    """Determine intervention type based on need and visit rate."""
    composite_score = row.get('composite_need_score', 0)
    visit_rate = row['visits_per_1000']

    if visit_rate >= DeprivationConstants.MEDIUM_VISIT_RATE_THRESHOLD:
        return InterventionTypes.MODEL_DISTRICT

    elif visit_rate < DeprivationConstants.LOW_VISIT_RATE_THRESHOLD:
        if composite_score >= DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD:
            return InterventionTypes.CRISIS_INTERVENTION
        elif composite_score >= DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD:
            return InterventionTypes.TARGETED_SUPPORT
        else:
            return InterventionTypes.GROWTH_AWARENESS

    return InterventionTypes.SUSTAIN_OPTIMIZE


def get_fsm_tier(row) -> str:
    """Determine FSM tier based on percentage."""
    if pd.isna(row['avg_fsm%']):
        return 'Unknown'
    if row['avg_fsm%'] >= DeprivationConstants.HIGH_FSM_THRESHOLD:
        return TierLabels.HIGH_FSM
    elif row['avg_fsm%'] >= DeprivationConstants.MEDIUM_FSM_THRESHOLD:
        return TierLabels.MEDIUM_FSM
    else:
        return TierLabels.LOW_FSM


def apply_categorizations(data):
    """Apply all classification tiers to the dataset."""
    data['deprivation_tier'] = data.apply(get_deprivation_tier, axis=1)
    data['fsm_tier'] = data.apply(get_fsm_tier, axis=1)
    data['visit_rate_tier'] = data['visits_per_1000'].apply(PriorityZoneService.calculate_visit_rate_tier)
    data['composite_need_tier'] = data.apply(PriorityZoneService.calculate_need_tier, axis=1)

    data['intersection_segment'] = (
        data['deprivation_tier'] + " + " +
        data['fsm_tier'] + " + " +
        data['visit_rate_tier']
    )
    return data


def filter_bcp_dorset_districts(data: pd.DataFrame) -> pd.DataFrame:
    """Filter to BCP and Dorset postcode areas."""
    return data[
        data['District'].str.contains(
            '|'.join([f'^{area}' for area in GeographicConstants.BCP_DORSET_POSTCODES]),
            na=False
        )
    ].copy()


def calculate_visit_metrics(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate visit-related metrics."""
    data['Population'] = pd.to_numeric(data['Population'], errors='coerce').replace(0, np.nan)
    data['visits_per_1000'] = (data['Visits'] / data['Population']) * 1000
    data['visits_per_1000'] = data['visits_per_1000'].fillna(0)

    target_visit_rate = DeprivationConstants.TARGET_VISIT_RATE

    data['visits_gap_raw'] = target_visit_rate - data['visits_per_1000']
    data['visits_gap'] = data['visits_gap_raw'].clip(lower=0)
    return data


def assign_intervention_types(data: pd.DataFrame) -> pd.DataFrame:
    """Assign intervention types to each district."""
    intervention_results = []
    for _, row in data.iterrows():
        intervention_results.append(diagnose_intervention_type(row))
    data['intervention_type'] = intervention_results
    return data


def add_model_predictions(analysis_df: pd.DataFrame, model_info,
                         X: pd.DataFrame, population: pd.Series) -> pd.DataFrame:
    """Add model predictions using Out-Of-Fold rates to prevent bias."""
    if model_info is None:
        analysis_df['predicted_visit_rate'] = np.nan
        analysis_df['performance_gap'] = np.nan
        return analysis_df

    common_indices = analysis_df.index.intersection(X.index)
    if len(common_indices) == 0:
        analysis_df['predicted_visit_rate'] = np.nan
        analysis_df['performance_gap'] = np.nan
        return analysis_df

    if 'oof_predictions' in model_info:
        LOG.info("Applying unbiased OOF predictions to prevent in-sample memorization")
        pred_series = model_info['oof_predictions'].loc[common_indices]
    else:
        LOG.warning("OOF predictions missing. Falling back to biased in-sample predictions.")
        from src.model_training import predict_rates
        X_aligned = X.loc[common_indices]
        pop_aligned = population.loc[common_indices]
        predictions = predict_rates(model_info, X_aligned, pop_aligned)
        pred_series = pd.Series(predictions, index=common_indices)

    analysis_df['predicted_visit_rate'] = pred_series
    analysis_df['predicted_visit_rate'] = analysis_df['predicted_visit_rate'].fillna(0)
    analysis_df['performance_gap'] = analysis_df['predicted_visit_rate'] - analysis_df['visits_per_1000']
    analysis_df['performance_gap'] = analysis_df['performance_gap'].fillna(0)

    return analysis_df


def perform_sensitivity_analysis(data: pd.DataFrame, feature_name: str,
                                delta: float, model_info, X: pd.DataFrame,
                                population: pd.Series) -> pd.DataFrame:
    """Perform sensitivity analysis using unified prediction."""
    LOG.info(f"Sensitivity analysis: {feature_name} ±{delta}")

    if feature_name not in X.columns:
        LOG.warning(f"Feature {feature_name} not found")
        return data

    common_indices = data.index.intersection(X.index)
    if len(common_indices) == 0:
        return data

    X_sensitivity = X.loc[common_indices].copy()
    pop_sensitivity = population.loc[common_indices]
    X_sensitivity[feature_name] = X_sensitivity[feature_name] + delta

    try:
        from src.model_training import predict_rates
        new_predictions = predict_rates(model_info, X_sensitivity, pop_sensitivity)

        data.loc[common_indices, f'sensitivity_{feature_name}_delta'] = delta
        data.loc[common_indices, f'sensitivity_{feature_name}_new_pred'] = new_predictions
        data.loc[common_indices, f'sensitivity_{feature_name}_impact'] = (
            new_predictions - data.loc[common_indices, 'predicted_visit_rate']
        )

        LOG.info(f"Sensitivity impact: {data[f'sensitivity_{feature_name}_impact'].mean():.2f}")

    except Exception as e:
        LOG.error(f"Sensitivity analysis failed: {e}")

    return data


def generate_shap_narratives(data: pd.DataFrame, model_info, X: pd.DataFrame, 
                             population: pd.Series) -> pd.DataFrame:
    """Generate explanations using SHAP values with dynamic competitor context."""
    LOG.info("Generating SHAP-based narratives")
    
    import shap
    from src.model_training import get_explanation_model
    
    data['shap_narrative'] = ''
    data['shap_error'] = ''

    try:
        if model_info is None:
            data['shap_error'] = 'No model provided'
            return data

        explain_model = get_explanation_model(model_info)
        if explain_model is None or 'pipeline' not in explain_model:
            return data

        pipeline = explain_model['pipeline']
        model = pipeline.named_steps['model']
        scaler = pipeline.named_steps['scaler']

        common_indices = data.index.intersection(X.index)
        if len(common_indices) == 0:
            return data

        X_aligned = X.loc[common_indices]
        X_scaled = scaler.transform(X_aligned)

        model_class = type(model).__name__
        tree_models = ['RandomForestRegressor', 'GradientBoostingRegressor', 'LGBMRegressor', 'XGBRegressor', 'CatBoostRegressor']

        if any(t in model_class for t in tree_models):
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X_scaled)
        else:
            explainer = shap.LinearExplainer(model, X_scaled)
            shap_values = explainer.shap_values(X_scaled)

        feature_names = X.columns.tolist()
        narratives_created = 0
        all_shap_vals = []
        shap_value_map = {}

        for i, idx in enumerate(common_indices):
            try:
                if isinstance(shap_values, list):
                    vals = shap_values[0][i] if len(shap_values) > 0 and i < len(shap_values[0]) else None
                elif len(shap_values.shape) == 3:
                    vals = shap_values[0][i]
                elif len(shap_values.shape) == 2:
                    vals = shap_values[i] if i < shap_values.shape[0] else None
                else:
                    vals = None

                if vals is not None:
                    shap_value_map[idx] = vals
                    all_shap_vals.extend(np.abs(vals))
            except Exception:
                continue

        if not all_shap_vals:
            data['shap_narrative'] = 'No SHAP values generated'
            return data

        threshold = np.percentile(all_shap_vals, 25)
        tolerance = ModelConstants.PERFORMANCE_TOLERANCE

        for i, idx in enumerate(common_indices):
            try:
                if idx not in shap_value_map:
                    continue

                vals = shap_value_map[idx]
                actual = data.loc[idx, 'visits_per_1000']
                predicted = data.loc[idx, 'predicted_visit_rate']

                lower_bound = predicted * (1 - tolerance)
                upper_bound = predicted * (1 + tolerance)
                status = "Below expected" if actual < lower_bound else ("Above expected" if actual > upper_bound else "In line with expected")

                drivers, barriers = [], []
                for j, feat in enumerate(feature_names):
                    if j >= len(vals):
                        continue
                    shap_val = vals[j]
                    if abs(shap_val) > threshold:
                        desc = FeatureConstants.DESCRIPTIONS.get(feat, feat.replace('_', ' ').title())

                        if feat == 'nearest_competitor_drive_min' and 'competitor_context' in data.columns:
                            ctx = data.loc[idx, 'competitor_context']
                            if pd.notna(ctx) and ctx != "":
                                desc += str(ctx)

                        if shap_val > 0:
                            drivers.append((abs(shap_val), desc))
                        else:
                            barriers.append((abs(shap_val), desc))

                drivers.sort(reverse=True)
                barriers.sort(reverse=True)
                top_drivers = [desc for _, desc in drivers[:2]]
                top_barriers = [desc for _, desc in barriers[:2]]

                narrative_parts = [f"Engagement status: {status}"]
                if top_barriers: narrative_parts.append(f"Main barriers: {', '.join(top_barriers)}")
                if top_drivers: narrative_parts.append(f"Positive factors: {', '.join(top_drivers)}")

                narrative = " | ".join(narrative_parts)
                if 'fragility_score' in data.columns:
                    fragility = data.loc[idx, 'fragility_score']
                    if fragility > 50:
                        narrative += " [Less typical visitor pattern]"

                data.loc[idx, 'shap_narrative'] = narrative
                narratives_created += 1

            except Exception:
                continue

        if narratives_created == 0:
            data['shap_narrative'] = 'No significant SHAP values'

    except Exception as e:
        data['shap_error'] = str(e)[:100]
        data['shap_narrative'] = 'SHAP explanation unavailable'

    return data


# =============================================================================
# ANALYSIS FUNCTIONS
# =============================================================================

def display_analysis_statistics(data: pd.DataFrame, config: dict | None = None):
    """Persist key analysis statistics and print compact file-first summaries."""
    import plotly.graph_objects as go

    total_districts = len(data)
    total_population = data['Population'].sum()
    total_visits = data['Visits'].sum()
    avg_gap = data.get('performance_gap', pd.Series([0])).mean()

    fig = go.Figure()
    fig.add_trace(go.Indicator(mode="number", value=total_districts, title={"text": "Total Districts Analyzed", "font": {"size": 14, "color": "gray"}}, number={"font": {"size": 36, "color": "#2c3e50"}}, domain={'row': 0, 'column': 0}))
    fig.add_trace(go.Indicator(mode="number", value=total_population, title={"text": "Total Population", "font": {"size": 14, "color": "gray"}}, number={"valueformat": ",.0f", "font": {"size": 36, "color": "#2c3e50"}}, domain={'row': 0, 'column': 1}))
    fig.add_trace(go.Indicator(mode="number", value=total_visits, title={"text": "Total Member Visits", "font": {"size": 14, "color": "gray"}}, number={"valueformat": ",.0f", "font": {"size": 36, "color": "#2c3e50"}}, domain={'row': 0, 'column': 2}))
    fig.add_trace(go.Indicator(mode="number+delta", value=avg_gap, title={"text": "Avg Performance Gap", "font": {"size": 14, "color": "gray"}}, number={"valueformat": ".2f", "font": {"size": 36, "color": "#2c3e50"}, "suffix": " v/1k"}, delta={"reference": 0, "position": "right", "valueformat": ".2f"}, domain={'row': 0, 'column': 3}))
    fig.update_layout(grid={'rows': 1, 'columns': 4, 'pattern': "independent"}, margin=dict(t=40, b=20, l=20, r=20), height=150, paper_bgcolor="white")

    print("Analysis outputs:")
    if config:
        fig_paths = save_plotly_bundle(fig, 'analysis_kpis', config)
        table_paths = save_dataframe_bundle(pd.DataFrame([{
            'total_districts': total_districts,
            'total_population': total_population,
            'total_visits': total_visits,
            'avg_performance_gap': round(avg_gap, 4),
        }]), 'analysis_kpis', config, title='Analysis KPIs', index=False)
        _print_saved('analysis KPIs figure', fig_paths)
        _print_saved('analysis KPIs table', table_paths)
    else:
        print(f"  - districts={total_districts:,}, population={total_population:,.0f}, visits={total_visits:,.0f}, avg_gap={avg_gap:.2f}")

    if 'intervention_type' in data.columns:
        intervention_dist = data['intervention_type'].value_counts()
        df_summary = pd.DataFrame({
            'Intervention Type': intervention_dist.index,
            'Districts': intervention_dist.values,
            '% of Total': (intervention_dist.values / total_districts * 100).round(1),
        })
        if config:
            paths = save_dataframe_bundle(df_summary, 'intervention_strategy_distribution', config, title='Intervention Strategy Distribution', index=False)
            _print_saved('intervention distribution', paths)
        else:
            print(f"  - intervention distribution: {len(df_summary)} categories")

    if 'visits_per_1000' in data.columns:
        low_visit = len(data[data['visits_per_1000'] < 4])
        medium_visit = len(data[(data['visits_per_1000'] >= 4) & (data['visits_per_1000'] < 7)])
        high_visit = len(data[data['visits_per_1000'] >= 7])
        df_perf = pd.DataFrame({
            'Performance Tier': ['Low (<4)', 'Medium (4-7)', 'High (≥7)'],
            'Districts': [low_visit, medium_visit, high_visit],
            '% of Total': [low_visit/total_districts*100, medium_visit/total_districts*100, high_visit/total_districts*100],
        }).round(1)
        if config:
            paths = save_dataframe_bundle(df_perf, 'performance_tier_distribution', config, title='Performance Tier Distribution', index=False)
            _print_saved('performance tier distribution', paths)
        else:
            print(f"  - performance tiers: low={low_visit}, medium={medium_visit}, high={high_visit}")

def create_executive_summary_dashboard(data: pd.DataFrame, quick_wins: pd.DataFrame = None, config: dict | None = None):
    """Persist watchlists for immediate action and print compact file-first summaries."""
    sections = []
    if 'intervention_type' in data.columns:
        urgent = data[data['intervention_type'].str.contains('Crisis', na=False)]
        if not urgent.empty:
            display_cols = ['District', 'Authority_Name', 'visits_per_1000', 'avg_fsm%', 'pop%_most_deprived']
            available_cols = [c for c in display_cols if c in urgent.columns]
            sort_col = 'pop%_most_deprived' if 'pop%_most_deprived' in urgent.columns else 'avg_fsm%'
            urgent_display = urgent.nlargest(5, sort_col)[available_cols].copy().round(2)
            urgent_display.rename(columns={'visits_per_1000': 'Visits/1k', 'avg_fsm%': 'FSM %', 'pop%_most_deprived': 'Most Deprived %'}, inplace=True)
            sections.append(('urgent_action_required', 'Urgent Action Required', urgent_display, len(urgent)))

        targeted = data[data['intervention_type'].str.contains('Targeted', na=False)]
        if not targeted.empty:
            display_cols = ['District', 'Authority_Name', 'visits_per_1000', 'avg_fsm%', 'pop%_most_deprived']
            available_cols = [c for c in display_cols if c in targeted.columns]
            targeted_display = targeted.nsmallest(5, 'visits_per_1000')[available_cols].copy().round(2)
            targeted_display.rename(columns={'visits_per_1000': 'Visits/1k', 'avg_fsm%': 'FSM %', 'pop%_most_deprived': 'Most Deprived %'}, inplace=True)
            sections.append(('targeted_support_needed', 'Targeted Support Needed', targeted_display, len(targeted)))

    if quick_wins is not None and not quick_wins.empty:
        display_cols = ['District', 'Authority_Name', 'visits_per_1000', 'predicted_visit_rate', 'performance_gap']
        available_cols = [c for c in display_cols if c in quick_wins.columns]
        qw_display = quick_wins.head(5)[available_cols].copy().round(2)
        qw_display.rename(columns={'visits_per_1000': 'Current Visits/1k', 'predicted_visit_rate': 'Target Visits/1k', 'performance_gap': 'Growth Gap'}, inplace=True)
        sections.append(('quick_wins', 'Quick Wins', qw_display, len(quick_wins)))

    if config and sections:
        print("Executive summary outputs:")
        html_parts = ['<!DOCTYPE html><html lang="en"><head><meta charset="utf-8" /><title>Executive Summary</title><style>body{font-family:Arial,sans-serif;margin:24px;}table{border-collapse:collapse;width:100%;margin-bottom:24px;}th,td{border:1px solid #ddd;padding:8px;text-align:left;}th{background:#2c3e50;color:white;}tr:nth-child(even){background:#f8f8f8;}</style></head><body><h1>Priority Watchlists</h1>']
        for base_name, title, table_df, source_count in sections:
            paths = save_dataframe_bundle(table_df, base_name, config, title=title, index=False)
            html_parts.append(f'<h2>{title}</h2><p>Matching districts: {source_count:,}. Showing top {len(table_df):,}.</p>')
            html_parts.append(table_df.to_html(index=False, border=0))
            _print_saved(f'{title} watchlist', paths)
        html_parts.append('</body></html>')
        report_path = save_text_report(''.join(html_parts), 'executive_summary.html', config)
        _print_saved('executive summary report', report_path)
    elif sections:
        print("Executive summary outputs:")
        for _, title, table_df, source_count in sections:
            print(f"  - {title}: {source_count:,} matches; top {len(table_df):,} prepared")

def create_sensitivity_dashboard(data: pd.DataFrame, config: dict):
    """Display and persist sensitivity analysis results dynamically based on CONFIG scenarios."""
    import plotly.express as px
    from src.visualization import _show_fig

    sensitivity_cols = [col for col in data.columns if col.startswith('sensitivity_')]
    if not sensitivity_cols:
        LOG.info("No sensitivity analysis results available")
        return
    impact_cols = [col for col in sensitivity_cols if 'impact' in col]
    strategy_mapping = {scenario['feature']: scenario['strategy_name'] for scenario in config.get('sensitivity_scenarios', [])}

    summary_data = []
    for impact_col in impact_cols:
        feature_raw = impact_col.replace('sensitivity_', '').replace('_impact', '')
        delta_col = f'sensitivity_{feature_raw}_delta'
        strategy_name = strategy_mapping.get(feature_raw, feature_raw.replace('_', ' ').title())
        if delta_col in data.columns:
            delta = data[delta_col].iloc[0] if not data.empty else 0
            avg_impact = data[impact_col].mean()
            if avg_impact > 0:
                summary_data.append({'Intervention Strategy': strategy_name, 'Model Proxy (Delta)': f"{feature_raw} ({delta:+})", 'Avg Impact (Visits/1k)': avg_impact, 'Max Impact': data[impact_col].max()})

    if summary_data:
        df_summary = pd.DataFrame(summary_data).round(2)
        paths = save_dataframe_bundle(df_summary, 'sensitivity_summary', config, title='Sensitivity Summary', index=False)
        _print_saved('sensitivity summary', paths)

    for impact_col in impact_cols:
        feature_raw = impact_col.replace('sensitivity_', '').replace('_impact', '')
        strategy_name = strategy_mapping.get(feature_raw, feature_raw.replace('_', ' ').title())
        top_districts = data.nlargest(5, impact_col)[['District', 'Authority_Name', impact_col]]
        if not top_districts.empty and top_districts[impact_col].max() > 0:
            top_districts = top_districts[top_districts[impact_col] > 0].sort_values(by=impact_col, ascending=True)
            fig = px.bar(top_districts, x=impact_col, y='District', orientation='h', title=f'ROI: Top 5 Districts for {strategy_name}', labels={impact_col: 'Predicted Increase in Visits per 1000', 'District': ''}, hover_data=['Authority_Name'], color=impact_col, color_continuous_scale='Teal')
            fig.update_layout(height=300, margin=dict(l=20, r=20, t=50, b=20), coloraxis_showscale=False, paper_bgcolor="white", plot_bgcolor="white")
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')
            paths = save_plotly_bundle(fig, f'sensitivity_{feature_raw}', config)
            _print_saved(f'sensitivity figure - {strategy_name}', paths)


def display_strategic_framework_definitions(priority_df, intervention_df, need_df, config: dict | None = None):
    """Persist framework definitions and print compact file-first summaries."""
    if config:
        print("Framework definition outputs:")
        paths = save_dataframe_bundle(priority_df.reset_index(), 'priority_action_matrix_categories', config, title='Priority Action Matrix Categories', index=False)
        _print_saved('priority action matrix categories', paths)
        paths = save_dataframe_bundle(intervention_df.reset_index(), 'intervention_strategy_framework', config, title='Intervention Strategy Framework', index=False)
        _print_saved('intervention strategy framework', paths)
        paths = save_dataframe_bundle(need_df.reset_index(), 'need_tier_definitions', config, title='Need Tier Definitions', index=False)
        _print_saved('need tier definitions', paths)
        html = '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8" /><title>Strategic Framework Definitions</title><style>body{font-family:Arial,sans-serif;margin:24px;}table{border-collapse:collapse;width:100%;margin-bottom:24px;}th,td{border:1px solid #ddd;padding:8px;text-align:left;}th{background:#2c3e50;color:white;}tr:nth-child(even){background:#f8f8f8;}</style></head><body><h1>Strategic Framework Definitions</h1>'
        for title, df in [('Priority Action Matrix Categories', priority_df.reset_index()), ('Intervention Strategy Framework', intervention_df.reset_index()), ('Need Tier Definitions', need_df.reset_index())]:
            html += f'<h2>{title}</h2>' + df.to_html(index=False, border=0)
        html += '</body></html>'
        report_path = save_text_report(html, 'strategic_framework_definitions.html', config)
        _print_saved('strategic framework report', report_path)
    else:
        print(f"Framework definitions: priority={len(priority_df):,}, interventions={len(intervention_df):,}, need_tiers={len(need_df):,}")

def export_analysis_results(data: pd.DataFrame, config: dict):
    """Export analysis results to stable artifact/report locations."""

    base_columns = [
        'District', 'Post_Town', 'Authority_Name', 'Region_Name', 'intervention_type',
        'deprivation_tier', 'fsm_tier', 'visit_rate_tier', 'need_tier',
        'Population', 'Visits', 'visits_per_1000', 'visits_gap',
        'avg_fsm%', 'pop%_most_deprived', 'pop%_moderately_deprived', 'pop%_least_deprived',
        'total_journey_min', 'nearest_competitor_drive_min', 'nearest_ferry_terminal',
        'composite_need_score'
    ]
    business_columns = ['fragility_score', 'fragility_tier', 'fragility_alert',
                       'risk_score', 'risk_tier', 'risk_flags',
                       'performance_gap', 'predicted_visit_rate', 'shap_narrative']
    sensitivity_cols = [col for col in data.columns if col.startswith('sensitivity_')]
    if 'priority_zone' in data.columns:
        base_columns.append('priority_zone')

    available_columns = [col for col in base_columns if col in data.columns]
    for col in business_columns + sensitivity_cols:
        if col in data.columns:
            available_columns.append(col)

    export_data = data[available_columns].copy()
    bundle_paths = save_dataframe_bundle(export_data, 'district_analysis_export', config, title='District Analysis Export', index=False)

    artifact_dir = Path(config.get('artifact_dir', config.get('output_dir', 'outputs')))
    excel_filename = artifact_dir / 'district_analysis_export.xlsx'
    json_filename = artifact_dir / 'district_analysis_export.json'
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        export_data.to_excel(writer, sheet_name='District Analysis', index=False)
        summary_data = data.groupby('intervention_type').agg({'District': 'count', 'Population': 'sum', 'visits_per_1000': 'mean', 'visits_gap': 'sum'}).round(2)
        summary_data.to_excel(writer, sheet_name='Summary Statistics')
        if 'fragility_score' in data.columns:
            business_summary = data.groupby('fragility_tier').agg({'District': 'count', 'fragility_score': 'mean', 'visits_per_1000': 'mean'}).round(2)
            business_summary.to_excel(writer, sheet_name='Fragility Analysis')
    export_data.to_json(json_filename, orient='records', indent=2)
    print("Analysis export outputs:")
    _print_saved('district analysis table', bundle_paths)
    _print_saved('district analysis Excel', str(excel_filename))
    _print_saved('district analysis JSON', str(json_filename))
    print(f"  - exported rows: {len(export_data):,}")
    return export_data

def analyze_three_way_intersection(ml_dataset, final_model_info, X, used_log_transform, 
                                   population, lsoa_master_df, district_lsoa_map, config):
    """Execute comprehensive strategic analysis and generate visualization assets."""
    import pandas as pd
    import numpy as np
    
    LOG.info("Comprehensive strategic analysis started")

    analysis_df = filter_bcp_dorset_districts(ml_dataset)
    analysis_df = add_model_predictions(analysis_df, final_model_info, X, population)
    analysis_df = calculate_visit_metrics(analysis_df)
    analysis_df = PriorityZoneService.add_zone_data(analysis_df)
    analysis_df = assign_intervention_types(analysis_df)
    analysis_df = apply_categorizations(analysis_df)

    analysis_df = calculate_fragility_score(analysis_df)
    analysis_df = calculate_growth_potential_scores(analysis_df)
    analysis_df = calculate_early_warnings(analysis_df)

    model_rmse = final_model_info['mae'] if final_model_info is not None and 'mae' in final_model_info else None
    analysis_df = calculate_safe_zone_benchmarks(analysis_df, model_rmse)

    for scenario in config.get('sensitivity_scenarios', []):
        analysis_df = perform_sensitivity_analysis(
            analysis_df,
            scenario['feature'],
            scenario['delta'],
            final_model_info, X, population
        )

    analysis_df = generate_shap_narratives(analysis_df, final_model_info, X, population)
    
    from src.visualization import create_shap_summary_plot
    shap_path = os.path.join(config.get('report_dir', config.get('output_dir', 'outputs')), 'figures', 'shap_summary.png')
    os.makedirs(os.path.dirname(shap_path), exist_ok=True)
    create_shap_summary_plot(final_model_info, X.loc[analysis_df.index], population, save_path=shap_path, show_plot=bool(config.get('enable_inline_display', False)))
    display_analysis_statistics(analysis_df, config)

    print("Strategic visualization outputs:")

    if not analysis_df.empty:
        from src.visualization import create_priority_matrix_plot
        fig = create_priority_matrix_plot(analysis_df)
        if fig is not None:
            _print_saved('priority action matrix', save_plotly_bundle(fig, 'priority_action_matrix', config))

        from src.visualization import create_growth_opportunity_matrix
        fig = create_growth_opportunity_matrix(analysis_df)
        if fig is not None:
            _print_saved('growth opportunity matrix', save_plotly_bundle(fig, 'growth_opportunity_matrix', config))

        from src.visualization import create_safe_zone_visualization
        fig = create_safe_zone_visualization(analysis_df)
        if fig is not None:
            _print_saved('safe zone analysis', save_plotly_bundle(fig, 'safe_zone_analysis', config))

        from src.visualization import create_intervention_treemap
        fig = create_intervention_treemap(analysis_df)
        if fig is not None:
            _print_saved('intervention strategy treemap', save_plotly_bundle(fig, 'intervention_strategy_treemap', config))

        create_sensitivity_dashboard(analysis_df, config)

        from src.visualization import create_equity_gap_visualization
        fig = create_equity_gap_visualization(analysis_df)
        if fig is not None:
            _print_saved('equity gap analysis', save_plotly_bundle(fig, 'equity_gap_analysis', config))

        from src.visualization import prepare_visualization_data, load_geojson_data, create_choropleth_map
        district_summary = prepare_visualization_data(analysis_df)
        if district_summary is not None:
            combined_geojson = load_geojson_data(config['visualization'])
            if combined_geojson and len(combined_geojson.get('features', [])) > 0:
                fig = create_choropleth_map(district_summary, combined_geojson, 'Visits', 'Geographic Distribution of Member Visits', 'RdYlGn')
                if fig is not None:
                    _print_saved('map - member visits distribution', save_plotly_bundle(fig, 'map_member_visits_distribution', config))
                fig = create_choropleth_map(district_summary, combined_geojson, 'visits_per_1000', 'Geographic Distribution of Visit Rate', 'RdYlGn')
                if fig is not None:
                    _print_saved('map - visit rate distribution', save_plotly_bundle(fig, 'map_visit_rate_distribution', config))
                if 'fragility_score' in district_summary.columns:
                    fig = create_choropleth_map(district_summary, combined_geojson, 'fragility_score', 'Geographic Distribution of Fragility Score', 'RdYlGn_r')
                    if fig is not None:
                        _print_saved('map - fragility distribution', save_plotly_bundle(fig, 'map_fragility_distribution', config))

        quick_wins = identify_quick_wins(analysis_df, top_n=10)
        create_executive_summary_dashboard(analysis_df, quick_wins, config)

        export_analysis_results(analysis_df, config)

    if config:
        output_files = config.get('output_files', {})
        three_way_path = output_files.get('three_way_intersection')
        if three_way_path:
            three_way_path = Path(three_way_path)
            three_way_path.parent.mkdir(parents=True, exist_ok=True)
            analysis_df.to_csv(three_way_path, index=False)
            LOG.info("Saved three-way strategic analysis artifact: %s", three_way_path)
            print(f"Saved three-way strategic analysis: {three_way_path}")

        save_dataframe_bundle(analysis_df, 'analysis_table', config, title='Strategic Analysis Table', index=False)

    return analysis_df