# -*- coding: utf-8 -*-
"""Business scoring and opportunity assessment helpers."""

import logging
import numpy as np
import pandas as pd

from src.constants import (
    DeprivationConstants, ModelConstants, TierLabels, PriorityZones,
    InterventionTypes
)

LOG = logging.getLogger("Brownsea_Equity_Analysis")


def calculate_growth_potential_scores(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate business-oriented growth potential scores for districts."""
    LOG.info("Calculating growth potential scores")

    if 'performance_gap' in data.columns:
        max_gap = data['performance_gap'].max()
        data['gap_score'] = (data['performance_gap'] / max_gap * 100) if max_gap > 0 else 0
    else:
        data['gap_score'] = 0

    if 'composite_need_score' in data.columns:
        max_need = data['composite_need_score'].max()
        data['need_inverse_score'] = 100 - (data['composite_need_score'] / max_need * 100) if max_need > 0 else 100
    else:
        data['need_inverse_score'] = 0

    if 'Population' in data.columns:
        max_pop = data['Population'].max()
        data['population_score'] = (data['Population'] / max_pop * 100) if max_pop > 0 else 0
    else:
        data['population_score'] = 0

    data['accessibility_score_norm'] = data.get('accessibility_score', 0)

    score_components = []
    weights = []

    if 'gap_score' in data.columns:
        score_components.append(data['gap_score'])
        weights.append(0.35)

    if 'need_inverse_score' in data.columns:
        score_components.append(data['need_inverse_score'])
        weights.append(0.30)

    if 'population_score' in data.columns:
        score_components.append(data['population_score'])
        weights.append(0.20)

    if 'accessibility_score_norm' in data.columns:
        score_components.append(data['accessibility_score_norm'])
        weights.append(0.15)

    if score_components:
        weighted_sum = sum(comp * weight for comp, weight in zip(score_components, weights))
        total_weight = sum(weights)
        data['growth_potential_score'] = weighted_sum / total_weight

        min_score = data['growth_potential_score'].min()
        max_score = data['growth_potential_score'].max()
        if max_score > min_score:
            data['growth_potential_score'] = ((data['growth_potential_score'] - min_score) /
                                            (max_score - min_score)) * 100

    if 'growth_potential_score' in data.columns:
        conditions = [
            data['growth_potential_score'] >= 75,
            data['growth_potential_score'] >= 50,
            data['growth_potential_score'] >= 25,
            True
        ]
        choices = ['Very High', 'High', 'Medium', 'Low']
        data['growth_potential_tier'] = np.select(conditions, choices, default='Unknown')

    LOG.info(f"Growth potential scores calculated for {len(data)} districts")
    return data


def calculate_safe_zone_benchmarks(data: pd.DataFrame, model_rmse: float = None) -> pd.DataFrame:
    """Add safe zone bands based on ModelConstants buffer."""
    buffer = ModelConstants.SAFE_ZONE_BUFFER
    LOG.info(f"Calculating safe zone benchmarks (Fixed Buffer: ±{buffer})")

    data = data.copy()

    data['safe_zone_lower_1rmse'] = data['predicted_visit_rate'] - (buffer / 2)
    data['safe_zone_upper_1rmse'] = data['predicted_visit_rate'] + (buffer / 2)
    data['safe_zone_lower_2rmse'] = data['predicted_visit_rate'] - buffer
    data['safe_zone_upper_2rmse'] = data['predicted_visit_rate'] + buffer

    conditions = [
        data['visits_per_1000'] < data['safe_zone_lower_2rmse'],
        data['visits_per_1000'] < data['safe_zone_lower_1rmse'],
        data['visits_per_1000'] > data['safe_zone_upper_2rmse'],
        data['visits_per_1000'] > data['safe_zone_upper_1rmse'],
        True
    ]
    choices = [
        'Severe Underperformance',
        'Moderate Underperformance',
        'Severe Overperformance',
        'Moderate Overperformance',
        'Within Safe Zone'
    ]
    data['safe_zone_status'] = np.select(conditions, choices, default='Unknown')

    data['needs_intervention'] = data['visits_per_1000'] < data['safe_zone_lower_2rmse']
    data['high_potential_flag'] = data['visits_per_1000'] > data['safe_zone_upper_2rmse']

    data['underperformance_confidence'] = np.where(
        data['visits_per_1000'] < data['predicted_visit_rate'],
        (data['predicted_visit_rate'] - data['visits_per_1000']) / buffer,
        0
    ).clip(0, 3)

    LOG.info(f"Districts needing intervention: {data['needs_intervention'].sum()}")
    LOG.info(f"Districts with high potential: {data['high_potential_flag'].sum()}")

    return data


def calculate_fragility_score(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate fragility score to identify overperforming districts at risk."""
    LOG.info("Calculating fragility scores")

    if 'predicted_visit_rate' not in data.columns:
        LOG.warning("Predicted visit rate not available")
        data['fragility_score'] = 0
        data['fragility_tier'] = 'Unknown'
        return data

    data['performance_ratio'] = data['visits_per_1000'] / data['predicted_visit_rate'].clip(lower=0.1)

    if 'deprivation_tier' in data.columns:
        peer_groups = data.groupby('deprivation_tier')['performance_ratio'].transform('median')
        data['fragility_score'] = (data['performance_ratio'] / peer_groups.clip(lower=0.1) - 1) * 100
    else:
        median_ratio = data['performance_ratio'].median()
        data['fragility_score'] = (data['performance_ratio'] / median_ratio - 1) * 100

    conditions = [
        data['fragility_score'] > 50,
        data['fragility_score'] > 25,
        data['fragility_score'] > 10,
        data['fragility_score'] > -10,
        data['fragility_score'] > -25,
        True
    ]
    choices = ['Extreme Over-performance', 'Significant Over-performance',
               'Moderate Over-performance', 'Stable', 'Under-performing',
               'Severe Under-performance']
    data['fragility_tier'] = np.select(conditions, choices, default='Unknown')

    data['fragility_alert'] = data['fragility_score'] > 50

    LOG.info(f"High fragility districts: {data['fragility_alert'].sum()}")
    return data


def identify_quick_wins(data: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Identify top quick win districts for immediate action."""
    LOG.info(f"Identifying top {top_n} quick win districts")

    quick_wins_criteria = (
        (data['visits_per_1000'] < DeprivationConstants.LOW_VISIT_RATE_THRESHOLD) &
        (data.get('composite_need_score', 0) < DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD) &
        (data.get('performance_gap', 0) > 0)
    )

    if 'predicted_visit_rate' in data.columns:
        quick_wins = data[quick_wins_criteria].copy()

        if not quick_wins.empty:
            if 'fragility_score' in quick_wins.columns:
                quick_wins = quick_wins[quick_wins['fragility_score'] < 25]

            quick_wins['quick_win_score'] = (
                quick_wins.get('performance_gap', 0) * 0.5 +
                (100 - quick_wins.get('composite_need_score', 0)) * 0.3 +
                (quick_wins['visits_per_1000'].max() - quick_wins['visits_per_1000']) * 0.2
            )

            quick_wins = quick_wins.sort_values('quick_win_score', ascending=False).head(top_n)

            quick_wins['quick_win_rationale'] = quick_wins.apply(
                lambda row: f"Gap: {row.get('performance_gap', 0):.1f}, Need: {row.get('composite_need_score', 0):.1f}, Current: {row['visits_per_1000']:.1f}",
                axis=1
            )

            LOG.info(f"Identified {len(quick_wins)} quick win districts")
            return quick_wins[['District', 'Post_Town', 'Authority_Name', 'visits_per_1000',
                             'predicted_visit_rate', 'performance_gap', 'composite_need_score',
                             'quick_win_score', 'quick_win_rationale']]

    LOG.warning("Could not identify quick wins")
    return pd.DataFrame()


def calculate_early_warnings(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate early warning indicators for risk monitoring."""
    LOG.info("Calculating early warning indicators")

    data['risk_score'] = 0
    data['risk_flags'] = ''

    risk1_mask = (
        (data.get('composite_need_score', 0) >= DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD) &
        (data['visits_per_1000'] < DeprivationConstants.LOW_VISIT_RATE_THRESHOLD * 0.5)
    )
    data.loc[risk1_mask, 'risk_score'] += 30
    data.loc[risk1_mask, 'risk_flags'] += 'HighNeedLowVisits;'

    if 'performance_gap' in data.columns:
        risk2_mask = data['performance_gap'] < -2
        data.loc[risk2_mask, 'risk_score'] += 25
        data.loc[risk2_mask, 'risk_flags'] += 'UnderperformingPrediction;'

    if 'distance_to_BI' in data.columns:
        risk3_mask = data['distance_to_BI'] > 100
        data.loc[risk3_mask, 'risk_score'] += 20
        data.loc[risk3_mask, 'risk_flags'] += 'HighDistance;'

    if 'Population' in data.columns:
        pop_median = data['Population'].median()
        risk4_mask = data['Population'] < pop_median * 0.3
        data.loc[risk4_mask, 'risk_score'] += 15
        data.loc[risk4_mask, 'risk_flags'] += 'SmallPopulation;'

    if 'pop%_most_deprived' in data.columns:
        risk5_mask = data['pop%_most_deprived'] > 50
        data.loc[risk5_mask, 'risk_score'] += 10
        data.loc[risk5_mask, 'risk_flags'] += 'ExtremeDeprivation;'

    if 'fragility_score' in data.columns:
        risk6_mask = data['fragility_score'] > 50
        data.loc[risk6_mask, 'risk_score'] += 15
        data.loc[risk6_mask, 'risk_flags'] += 'FragileOverperformance;'

    conditions = [
        data['risk_score'] >= 50,
        data['risk_score'] >= 30,
        data['risk_score'] >= 15,
        True
    ]
    choices = ['High Risk', 'Medium Risk', 'Low Risk', 'Minimal Risk']
    data['risk_tier'] = np.select(conditions, choices, default='Unknown')

    data['risk_flags'] = data['risk_flags'].str.rstrip(';')

    LOG.info(f"High risk districts: {len(data[data['risk_tier'] == 'High Risk'])}")

    return data