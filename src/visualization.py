# -*- coding: utf-8 -*-
"""Visualization functions - properly fixed for Colab display."""

import logging
import json
import os
import subprocess
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import shap
import matplotlib.pyplot as plt

from src.constants import (
    ColorService, DeprivationConstants, DashboardConfig, VisualizationConstants,
    PriorityZones, InterventionTypes
)

LOG = logging.getLogger("Brownsea_Equity_Analysis")


# =============================================================================
# PROPER COLAB DISPLAY FIX
# =============================================================================

def _show_fig(fig):
    """Do not attempt rich inline Plotly display from CLI/subprocess runs."""
    return None


def configure_colab_plotly():
    """Configure Plotly for file-first rendering in Colab."""
    import plotly.io as pio
    try:
        pio.renderers.default = 'png'
        LOG.info("Plotly configured for file-first PNG rendering")
    except Exception as e:
        LOG.warning(f"Could not configure Plotly PNG renderer: {e}")


# Run configuration when module loads
try:
    configure_colab_plotly()
except Exception:
    pass


# =============================================================================
# VISUALIZATION FUNCTIONS (Modified to use _show_fig)
# =============================================================================

def prepare_visualization_data(ml_dataset: pd.DataFrame) -> pd.DataFrame:
    """Prepare data for visualizations."""
    try:
        required_cols = ['District', 'Visits', 'visits_per_1000', 'Population']
        available_cols = [col for col in required_cols if col in ml_dataset.columns]
        if not available_cols: 
            return None

        district_summary = ml_dataset[available_cols].drop_duplicates()
        if 'Post_Town' in ml_dataset.columns:
            district_summary = pd.merge(district_summary, ml_dataset[['District', 'Post_Town']].drop_duplicates(), on='District', how='left')
        if 'Authority_Name' in ml_dataset.columns:
            district_summary = pd.merge(district_summary, ml_dataset[['District', 'Authority_Name']].drop_duplicates(), on='District', how='left')
        return district_summary
    except Exception as e:
        LOG.error(f"Error preparing visualization data: {e}")
        return None


def create_priority_matrix_plot(data, show_plot=True):
    """Create priority matrix scatter plot."""
    plot_data = data.copy()
    fig = px.scatter(
        plot_data, x='composite_need_score', y='visits_per_1000', size='Population', color='priority_zone',
        color_discrete_map=ColorService.get_priority_matrix_colors(), hover_name='District',
        hover_data=['Authority_Name', 'impact_score', 'total_journey_min', 'pop%_most_deprived', 'avg_fsm%', 'composite_need_score'],
        title='BCP & Dorset Districts: Priority Action Matrix',
        labels={'composite_need_score': 'Composite Need Score', 'visits_per_1000': 'Visit Rate (per 1000)', 'priority_zone': 'Priority Zone'},
        height=600
    )
    fig.add_hline(y=DeprivationConstants.LOW_VISIT_RATE_THRESHOLD, line_dash="dash", line_color="red", annotation_text=f"Low Visit Threshold ({DeprivationConstants.LOW_VISIT_RATE_THRESHOLD})")
    fig.add_vline(x=DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD, line_dash="dash", line_color="red", annotation_text="High Need Threshold")

    max_composite_score = plot_data['composite_need_score'].max()
    max_visit_rate = plot_data['visits_per_1000'].max()
    fig.update_xaxes(range=[0, max(max_composite_score * 1.1, 40)])
    fig.update_yaxes(range=[0, max(max_visit_rate * 1.1, DeprivationConstants.MEDIUM_VISIT_RATE_THRESHOLD * 1.2)])

    if show_plot:
        _show_fig(fig)
    return fig


def create_intervention_treemap(data: pd.DataFrame, show_plot=True):
    """Create treemap of intervention strategies."""
    fig = px.treemap(
        data, path=['intervention_type', 'Authority_Name', 'District'], values='Population', color='intervention_type',
        color_discrete_map=ColorService.get_intervention_colors(), title='Intervention Strategy Map',
        hover_data={'visits_per_1000': ':.2f', 'visits_gap': ':.2f', 'total_journey_min': ':.1f', 'avg_fsm%': ':.1f', 'pop%_most_deprived': ':.1f', 'Population': ':,.0f'},
        height=600
    )
    fig.update_layout(margin=dict(t=60, l=25, r=25, b=25), font=dict(size=14))
    fig.update_traces(texttemplate="<b>%{label}</b>", textposition="middle center")
    if show_plot: 
        _show_fig(fig)
    return fig


def create_growth_opportunity_matrix(data: pd.DataFrame, show_plot=True):
    """Create 2x2 growth opportunity matrix."""
    required_cols = ['composite_need_score', 'growth_potential_score']
    for col in required_cols:
        if col not in data.columns: 
            return None

    def assign_quadrant(row):
        need_high = row['composite_need_score'] >= DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD
        growth_high = row['growth_potential_score'] >= 50
        if need_high and growth_high: return 'Strategic Investments'
        elif need_high and not growth_high: return 'Challenge Areas'
        elif not need_high and growth_high: return 'Quick Wins'
        else: return 'Maintenance'

    data = data.copy()
    data['growth_quadrant'] = data.apply(assign_quadrant, axis=1)

    fig = px.scatter(
        data, x='composite_need_score', y='growth_potential_score', color='growth_quadrant',
        color_discrete_map=ColorService.GROWTH_MATRIX_COLORS, size='Population', hover_name='District',
        hover_data={'Authority_Name': True, 'visits_per_1000': ':.2f', 'performance_gap': ':.2f', 'safe_zone_status': True, 'needs_intervention': True},
        title='Growth Opportunity Matrix', labels={'composite_need_score': 'Need Score', 'growth_potential_score': 'Growth Potential Score (0-100)', 'growth_quadrant': 'Opportunity Quadrant'},
        height=600
    )
    fig.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.7)
    fig.add_vline(x=DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD, line_dash="dash", line_color="gray", opacity=0.7)

    fig.add_annotation(x=DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD/2, y=75, text="Quick Wins", showarrow=False, font=dict(size=12, color="#45B7D1"))
    fig.add_annotation(x=DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD/2, y=25, text="Maintenance", showarrow=False, font=dict(size=12, color="#96CEB4"))
    fig.add_annotation(x=DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD*1.5, y=75, text="Strategic Investments", showarrow=False, font=dict(size=12, color="#FF6B6B"))
    fig.add_annotation(x=DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD*1.5, y=25, text="Challenge Areas", showarrow=False, font=dict(size=12, color="#4ECDC4"))

    fig.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    if show_plot: 
        _show_fig(fig)
    return fig


def create_safe_zone_visualization(data: pd.DataFrame, show_plot=True):
    """Visualize districts within/outside safe zone."""
    plot_data = data.sort_values('predicted_visit_rate').reset_index(drop=True)
    plot_data['index'] = range(len(plot_data))

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=plot_data['index'].tolist() + plot_data['index'].tolist()[::-1], 
                            y=plot_data['safe_zone_upper_2rmse'].tolist() + plot_data['safe_zone_lower_2rmse'].tolist()[::-1], 
                            fill='toself', fillcolor='rgba(0,100,80,0.2)', line=dict(color='rgba(255,255,255,0)'), 
                            hoverinfo='skip', showlegend=True, name='Safe Zone'))
    fig.add_trace(go.Scatter(x=plot_data['index'], y=plot_data['predicted_visit_rate'], 
                            mode='lines', name='Predicted', line=dict(color='green', width=2)))

    colors = ['red' if x < y else 'orange' if x < z else 'lightgreen' 
              for x, y, z in zip(plot_data['visits_per_1000'], plot_data['safe_zone_lower_2rmse'], plot_data['predicted_visit_rate'])]
    fig.add_trace(go.Scatter(x=plot_data['index'], y=plot_data['visits_per_1000'], 
                            mode='markers+text', name='Actual', marker=dict(color=colors, size=8), 
                            text=plot_data['District'], textposition='top center', textfont=dict(size=9)))

    fig.update_layout(title='District Performance Relative to Safe Zone', 
                     xaxis_title='Districts (Sorted by Prediction)', yaxis_title='Visits per 1000', 
                     height=600, showlegend=True)
    if show_plot: 
        _show_fig(fig)
    return fig


def create_equity_gap_visualization(data: pd.DataFrame, show_plot=True):
    """Create equity gap bar chart by deprivation tier."""
    from src.constants import TierLabels
    
    equity_data = data.groupby('deprivation_tier').agg({'visits_per_1000': 'mean', 'Population': 'sum', 'District': 'count'}).reset_index()
    deprivation_colors = {TierLabels.HIGH_DEPRIVATION: '#d73027', TierLabels.MODERATE_DEPRIVATION: '#fee08b', TierLabels.LOW_DEPRIVATION: '#1a9850'}

    fig = px.bar(equity_data, x='deprivation_tier', y='visits_per_1000', 
                title='Equity Gap: Visit Rates by Deprivation Level', 
                labels={'visits_per_1000': 'Average Visits per 1000 People', 'deprivation_tier': 'Deprivation Tier'},
                color='deprivation_tier', color_discrete_map=deprivation_colors, height=500)
    fig.update_traces(marker_line_width=0)
    fig.update_layout(showlegend=False)
    if show_plot: 
        _show_fig(fig)
    return fig


def create_choropleth_map(district_summary: pd.DataFrame, combined_geojson: dict, 
                          color_column: str, title: str, color_scale: str, show_plot=True):
    """Create choropleth map for geographic visualization."""
    if combined_geojson is None or 'features' not in combined_geojson: 
        return None
    if color_column not in district_summary.columns: 
        return None

    def get_centroid(feature):
        if feature['geometry']['type'] == 'Polygon': 
            coords = feature['geometry']['coordinates'][0]
        elif feature['geometry']['type'] == 'MultiPolygon': 
            coords = feature['geometry']['coordinates'][0][0]
        else: 
            return None
        lats = [coord[1] for coord in coords]
        lons = [coord[0] for coord in coords]
        return {'lat': sum(lats) / len(lats), 'lon': sum(lons) / len(lons)}

    district_centroids = {}
    for feature in combined_geojson['features']:
        district_name = feature['properties'].get('name')
        if district_name:
            centroid = get_centroid(feature)
            if centroid: 
                district_centroids[district_name] = centroid

    district_summary = district_summary.copy()
    district_summary['centroid_lat'] = district_summary['District'].map(lambda x: district_centroids.get(x, {}).get('lat', 0))
    district_summary['centroid_lon'] = district_summary['District'].map(lambda x: district_centroids.get(x, {}).get('lon', 0))

    is_categorical = district_summary[color_column].dtype == 'object'

    if is_categorical:
        unique_categories = district_summary[color_column].unique()
        traffic_light_colors = px.colors.qualitative.Set1
        color_discrete_map = {cat: traffic_light_colors[i % len(traffic_light_colors)] for i, cat in enumerate(unique_categories)}
        fig = px.choropleth_mapbox(district_summary, geojson=combined_geojson, locations='District', 
                                   featureidkey="properties.name", color=color_column, 
                                   color_discrete_map=color_discrete_map, mapbox_style="carto-positron", 
                                   zoom=VisualizationConstants.MAP_ZOOM, 
                                   center={"lat": VisualizationConstants.MAP_CENTER_LAT, "lon": VisualizationConstants.MAP_CENTER_LON}, 
                                   opacity=VisualizationConstants.MAP_OPACITY, title=f'<b>{title}</b>', 
                                   labels={color_column: title.split(' of ')[-1]}, 
                                   hover_data=['Population', 'visits_per_1000'], height=600)
    else:
        fig = px.choropleth_mapbox(district_summary, geojson=combined_geojson, locations='District', 
                                   featureidkey="properties.name", color=color_column, 
                                   color_continuous_scale=color_scale, 
                                   range_color=(0, district_summary[color_column].quantile(0.95)), 
                                   mapbox_style="carto-positron", zoom=VisualizationConstants.MAP_ZOOM, 
                                   center={"lat": VisualizationConstants.MAP_CENTER_LAT, "lon": VisualizationConstants.MAP_CENTER_LON}, 
                                   opacity=VisualizationConstants.MAP_OPACITY, title=f'<b>{title}</b>', 
                                   labels={color_column: title.split(' of ')[-1]}, 
                                   hover_data=['Population', 'visits_per_1000'], height=600)

    for i, row in district_summary.iterrows():
        if not pd.isna(row['centroid_lat']) and row['centroid_lat'] != 0:
            fig.add_trace(go.Scattermapbox(lat=[row['centroid_lat']], lon=[row['centroid_lon']], 
                                          mode='text', text=[row['District']], 
                                          textfont=dict(size=12, color='black', weight='bold'), 
                                          showlegend=False, hoverinfo='skip'))

    fig.update_layout(margin={"r": 0, "t": 60, "l": 0, "b": 0})
    if not is_categorical: 
        fig.update_layout(coloraxis_colorbar=dict(title=title.split(' of ')[-1]))
    if show_plot: 
        _show_fig(fig)
    return fig


def load_geojson_data(viz_config: dict) -> dict:
    """Load GeoJSON data for mapping."""
    if not os.path.exists(viz_config['geojson_local_path']):
        LOG.info("Cloning GeoJSON repository...")
        subprocess.run(['git', 'clone', viz_config['geojson_repo_url'], viz_config['geojson_local_path']])

    combined_geojson = {"type": "FeatureCollection", "features": []}
    geojson_base_path = os.path.join(viz_config['geojson_local_path'], 'geojson')

    for area in viz_config['dorset_postcode_areas']:
        geojson_path = os.path.join(geojson_base_path, f"{area}.geojson")
        try:
            with open(geojson_path) as f:
                data = json.load(f)
                combined_geojson['features'].extend(data['features'])
        except FileNotFoundError:
            LOG.warning(f"GeoJSON file not found for {area}")

    return combined_geojson


def create_shap_summary_plot(model_info, X: pd.DataFrame, population: pd.Series, save_path: str = 'shap_summary.png', show_plot: bool = True):
    """Create SHAP summary plot for feature importance on a 1x2 grid."""
    from src.model_training import get_explanation_model
    
    try:
        explain_model = get_explanation_model(model_info)
        if explain_model is None or 'pipeline' not in explain_model:
            LOG.error("No valid model for SHAP")
            return

        pipeline = explain_model['pipeline']
        model = pipeline.named_steps['model']
        scaler = pipeline.named_steps['scaler']
        X_scaled = scaler.transform(X)

        model_class = type(model).__name__
        tree_models = ['RandomForestRegressor', 'GradientBoostingRegressor', 'LGBMRegressor', 'XGBRegressor', 'CatBoostRegressor']

        if any(t in model_class for t in tree_models):
            explainer = shap.TreeExplainer(model)
        else:
            explainer = shap.LinearExplainer(model, X_scaled)

        X_sample = X_scaled[:min(500, len(X_scaled))]
        shap_values = explainer.shap_values(X_sample)

        if isinstance(shap_values, list):
            shap_values = shap_values[0]
        if len(shap_values.shape) == 3:
            shap_values = shap_values[0]

        feature_names = X.columns.tolist()

        fig, axes = plt.subplots(1, 2, figsize=(24, 8))
        plt.sca(axes[0])
        shap.summary_plot(shap_values, X_sample, feature_names=feature_names, show=False, plot_size=None)
        axes[0].set_title('SHAP Feature Importance Summary', fontsize=14, pad=20)

        plt.sca(axes[1])
        shap.summary_plot(shap_values, X_sample, feature_names=feature_names, plot_type="bar", show=False, plot_size=None)
        axes[1].set_title('Mean |SHAP| Feature Importance', fontsize=14, pad=20)

        plt.subplots_adjust(wspace=1.0, bottom=0.15)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(save_path) if os.path.dirname(save_path) else '.', exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        if show_plot:
            plt.show()
        else:
            plt.close(fig)
        LOG.info(f"SHAP plots saved as 1x2 grid at {save_path}")

    except Exception as e:
        LOG.error(f"SHAP plot failed: {e}")
