# -*- coding: utf-8 -*-
"""Shared constants and configuration classes."""

import os
import numpy as np
import pandas as pd

# =============================================================================
# CONSTANT DEFINITIONS
# =============================================================================

class DeprivationConstants:
    HIGH_DEPRIVATION_POPULATION = 35
    LOW_DEPRIVATION_POPULATION = 35
    MODERATE_DEPRIVATION_THRESHOLD = 40

    HIGH_FSM_THRESHOLD = 25
    MEDIUM_FSM_THRESHOLD = 15

    MOST_DEPRIVED_DECILES = [1, 2, 3, 4]
    MODERATELY_DEPRIVED_DECILES = [5, 6, 7]
    LEAST_DEPRIVED_DECILES = [8, 9, 10]

    LOW_VISIT_RATE_THRESHOLD = 4
    MEDIUM_VISIT_RATE_THRESHOLD = 7
    HIGH_VISIT_RATE_THRESHOLD = 7
    TARGET_VISIT_RATE = 10

    FSM_WEIGHT = 0.3
    DEPRIVATION_WEIGHT = 0.7

    HIGH_NEED_SCORE_THRESHOLD = 26
    MEDIUM_NEED_SCORE_THRESHOLD = 16
    LOW_NEED_SCORE_THRESHOLD = 16

    HIGH_DEPRIVATION_PERCENTAGE = 35
    MEDIUM_DEPRIVATION_PERCENTAGE = 15

class InterventionConstants:
    STRATEGIC_DEPRIVATION_THRESHOLD = 40
    STRATEGIC_FSM_THRESHOLD = 15
    URGENT_ACTION_VISIT_RATE = DeprivationConstants.LOW_VISIT_RATE_THRESHOLD

class GeographicConstants:
    BCP_DORSET_POSTCODES = ['BH', 'DT', 'SP']
    DORSET_POSTCODE_AREAS = ['BH', 'DT', 'SP']
    BI_LATITUDE = 50.68900
    BI_LONGITUDE = -1.95732

class CompetitorConstants:
    """Major National Trust properties competing for the same local demographic."""
    NT_SITES = {
        'kingston_lacy': {
            'name': 'Kingston Lacy',
            'lat': 50.8153,
            'lon': -2.0305
        },
        'corfe_castle': {
            'name': 'Corfe Castle',
            'lat': 50.6395,
            'lon': -2.0566
        },
        'studland_bay': {
            'name': 'Studland Bay',
            'lat': 50.6425,
            'lon': -1.9430
        }
    }

class TierLabels:
    HIGH_DEPRIVATION = 'High Deprivation'
    LOW_DEPRIVATION = 'Low Deprivation'
    MODERATE_DEPRIVATION = 'Moderate Deprivation'
    MIXED_PROFILE = 'Mixed Deprivation'
    HIGH_FSM = 'High FSM'
    MEDIUM_FSM = 'Medium FSM'
    LOW_FSM = 'Low FSM'
    HIGH_VISIT_RATE = 'High Visit Rate'
    MEDIUM_VISIT_RATE = 'Medium Visit Rate'
    LOW_VISIT_RATE = 'Low Visit Rate'
    MOST_DEPRIVED = 'most_deprived'
    MODERATELY_DEPRIVED = 'moderately_deprived'
    LEAST_DEPRIVED = 'least_deprived'
    HIGH_NEED = 'High Need'
    MEDIUM_NEED = 'Medium Need'
    LOW_NEED = 'Low Need'

class InterventionTypes:
    CRISIS_INTERVENTION = 'Crisis Intervention'
    TARGETED_SUPPORT = 'Targeted Support'
    GROWTH_AWARENESS = 'Growth & Awareness'
    MODEL_DISTRICT = 'Model District'
    SUSTAIN_OPTIMIZE = 'Sustain & Optimize'

class PriorityZones:
    URGENT_ACTION = 'Urgent Action'
    HIGH_PRIORITY = 'High Priority'
    MONITOR = 'Monitor'
    GROWTH_OPPORTUNITY = 'Growth Opportunity'
    MAINTAIN = 'Maintain'

class FerryConstants:
    FERRY_TERMINALS = {
        'poole_quay': {'lat': 50.7119, 'lon': -1.9884, 'name': 'Poole Quay', 'crossing_time_minutes': 15, 'crossing_distance_km': 2.5},
        'sandbanks': {'lat': 50.6769, 'lon': -1.9477, 'name': 'Sandbanks Jetty', 'crossing_time_minutes': 5, 'crossing_distance_km': 1.2}
    }
    # Vehicle route between Shell Bay/Studland and Sandbanks. This is separate
    # from the passenger ferry from Sandbanks Jetty to Brownsea Island.
    SANDBANKS_CHAIN_FERRY = {
        'enabled': True,
        'south_landing': {'lat': 50.6750, 'lon': -1.9460, 'name': 'Shell Bay chain ferry landing'},
        'north_landing': {'lat': 50.6769, 'lon': -1.9477, 'name': 'Sandbanks chain ferry landing'},
        # Fixed allowance used for planning-level access estimates. It represents
        # the local chain-ferry transfer and avoids treating the harbour mouth as
        # a direct road.
        'allowance_minutes': 10,
        'terminal_transfer_minutes': 1,
        'purbeck_district_prefixes': ['BH19', 'BH20'],
    }
    BI_JETTY = {'lat': 50.68900, 'lon': -1.95732}
    CAR_SPEED_KMPH = 50
    ROAD_DISTANCE_FACTOR = 1.3
    MAX_ACCEPTABLE_TIME = 120

class ModelConstants:
    TEST_SIZE = 0.2
    RANDOM_STATE = 42
    N_SPLITS_CV = 5
    N_ITER_TUNING = 50
    PREDICTION_INTERVAL_ALPHA = 0.1
    SAFE_ZONE_BUFFER = 1.5
    PERFORMANCE_TOLERANCE = 0.1

class FeatureConstants:
    """Plain-language labels for model features in dashboards and narratives."""
    DESCRIPTIONS = {
        'total_journey_min': 'Brownsea journey time',
        'nearest_competitor_drive_min': 'drive time to nearest NT site',
        'accessibility_score': 'Brownsea accessibility score',
        'driving_time_min': 'journey to departure terminal',
        'ferry_crossing_min': 'Brownsea ferry crossing time',
        'avg_fsm%': 'FSM and poverty context',
        'pop%_most_deprived': 'high deprivation',
        'pop%_moderately_deprived': 'moderate deprivation',
        'pop%_least_deprived': 'lower deprivation',
        'imd_decile_mean': 'deprivation level',
        'income_decile': 'local income context',
        'geo_barriers_decile': 'geographic access barriers',
        'wider_barriers_decile': 'wider access barriers'
    }

class VisualizationConstants:
    GEOJSON_REPO_URL = "https://github.com/missinglink/uk-postcode-polygons.git"
    GEOJSON_LOCAL_PATH = "/content/uk-postcode-polygons"
    MAP_CENTER_LAT = 50.75
    MAP_CENTER_LON = -2.2
    MAP_ZOOM = 8
    MAP_OPACITY = 0.5

class RoutingConstants:
    ORS_API_KEY = os.environ.get('ORS_API_KEY', '').strip()
    PROFILE = 'driving-car'
    MAX_REQUESTS_PER_DAY = 2000
    REQUEST_DELAY = 1
    COMPETITOR_SHORTLIST_SIZE = 5
    CACHE_DIR = 'route_cache'
    BROWNSEA_CACHE_FILE = 'route_cache/brownsea_routes.json'
    BROWNSEA_CACHE_META_FILE = 'route_cache/brownsea_routes.metadata.json'
    COMPETITOR_CACHE_FILE = 'route_cache/competitor_routes.json'
    COMPETITOR_CACHE_META_FILE = 'route_cache/competitor_routes.metadata.json'
    CACHE_VERSION = 'v1'

class DashboardConfig:
    PRIMARY_COLOR = "#2c3e50"
    TEXT_COLOR_WHITE = "white"
    HEADER_FONT_SIZE = "14px"
    CELL_FONT_SIZE = "13px"

    @classmethod
    def get_table_styles(cls):
        return [
            dict(selector="th", props=[
                ("text-align", "left"),
                ("background-color", cls.PRIMARY_COLOR),
                ("color", cls.TEXT_COLOR_WHITE),
                ("font-size", cls.HEADER_FONT_SIZE)
            ]),
            dict(selector="td", props=[
                ("text-align", "left"),
                ("white-space", "pre-wrap"),
                ("font-size", cls.CELL_FONT_SIZE)
            ])
        ]

    @classmethod
    def get_table_properties(cls):
        return {'white-space': 'pre-wrap'}

class ColorService:
    UNIFIED_COLORS = {
        PriorityZones.URGENT_ACTION: '#8B0000',
        PriorityZones.HIGH_PRIORITY: '#FF0000',
        PriorityZones.MONITOR: '#FFA500',
        PriorityZones.GROWTH_OPPORTUNITY: '#2196F3',
        PriorityZones.MAINTAIN: '#4CAF50',
        InterventionTypes.CRISIS_INTERVENTION: '#8B0000',
        InterventionTypes.TARGETED_SUPPORT: '#FF0000',
        InterventionTypes.GROWTH_AWARENESS: '#2196F3',
        InterventionTypes.MODEL_DISTRICT: '#4CAF50',
        InterventionTypes.SUSTAIN_OPTIMIZE: '#FFA500'
    }

    GROWTH_MATRIX_COLORS = {
        'Strategic Investments': '#FF6B6B',
        'Challenge Areas': '#4ECDC4',
        'Quick Wins': '#45B7D1',
        'Maintenance': '#96CEB4'
    }

    @classmethod
    def get_priority_matrix_colors(cls):
        return {k: v for k, v in cls.UNIFIED_COLORS.items() if 'Priority' in k or 'Action' in k or 'Monitor' in k or 'Maintain' in k or 'Growth' in k}

    @classmethod
    def get_intervention_colors(cls):
        return {k: v for k, v in cls.UNIFIED_COLORS.items() if k in [InterventionTypes.CRISIS_INTERVENTION, InterventionTypes.TARGETED_SUPPORT, InterventionTypes.GROWTH_AWARENESS, InterventionTypes.MODEL_DISTRICT, InterventionTypes.SUSTAIN_OPTIMIZE]}


# Static Definitions DataFrames
INTERVENTION_CATEGORIES = pd.DataFrame([
    {"Intervention Type": InterventionTypes.CRISIS_INTERVENTION, "Priority Level": "Highest", "Key Criteria": f"High Need (Score ≥{DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD}) + Low Visits (<{DeprivationConstants.LOW_VISIT_RATE_THRESHOLD})"},
    {"Intervention Type": InterventionTypes.TARGETED_SUPPORT, "Priority Level": "High", "Key Criteria": f"Medium Need (Score {DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD}-{DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD-0.1}) + Low Visits (<{DeprivationConstants.LOW_VISIT_RATE_THRESHOLD})"},
    {"Intervention Type": InterventionTypes.GROWTH_AWARENESS, "Priority Level": "Medium", "Key Criteria": f"Low Need (Score <{DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD}) + Low Visits (<{DeprivationConstants.LOW_VISIT_RATE_THRESHOLD})"},
    {"Intervention Type": InterventionTypes.MODEL_DISTRICT, "Priority Level": "Benchmark", "Key Criteria": f"High Performance (Visits ≥{DeprivationConstants.MEDIUM_VISIT_RATE_THRESHOLD}) + Any Need Level"},
    {"Intervention Type": InterventionTypes.SUSTAIN_OPTIMIZE, "Priority Level": "Ongoing", "Key Criteria": f"Medium Performance (Visits {DeprivationConstants.LOW_VISIT_RATE_THRESHOLD}-{DeprivationConstants.MEDIUM_VISIT_RATE_THRESHOLD-0.1}) + Any Need Level"}
]).set_index("Intervention Type")

NEED_TIER_DEFINITIONS = pd.DataFrame([
    {"Need Tier": TierLabels.HIGH_NEED, "Definition": f"Composite Need Score ≥ {DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD}", "Calculation": f"(FSM% × {DeprivationConstants.FSM_WEIGHT}) + (Deprivation% × {DeprivationConstants.DEPRIVATION_WEIGHT}) ≥ {DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD}", "Purpose": "Identifies areas with severe socioeconomic challenges using balanced metrics"},
    {"Need Tier": TierLabels.MEDIUM_NEED, "Definition": f"Composite Need Score {DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD}-{DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD-0.1}", "Calculation": f"(FSM% × {DeprivationConstants.FSM_WEIGHT}) + (Deprivation% × {DeprivationConstants.DEPRIVATION_WEIGHT}) between {DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD} and {DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD-0.1}", "Purpose": "Identifies areas with moderate socioeconomic challenges"},
    {"Need Tier": TierLabels.LOW_NEED, "Definition": f"Composite Need Score < {DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD}", "Calculation": f"(FSM% × {DeprivationConstants.FSM_WEIGHT}) + (Deprivation% × {DeprivationConstants.DEPRIVATION_WEIGHT}) < {DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD}", "Purpose": "Identifies relatively affluent areas with lower challenges"}
]).set_index("Need Tier")

PRIORITY_MATRIX_CATEGORIES = pd.DataFrame([
    {"Priority Zone": PriorityZones.URGENT_ACTION, "Need Criteria": f"High Need (Score ≥ {DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD})", "Visit Rate Range": f"< {DeprivationConstants.LOW_VISIT_RATE_THRESHOLD} visits/1000", "Description": "High-need areas with critically low engagement", "Strategic Focus": "Immediate equity interventions and crisis response"},
    {"Priority Zone": PriorityZones.HIGH_PRIORITY, "Need Criteria": f"Medium Need (Score ≥ {DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD})", "Visit Rate Range": f"< {DeprivationConstants.LOW_VISIT_RATE_THRESHOLD} visits/1000", "Description": "Medium need areas with low engagement", "Strategic Focus": "Targeted outreach and retention programs"},
    {"Priority Zone": PriorityZones.MONITOR, "Need Criteria": f"Medium Need (Score {DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD}-{DeprivationConstants.HIGH_NEED_SCORE_THRESHOLD-0.1})", "Visit Rate Range": f"{DeprivationConstants.LOW_VISIT_RATE_THRESHOLD}-{DeprivationConstants.MEDIUM_VISIT_RATE_THRESHOLD-0.1} visits/1000", "Description": "Medium-need areas with moderate engagement", "Strategic Focus": "Monitor performance and emerging needs"},
    {"Priority Zone": PriorityZones.GROWTH_OPPORTUNITY, "Need Criteria": f"Low Need (Score < {DeprivationConstants.MEDIUM_NEED_SCORE_THRESHOLD})", "Visit Rate Range": f"< {DeprivationConstants.LOW_VISIT_RATE_THRESHOLD} visits/1000", "Description": "Lower-need areas with growth potential", "Strategic Focus": "Expansion and awareness campaigns"},
    {"Priority Zone": PriorityZones.MAINTAIN, "Need Criteria": "Any Need Level", "Visit Rate Range": f"≥ {DeprivationConstants.MEDIUM_VISIT_RATE_THRESHOLD} visits/1000", "Description": "Areas with good engagement meeting expectations", "Strategic Focus": "Sustain current performance and optimize"}
]).set_index("Priority Zone")


# =============================================================================
# CONFIG PROXY
# =============================================================================

class _ConfigProxy:
    """Proxy that lazily loads config from src.config to avoid circular imports."""
    
    def __getitem__(self, key):
        from src.config import get_config
        return get_config()[key]
    
    def get(self, key, default=None):
        from src.config import get_config
        return get_config().get(key, default)
    
    def __contains__(self, key):
        from src.config import get_config
        return key in get_config()
    
    def keys(self):
        from src.config import get_config
        return get_config().keys()
    
    def values(self):
        from src.config import get_config
        return get_config().values()
    
    def items(self):
        from src.config import get_config
        return get_config().items()
    
    def __iter__(self):
        from src.config import get_config
        return iter(get_config())


# CONFIG provides lazy access to runtime configuration values.
CONFIG = _ConfigProxy()