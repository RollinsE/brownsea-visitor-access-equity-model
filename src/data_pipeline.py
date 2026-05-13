# -*- coding: utf-8 -*-
"""Data loading, cleaning, and feature engineering pipeline."""

import logging
import pandas as pd
import numpy as np
import os
import time
from typing import Dict, Tuple, Optional
from tqdm import tqdm

from src.constants import (
    GeographicConstants, FerryConstants, TierLabels, DeprivationConstants,
    RoutingConstants
)
from src.utils import (
    get_outward_code, get_deprivation_category, setup_routing_cache,
    save_routing_cache, create_checkpoint, load_checkpoint
)
from src.routing_service import calculate_ors_ferry_metrics

LOG = logging.getLogger("Brownsea_Equity_Analysis")

# =============================================================================
# DATA LOADING AND PROCESSING FUNCTIONS
# =============================================================================

def load_school_data(school_info_path: str, school_census_path: str) -> pd.DataFrame:
    """Load and merge school information with census data."""
    school_info = pd.read_csv(school_info_path, usecols=['URN', 'POSTCODE'])
    school_census = pd.read_csv(school_census_path, usecols=['URN', 'PNUMFSMEVER', 'NOR'])
    school_info['URN'] = school_info['URN'].astype(str)
    school_census['URN'] = school_census['URN'].astype(str)
    school_census['PNUMFSMEVER'] = pd.to_numeric(
        school_census['PNUMFSMEVER'].astype(str).str.replace('%', ''), errors='coerce'
    )
    school_census['NOR'] = pd.to_numeric(school_census['NOR'], errors='coerce')
    school_info['POSTCODE_CLEAN'] = school_info['POSTCODE'].str.replace(r'\s+', '', regex=True)
    return pd.merge(
        school_info.dropna(subset=['POSTCODE_CLEAN']),
        school_census[['URN', 'PNUMFSMEVER', 'NOR']],
        on='URN'
    )


def calculate_lsoa_fsm_rates(school_df: pd.DataFrame, postcode_to_lsoa_map: pd.DataFrame) -> pd.DataFrame:
    """Calculate weighted FSM averages at LSOA level using school size."""
    LOG.info("Calculating weighted FSM averages by LSOA")

    school_with_lsoa = pd.merge(school_df, postcode_to_lsoa_map, on='POSTCODE_CLEAN', how='left')
    school_with_lsoa = school_with_lsoa.dropna(subset=['lsoa21cd'])
    school_with_lsoa['fsm_count'] = (school_with_lsoa['PNUMFSMEVER'] / 100) * school_with_lsoa['NOR']

    lsoa_fsm_agg = school_with_lsoa.groupby('lsoa21cd').agg({
        'fsm_count': 'sum',
        'NOR': 'sum'
    }).reset_index()

    lsoa_fsm_agg['avg_fsm%'] = (lsoa_fsm_agg['fsm_count'] / lsoa_fsm_agg['NOR']) * 100
    lsoa_fsm_agg['avg_fsm%'] = lsoa_fsm_agg['avg_fsm%'].fillna(0)

    LOG.info(f"Calculated FSM for {len(lsoa_fsm_agg)} LSOAs")
    return lsoa_fsm_agg[['lsoa21cd', 'avg_fsm%']]


def load_and_clean_ons_data(ons_path: str) -> pd.DataFrame:
    """Load and clean ONS Postcode Directory data."""
    ons_cols = ['pcds', 'lsoa21cd', 'lad25cd', 'lat', 'long']
    ons_df = pd.read_csv(ons_path, dtype=str, usecols=ons_cols)
    ons_df.rename(columns={'pcds': 'POSTCODE', 'lsoa21cd': 'lsoa21cd'}, inplace=True)
    ons_df.dropna(subset=['POSTCODE', 'lsoa21cd', 'lad25cd'], inplace=True)
    ons_df['District'] = ons_df['POSTCODE'].apply(get_outward_code)
    ons_df['lat'] = pd.to_numeric(ons_df['lat'], errors='coerce')
    ons_df['long'] = pd.to_numeric(ons_df['long'], errors='coerce')
    ons_df['POSTCODE_CLEAN'] = ons_df['POSTCODE'].str.replace(r'\s+', '', regex=True)
    return ons_df


def load_data(file_paths: dict) -> dict:
    """Load all raw data files."""
    LOG.info("Loading all raw data files")
    dataframes = {}
    current_file = None

    try:
        current_file = 'imd_decile'
        dataframes['imd_decile'] = pd.read_csv(file_paths['imd_decile'])
        LOG.info("Loaded IMD decile and population data")

        current_file = 'lad_names'
        dataframes['lad_names'] = pd.read_csv(file_paths['lad_names'])

        current_file = 'lad_regions'
        try:
            dataframes['lad_regions'] = pd.read_csv(file_paths['lad_regions'])
        except Exception:
            dataframes['lad_regions'] = pd.read_excel(file_paths['lad_regions'])

        current_file = 'membership'
        dataframes['membership'] = pd.read_csv(file_paths['membership'])

        current_file = 'ons_data'
        dataframes['ons_df'] = load_and_clean_ons_data(file_paths['ons_data'])

        current_file = 'school_data'
        school_df = load_school_data(file_paths['school_info'], file_paths['school_census'])
        postcode_to_lsoa_map = dataframes['ons_df'][['POSTCODE_CLEAN', 'lsoa21cd']].drop_duplicates()
        dataframes['lsoa_fsm'] = calculate_lsoa_fsm_rates(school_df, postcode_to_lsoa_map)
        dataframes['school_data'] = school_df

        LOG.info("All source data loaded successfully")
        return dataframes

    except FileNotFoundError as e:
        missing_file = e.filename if hasattr(e, 'filename') else file_paths.get(current_file, 'Unknown File')
        LOG.error(f"Missing data file: {missing_file}")
        raise e
    except Exception as e:
        LOG.error(f"Data loading failed at '{current_file}': {e}")
        raise e


def clean_imd_data(imd_decile: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare IMD data."""
    imd_cols_to_keep = {
        'LSOA code (2021)': 'lsoa21cd',
        'Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)': 'imd_decile',
        'Income Score (rate)': 'income_score',
        'Employment Score (rate)': 'employment_score',
        'Total population: mid 2022': 'Population'
    }

    missing_imd_cols = [col for col in imd_cols_to_keep if col not in imd_decile.columns]
    if missing_imd_cols:
        LOG.warning(f"IMD file missing columns: {missing_imd_cols}")
        imd_cols_to_keep = {k: v for k, v in imd_cols_to_keep.items() if k in imd_decile.columns}

    imd_decile_clean = imd_decile[list(imd_cols_to_keep.keys())].rename(columns=imd_cols_to_keep)
    imd_decile_clean['lsoa21cd'] = imd_decile_clean['lsoa21cd'].astype(str).str.strip()
    imd_decile_clean['deprivation_category'] = imd_decile_clean['imd_decile'].apply(get_deprivation_category)

    return imd_decile_clean


def clean_membership_data(membership_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare membership data."""
    membership_df.columns = membership_df.columns.str.strip()
    membership_df.rename(columns={'Primary Supporter Postal District': 'District'}, inplace=True)
    membership_df['District'] = membership_df['District'].astype(str).str.strip().str.upper()

    if 'Visits' not in membership_df.columns:
        LOG.error("Membership file missing 'Visits' column")
        raise KeyError("Missing 'Visits' column in membership file")

    membership_clean = membership_df[['District', 'Visits']].dropna(subset=['District'])
    LOG.info(f"Cleaned membership data: {len(membership_clean)} records")
    return membership_clean


def clean_lad_data(lad_names: pd.DataFrame, lad_regions: pd.DataFrame) -> tuple:
    """Clean and prepare Local Authority District data."""
    lad_names.columns = lad_names.columns.str.lower()
    lad_names.rename(columns={'lad24cd': 'lad25cd', 'lad24nm': 'Authority_Name'}, inplace=True)
    lad_cols_to_merge = ['lad25cd', 'Authority_Name']

    lad_regions.columns = lad_regions.columns.str.lower()
    rename_cols_region = {'lad25cd': 'lad25cd_merge', 'rgn25nm': 'Region_Name'}

    if 'lad25cd' in lad_regions.columns and 'rgn25nm' in lad_regions.columns:
        lad_regions.rename(columns=rename_cols_region, inplace=True)
        region_cols_to_merge = ['lad25cd_merge', 'Region_Name']
    else:
        LOG.warning("Region data not available")
        region_cols_to_merge = []

    return lad_cols_to_merge, region_cols_to_merge


def build_lsoa_master_data(ons_df: pd.DataFrame, imd_decile_clean: pd.DataFrame,
                          lsoa_fsm: pd.DataFrame, lad_names: pd.DataFrame,
                          lad_regions: pd.DataFrame) -> pd.DataFrame:
    """Build master LSOA-level dataset with all attributes."""
    lsoa_df = ons_df.drop_duplicates(subset=['lsoa21cd']).drop(
        columns=['POSTCODE', 'POSTCODE_CLEAN', 'District']
    )

    lsoa_data = pd.merge(lsoa_df, imd_decile_clean, on='lsoa21cd', how='left')
    lsoa_data = pd.merge(lsoa_data, lsoa_fsm, on='lsoa21cd', how='left')

    lad_cols_to_merge, region_cols_to_merge = clean_lad_data(lad_names, lad_regions)
    lsoa_data = pd.merge(lsoa_data, lad_names[lad_cols_to_merge], on='lad25cd', how='left')

    if region_cols_to_merge:
        lsoa_data = pd.merge(lsoa_data, lad_regions[region_cols_to_merge],
                           left_on='lad25cd', right_on='lad25cd_merge', how='left')

    lsoa_data['Region_Name'] = lsoa_data.get('Region_Name', 'Unknown')
    lsoa_data['Authority_Name'] = lsoa_data.get('Authority_Name', 'Unknown')

    LOG.info(f"LSOA master created: {len(lsoa_data)} rows")
    return lsoa_data


def clean_and_merge(data: dict) -> tuple:
    """Clean and merge all data sources into LSOA master."""
    LOG.info("Building LSOA-level master data")

    ons_df = data['ons_df'].copy()
    district_lsoa_map = ons_df[['District', 'lsoa21cd']].drop_duplicates().dropna()
    LOG.info(f"District-LSOA map: {len(district_lsoa_map)} links")

    imd_decile_clean = clean_imd_data(data['imd_decile'])
    membership_clean = clean_membership_data(data['membership'])

    lsoa_data = build_lsoa_master_data(
        ons_df, imd_decile_clean, data['lsoa_fsm'],
        data['lad_names'], data['lad_regions']
    )

    return lsoa_data, district_lsoa_map, membership_clean, ons_df


def calculate_deprivation_percentages(district_lsoa_data: pd.DataFrame, district_features: pd.DataFrame) -> pd.DataFrame:
    """Calculate population-weighted deprivation percentages by district."""
    from src.constants import TierLabels
    
    if 'Population' not in district_lsoa_data.columns or district_lsoa_data['Population'].sum() <= 0:
        LOG.warning("Cannot calculate deprivation percentages - population data missing")
        district_features['pop%_most_deprived'] = 0
        district_features['pop%_moderately_deprived'] = 0
        district_features['pop%_least_deprived'] = 0
        return district_features

    deprivation_population = district_lsoa_data.groupby(
        ['District', 'deprivation_category']
    )['Population'].sum().unstack(fill_value=0)

    total_population = deprivation_population.sum(axis=1)

    if TierLabels.MOST_DEPRIVED in deprivation_population.columns:
        district_features['pop%_most_deprived'] = (
            deprivation_population[TierLabels.MOST_DEPRIVED] / total_population
        ) * 100
    else:
        district_features['pop%_most_deprived'] = 0

    if TierLabels.MODERATELY_DEPRIVED in deprivation_population.columns:
        district_features['pop%_moderately_deprived'] = (
            deprivation_population[TierLabels.MODERATELY_DEPRIVED] / total_population
        ) * 100
    else:
        district_features['pop%_moderately_deprived'] = 0

    if TierLabels.LEAST_DEPRIVED in deprivation_population.columns:
        district_features['pop%_least_deprived'] = (
            deprivation_population[TierLabels.LEAST_DEPRIVED] / total_population
        ) * 100
    else:
        district_features['pop%_least_deprived'] = 0

    cols_to_fill = ['pop%_most_deprived', 'pop%_moderately_deprived', 'pop%_least_deprived']
    for col in cols_to_fill:
        if col in district_features.columns:
            district_features[col] = district_features[col].fillna(0)

    return district_features


def engineer_features(lsoa_data: pd.DataFrame, district_lsoa_map: pd.DataFrame,
                      membership_df: pd.DataFrame, reserve_coords: dict,
                      selected_features: list, imd_data: pd.DataFrame = None,
                      config: dict = None) -> pd.DataFrame:
    """Aggregate LSOA data to District level with population-weighted IMD and ferry-aware accessibility."""
    LOG.info("Engineering district-level features")
    
    if config is None:
        config = {}

    district_lsoa_data = pd.merge(district_lsoa_map, lsoa_data, on='lsoa21cd')

    if 'imd_decile' in district_lsoa_data.columns and 'Population' in district_lsoa_data.columns:
        district_lsoa_data['imd_decile'] = pd.to_numeric(district_lsoa_data['imd_decile'], errors='coerce')
        district_lsoa_data['Population'] = pd.to_numeric(district_lsoa_data['Population'], errors='coerce').fillna(0)
        district_lsoa_data['imd_decile_volume'] = district_lsoa_data['imd_decile'] * district_lsoa_data['Population']

    base_aggs = {}
    base_aggs['lat'] = 'mean'
    base_aggs['long'] = 'mean'

    if 'avg_fsm%' in selected_features:
        base_aggs['avg_fsm%'] = 'mean'

    if 'imd_decile_mean' in selected_features:
        base_aggs['imd_decile_volume'] = 'sum'

    if 'Population' in district_lsoa_data.columns:
        base_aggs['Population'] = 'sum'
    else:
        district_lsoa_data['Population'] = 0
        base_aggs['Population'] = 'sum'

    base_aggs['Region_Name'] = [lambda x: x.mode().iloc[0] if not x.mode().empty else 'Unknown']
    base_aggs['Authority_Name'] = [lambda x: x.mode().iloc[0] if not x.mode().empty else 'Unknown']
    base_aggs['lsoa21cd'] = 'count'

    district_features = district_lsoa_data.groupby('District').agg(base_aggs)

    if isinstance(district_features.columns, pd.MultiIndex):
        district_features.columns = ['_'.join(col).strip() for col in district_features.columns.values]

    rename_map = {
        'avg_fsm%_mean': 'avg_fsm%',
        'lat_mean': 'avg_lat',
        'long_mean': 'avg_long',
        'Region_Name_<lambda>': 'Region_Name',
        'Authority_Name_<lambda>': 'Authority_Name',
        'lsoa21cd_count': 'lsoa_count',
        'Population_sum': 'Population'
    }

    if 'imd_decile_volume_sum' in district_features.columns and 'Population_sum' in district_features.columns:
        district_features['imd_decile_mean'] = (district_features['imd_decile_volume_sum'] / district_features['Population_sum']).round(2)
        district_features.drop(columns=['imd_decile_volume_sum'], inplace=True)
        rename_map['imd_decile_mean'] = 'imd_decile_mean'

    for col in ['avg_fsm%', 'Region_Name', 'Authority_Name', 'Population']:
        if col in district_features.columns:
            rename_map[col] = col

    district_features.rename(columns=rename_map, inplace=True)
    district_features = calculate_deprivation_percentages(district_lsoa_data, district_features)

    centroids = district_lsoa_data.groupby('District').agg({
        'lat': 'mean',
        'long': 'mean'
    }).reset_index()

    district_features = district_features.merge(
        centroids.set_index('District')[['lat', 'long']],
        left_index=True,
        right_index=True,
        how='left'
    )

    if 'lat' in district_features.columns and 'avg_lat' not in district_features.columns:
        district_features.rename(columns={'lat': 'avg_lat', 'long': 'avg_long'}, inplace=True)

    place_names_path = config.get('file_paths', {}).get('district_place_names', 'district_place_names.csv')
    if os.path.exists(place_names_path):
        place_lookup = pd.read_csv(place_names_path, index_col='District')
        district_features['Post_Town'] = district_features.index.map(
            lambda d: place_lookup.loc[d, 'Post_Town'] if d in place_lookup.index else 'Unknown'
        )
    else:
        LOG.warning(f"Place names lookup not found - using 'Unknown'")
        district_features['Post_Town'] = 'Unknown'

    district_features = calculate_ors_ferry_metrics(
        district_features,
        imd_data=imd_data,
        district_lsoa_map=district_lsoa_map
    )

    district_features = district_features.drop(columns=['avg_lat', 'avg_long'], errors='ignore')

    temp_merge = pd.merge(
        district_features[['Population']].reset_index(),
        membership_df,
        on='District',
        how='left'
    )

    temp_merge['Visits'] = temp_merge['Visits'].fillna(0)
    temp_merge['visits_per_1000'] = (temp_merge['Visits'] / temp_merge['Population']) * 1000

    target_df = temp_merge[['District', 'Visits', 'visits_per_1000']].set_index('District')

    feature_columns = [col for col in selected_features if col in district_features.columns]
    X = district_features[feature_columns].copy()

    X['Population'] = district_features['Population']

    for col in ['Authority_Name', 'Region_Name', 'Post_Town', 'lsoa_count', 'nearest_ferry_terminal', 'competitor_context']:
        if col in district_features.columns:
            X[col] = district_features[col]

    final_dataset = pd.concat([X, target_df], axis=1)
    final_dataset = final_dataset.reset_index()

    if 'Visits' in final_dataset.columns:
        final_dataset['Visits'] = final_dataset['Visits'].fillna(0)

    for col in selected_features:
        if col in final_dataset.columns and pd.api.types.is_numeric_dtype(final_dataset[col]):
            final_dataset[col] = final_dataset[col].fillna(0)

    LOG.info(f"District features created: {len(final_dataset)} districts")
    return final_dataset


def execute_data_pipeline(config: dict, skip_checkpoints: bool = False) -> tuple:
    """Execute data loading and processing pipeline with checkpoint support."""
    LOG.info("Starting data pipeline")

    checkpoint_dir = config.get('checkpoint_dir', 'checkpoints')
    os.makedirs(checkpoint_dir, exist_ok=True)

    lsoa_checkpoint = os.path.join(checkpoint_dir, 'lsoa_master.parquet')
    features_checkpoint = os.path.join(checkpoint_dir, 'engineered_features.parquet')
    district_map_checkpoint = os.path.join(checkpoint_dir, 'district_lsoa_map.parquet')
    ons_checkpoint = os.path.join(checkpoint_dir, 'ons_clean.parquet')

    if not skip_checkpoints and os.path.exists(features_checkpoint):
        LOG.info(f"Loading engineered features from checkpoint: {features_checkpoint}")
        ml_dataset = pd.read_parquet(features_checkpoint)
        lsoa_master_df = pd.read_parquet(lsoa_checkpoint) if os.path.exists(lsoa_checkpoint) else None
        district_lsoa_map = pd.read_parquet(district_map_checkpoint) if os.path.exists(district_map_checkpoint) else None
        ons_df = pd.read_parquet(ons_checkpoint) if os.path.exists(ons_checkpoint) else None
        school_df = None
        return ml_dataset, lsoa_master_df, district_lsoa_map, school_df, ons_df

    raw_data = load_data(config['file_paths'])
    school_df = raw_data.get('school_data')
    ons_df = raw_data.get('ons_df')
    imd_decile = raw_data.get('imd_decile')

    lsoa_master_df, district_lsoa_map, membership_df, ons_clean = clean_and_merge(raw_data)

    if lsoa_master_df is not None:
        lsoa_master_df.to_parquet(lsoa_checkpoint, index=False)
        LOG.info(f"Saved LSOA master checkpoint: {lsoa_checkpoint}")
    if district_lsoa_map is not None:
        district_lsoa_map.to_parquet(district_map_checkpoint, index=False)
        LOG.info(f"Saved district map checkpoint: {district_map_checkpoint}")
    if ons_clean is not None:
        ons_clean.to_parquet(ons_checkpoint, index=False)
        LOG.info(f"Saved ONS checkpoint: {ons_checkpoint}")

    LOG.info("Engineering district-level features")
    ml_dataset = engineer_features(
        lsoa_master_df, district_lsoa_map, membership_df,
        config['BI_coordinates'], config['selected_features'],
        imd_data=imd_decile, config=config
    )

    ml_dataset['Population'] = pd.to_numeric(ml_dataset['Population'], errors='coerce').replace(0, np.nan)
    ml_dataset['visits_per_1000'] = (ml_dataset['Visits'] / ml_dataset['Population']) * 1000
    ml_dataset['visits_per_1000'] = ml_dataset['visits_per_1000'].fillna(0)

    ml_dataset.to_csv(config['output_files']['ml_ready_data'], index=False)
    LOG.info(f"Data saved to '{config['output_files']['ml_ready_data']}'")

    ml_dataset.to_parquet(features_checkpoint, index=False)
    LOG.info(f"Saved engineered features checkpoint: {features_checkpoint}")

    return ml_dataset, lsoa_master_df, district_lsoa_map, school_df, ons_clean
