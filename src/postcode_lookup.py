# -*- coding: utf-8 -*-
"""Postcode lookup artifact builder for browser-friendly outputs."""

import logging
import os
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from src.constants import FerryConstants, GeographicConstants
from src.utils import get_timestamp
from src.help_page import write_help_html
from src.web_ui import build_downloads_html, build_postcode_app_html, build_postcode_json
from src.nt_sites import load_nt_sites

LOG = logging.getLogger("Brownsea_Equity_Analysis")
EARTH_RADIUS_KM = 6371.0088


def _require_columns(df: pd.DataFrame, columns: set[str], frame_name: str) -> None:
    missing = sorted(columns - set(df.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {', '.join(missing)}")


def _write_parquet_with_fallback(df: pd.DataFrame, parquet_path: Path) -> None:
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as exc:
        LOG.warning("Could not write parquet artifact %s: %s", parquet_path, exc)
        fallback_path = parquet_path.with_suffix(parquet_path.suffix + ".skipped.txt")
        fallback_path.write_text(
            "Parquet artifact was not written because the local parquet engine failed. "
            "CSV and JSON artifacts were written successfully.\n"
            f"Reason: {exc}\n",
            encoding="utf-8",
        )


def _is_target_district(series: pd.Series) -> pd.Series:
    return series.astype(str).str.startswith(tuple(GeographicConstants.BCP_DORSET_POSTCODES), na=False)


def _is_purbeck_side_district(series: pd.Series) -> pd.Series:
    prefixes = tuple(FerryConstants.SANDBANKS_CHAIN_FERRY.get('purbeck_district_prefixes', []))
    return series.astype(str).str.upper().str.replace(' ', '', regex=False).str.startswith(prefixes, na=False)


def _haversine_np(lat1, lon1, lat2, lon2):
    lat1 = np.radians(np.asarray(lat1, dtype=float))
    lon1 = np.radians(np.asarray(lon1, dtype=float))
    lat2 = np.radians(np.asarray(lat2, dtype=float))
    lon2 = np.radians(np.asarray(lon2, dtype=float))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371.0088 * c


def _build_brownsea_metrics(postcodes: pd.DataFrame) -> pd.DataFrame:
    """Vectorized Brownsea access metrics for all postcodes.

    This includes the Sandbanks chain ferry for Swanage/Studland/Purbeck-side
    districts. Those areas do not have direct road access across the harbour
    mouth to Sandbanks. The chain-ferry allowance keeps the journey estimate
    aligned with the local access route.
    """
    lat = pd.to_numeric(postcodes['lat'], errors='coerce')
    lon = pd.to_numeric(postcodes['long'], errors='coerce')
    district = postcodes.get('district', pd.Series([''] * len(postcodes), index=postcodes.index))

    pq = FerryConstants.FERRY_TERMINALS['poole_quay']
    sb = FerryConstants.FERRY_TERMINALS['sandbanks']
    chain = FerryConstants.SANDBANKS_CHAIN_FERRY
    shell = chain['south_landing']

    pq_straight = _haversine_np(lat, lon, pq['lat'], pq['lon'])
    sb_straight = _haversine_np(lat, lon, sb['lat'], sb['lon'])
    shell_straight = _haversine_np(lat, lon, shell['lat'], shell['lon'])

    pq_road = pq_straight * FerryConstants.ROAD_DISTANCE_FACTOR
    sb_road = sb_straight * FerryConstants.ROAD_DISTANCE_FACTOR
    shell_road = shell_straight * FerryConstants.ROAD_DISTANCE_FACTOR

    pq_drive = (pq_road / FerryConstants.CAR_SPEED_KMPH) * 60
    sb_drive = (sb_road / FerryConstants.CAR_SPEED_KMPH) * 60
    shell_drive = (shell_road / FerryConstants.CAR_SPEED_KMPH) * 60

    pq_total = pq_drive + pq['crossing_time_minutes']
    sb_road_total = sb_drive + sb['crossing_time_minutes']

    chain_allowance = float(chain.get('allowance_minutes', 10))
    terminal_transfer = float(chain.get('terminal_transfer_minutes', 0))
    sb_chain_to_terminal = shell_drive + chain_allowance + terminal_transfer
    sb_chain_total = sb_chain_to_terminal + sb['crossing_time_minutes']

    # For Purbeck/Swanage-side postcodes, do not allow the straight-line
    # Sandbanks approximation to stand in for an impossible direct road route.
    purbeck_side = _is_purbeck_side_district(district).to_numpy()
    sb_road_candidate_total = np.where(purbeck_side, np.inf, sb_road_total)

    candidates = np.vstack([pq_total, sb_road_candidate_total, sb_chain_total])
    choice = np.nanargmin(candidates, axis=0)

    best_total = np.choose(choice, [pq_total, sb_road_total, sb_chain_total])
    best_road = np.choose(choice, [pq_road, sb_road, shell_road])
    best_to_terminal = np.choose(choice, [pq_drive, sb_drive, sb_chain_to_terminal])
    best_cross = np.choose(choice, [pq['crossing_time_minutes'], sb['crossing_time_minutes'], sb['crossing_time_minutes']])
    best_terminal = np.choose(choice, [pq['name'], sb['name'], sb['name']])
    chain_used = choice == 2
    route_mode = np.where(chain_used, 'Sandbanks via chain ferry', 'Road to terminal')
    chain_minutes = np.where(chain_used, chain_allowance, 0.0)

    alt_terminal = np.where(best_terminal == pq['name'], sb['name'], pq['name'])
    # Alternative is shown as the other Brownsea passenger-ferry terminal. For
    # Purbeck-side Sandbanks cases, the Sandbanks total includes the chain ferry.
    alt_total = np.where(best_terminal == pq['name'], np.minimum(sb_road_total, sb_chain_total), pq_total)

    accessibility_score = np.where(
        best_total >= FerryConstants.MAX_ACCEPTABLE_TIME,
        0,
        100 * (1 - best_total / FerryConstants.MAX_ACCEPTABLE_TIME)
    )

    invalid = lat.isna() | lon.isna() | (lat == 0) | (lon == 0)

    metrics = pd.DataFrame({
        'brownsea_departure_terminal': best_terminal,
        'access_route_mode': route_mode,
        'chain_ferry_used': chain_used,
        'chain_ferry_allowance_min': np.round(chain_minutes, 1),
        'distance_to_departure_terminal_km': np.round(best_road, 2),
        'drive_to_departure_terminal_min': np.round(best_to_terminal, 1),
        'brownsea_crossing_min': np.round(best_cross, 1),
        'total_brownsea_journey_min': np.round(best_total, 1),
        'brownsea_accessibility_score': np.round(accessibility_score, 1),
        'alternative_brownsea_departure_terminal': alt_terminal,
        'alternative_total_brownsea_journey_min': np.round(alt_total, 1),
    })

    if invalid.any():
        metrics.loc[invalid, 'brownsea_departure_terminal'] = 'Unknown'
        metrics.loc[invalid, 'access_route_mode'] = 'Unknown'
        metrics.loc[invalid, 'chain_ferry_used'] = False
        metrics.loc[invalid, 'chain_ferry_allowance_min'] = np.nan
        metrics.loc[invalid, 'distance_to_departure_terminal_km'] = np.nan
        metrics.loc[invalid, 'drive_to_departure_terminal_min'] = np.nan
        metrics.loc[invalid, 'brownsea_crossing_min'] = np.nan
        metrics.loc[invalid, 'total_brownsea_journey_min'] = np.nan
        metrics.loc[invalid, 'brownsea_accessibility_score'] = np.nan
        metrics.loc[invalid, 'alternative_brownsea_departure_terminal'] = 'Unknown'
        metrics.loc[invalid, 'alternative_total_brownsea_journey_min'] = np.nan

    return metrics

def _build_competitor_matches_vectorized(postcodes: pd.DataFrame, competitors: pd.DataFrame) -> pd.DataFrame:
    """Fallback nearest competitor matching without optional sklearn import."""
    post_lat = pd.to_numeric(postcodes['lat'], errors='coerce')
    post_lon = pd.to_numeric(postcodes['long'], errors='coerce')
    invalid = post_lat.isna() | post_lon.isna() | (post_lat == 0) | (post_lon == 0)

    nearest_names = np.array(['Unknown'] * len(postcodes), dtype=object)
    nearest_drive = np.full(len(postcodes), np.nan)

    valid_idx = np.where(~invalid)[0]
    if len(valid_idx):
        lat1 = post_lat.iloc[valid_idx].to_numpy()[:, None]
        lon1 = post_lon.iloc[valid_idx].to_numpy()[:, None]
        lat2 = competitors['lat'].to_numpy()[None, :]
        lon2 = competitors['lon'].to_numpy()[None, :]
        dist_km = _haversine_np(lat1, lon1, lat2, lon2)
        nearest = np.nanargmin(dist_km, axis=1)
        nearest_dist = dist_km[np.arange(len(valid_idx)), nearest]
        drive_min = (nearest_dist * FerryConstants.ROAD_DISTANCE_FACTOR / FerryConstants.CAR_SPEED_KMPH) * 60
        nearest_names[valid_idx] = competitors.iloc[nearest]['site_name'].astype(str).to_numpy()
        nearest_drive[valid_idx] = np.round(drive_min, 1)

    return pd.DataFrame({
        'nearest_nt_site_name': nearest_names,
        'nearest_nt_site_drive_min': nearest_drive,
    })


def _build_competitor_matches(postcodes: pd.DataFrame, competitor_sites: pd.DataFrame) -> pd.DataFrame:
    """Fast nearest competitor matching using BallTree, with a deterministic NumPy fallback."""
    competitors = competitor_sites.copy()
    competitors = competitors[~competitors['site_name'].astype(str).str.contains('brownsea', case=False, na=False)].copy()
    competitors['lat'] = pd.to_numeric(competitors['lat'], errors='coerce')
    competitors['lon'] = pd.to_numeric(competitors['lon'], errors='coerce')
    competitors = competitors.dropna(subset=['lat', 'lon']).reset_index(drop=True)

    if competitors.empty:
        return pd.DataFrame({
            'nearest_nt_site_name': ['Unknown'] * len(postcodes),
            'nearest_nt_site_drive_min': [np.nan] * len(postcodes),
        })

    if os.environ.get("BROWNSEA_DISABLE_BALLTREE") == "1":
        LOG.info("Using NumPy nearest-neighbour fallback for postcode lookup")
        return _build_competitor_matches_vectorized(postcodes, competitors)

    LOG.info("Using BallTree nearest-neighbour competitor matching for postcode lookup")

    try:
        from sklearn.neighbors import BallTree
    except Exception as exc:
        LOG.warning("BallTree unavailable; using NumPy nearest-neighbour fallback: %s", exc)
        return _build_competitor_matches_vectorized(postcodes, competitors)

    comp_coords = np.deg2rad(competitors[['lat', 'lon']].to_numpy())
    tree = BallTree(comp_coords, metric='haversine')

    post_lat = pd.to_numeric(postcodes['lat'], errors='coerce')
    post_lon = pd.to_numeric(postcodes['long'], errors='coerce')
    invalid = post_lat.isna() | post_lon.isna() | (post_lat == 0) | (post_lon == 0)

    query_df = pd.DataFrame({'lat': post_lat, 'lon': post_lon})
    valid_coords = np.deg2rad(query_df.loc[~invalid, ['lat', 'lon']].to_numpy())

    nearest_names = np.array(['Unknown'] * len(postcodes), dtype=object)
    nearest_drive = np.full(len(postcodes), np.nan)

    if len(valid_coords):
        dist_rad, ind = tree.query(valid_coords, k=1)
        dist_km = dist_rad[:, 0] * EARTH_RADIUS_KM
        chosen = competitors.iloc[ind[:, 0]].reset_index(drop=True)
        drive_min = (dist_km * FerryConstants.ROAD_DISTANCE_FACTOR / FerryConstants.CAR_SPEED_KMPH) * 60

        nearest_names[np.where(~invalid)[0]] = chosen['site_name'].astype(str).to_numpy()
        nearest_drive[np.where(~invalid)[0]] = np.round(drive_min, 1)

    return pd.DataFrame({
        'nearest_nt_site_name': nearest_names,
        'nearest_nt_site_drive_min': nearest_drive,
    })


def _build_lookup_rows(postcodes: pd.DataFrame, competitor_sites: pd.DataFrame) -> pd.DataFrame:
    metrics = _build_brownsea_metrics(postcodes)
    competitor_df = _build_competitor_matches(postcodes, competitor_sites)
    lookup = pd.concat([postcodes.reset_index(drop=True), metrics, competitor_df], axis=1)
    lookup['brownsea_vs_nearest_nt_gap_min'] = (
        lookup['total_brownsea_journey_min'] - lookup['nearest_nt_site_drive_min']
    ).round(1)
    return lookup


def _make_postcode_html(df: pd.DataFrame, output_path: Path, config: Dict) -> None:
    metadata = {
        'title': 'Brownsea Visitor Opportunity Lookup',
        'generated_at': get_timestamp(),
        'download_csv': '../artifacts/postcode_lookup.csv',
        'download_json': '../artifacts/postcode_lookup.json',
        'reports_index': 'index.html',
        'downloads_page': 'downloads.html',
        'help_page': 'help.html',
    }
    build_postcode_app_html(df, output_path, metadata)


def build_postcode_lookup_artifacts(
    ons_df: pd.DataFrame,
    lsoa_master_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    config: Dict,
) -> pd.DataFrame:
    """Build target-area postcode lookup artifacts for browser and CSV consumption."""
    if ons_df is None or lsoa_master_df is None or analysis_df is None:
        LOG.warning('Skipping postcode lookup - required inputs are missing')
        return pd.DataFrame()

    _require_columns(ons_df, {'POSTCODE', 'District', 'lat', 'long', 'lsoa21cd'}, 'ons_df')
    _require_columns(lsoa_master_df, {'lsoa21cd'}, 'lsoa_master_df')
    _require_columns(analysis_df, {'District'}, 'analysis_df')

    target_postcodes = ons_df[_is_target_district(ons_df['District'])].copy()
    if target_postcodes.empty:
        LOG.warning('Skipping postcode lookup - no target-area postcodes found')
        return pd.DataFrame()

    LOG.info(f"Building postcode lookup table for {len(target_postcodes)} target-area postcodes")

    postcode_cols = {
        'POSTCODE': 'postcode',
        'District': 'district',
        'lat': 'lat',
        'long': 'long',
        'lsoa21cd': 'lsoa21cd',
    }
    target_postcodes = target_postcodes[list(postcode_cols.keys())].rename(columns=postcode_cols)

    lsoa_cols = [
        col for col in ['lsoa21cd', 'imd_decile', 'deprivation_category', 'avg_fsm%', 'Authority_Name', 'Region_Name']
        if col in lsoa_master_df.columns
    ]
    lsoa_context = lsoa_master_df[lsoa_cols].drop_duplicates(subset=['lsoa21cd'])
    target_postcodes = target_postcodes.merge(lsoa_context, on='lsoa21cd', how='left')
    target_postcodes = target_postcodes.rename(columns={'Authority_Name': 'authority_name', 'Region_Name': 'region_name'})

    district_cols = [
        col for col in ['District', 'visits_per_1000', 'predicted_visit_rate', 'performance_gap', 'priority_zone', 'intervention_type', 'shap_narrative', 'need_tier', 'visit_tier', 'composite_need_score']
        if col in analysis_df.columns
    ]
    district_context = analysis_df[district_cols].drop_duplicates(subset=['District'])
    district_context = district_context.rename(columns={
        'District': 'district',
        'visits_per_1000': 'district_visits_per_1000',
        'predicted_visit_rate': 'district_predicted_visit_rate',
        'performance_gap': 'district_model_gap_per_1000',
    })
    target_postcodes = target_postcodes.merge(district_context, on='district', how='left')

    competitor_sites = load_nt_sites(config.get('reference_paths', {}).get('nt_sites'))
    lookup = _build_lookup_rows(target_postcodes, competitor_sites)
    lookup['postcode_clean'] = lookup['postcode'].astype(str).str.replace(r"\s+", "", regex=True).str.upper()

    csv_path = Path(config['output_files']['postcode_lookup_csv'])
    parquet_path = Path(config['output_files']['postcode_lookup_parquet'])
    html_path = Path(config['output_files']['postcode_lookup_html'])
    json_path = Path(config['output_files']['postcode_lookup_json'])
    app_path = Path(config['output_files']['postcode_app_html'])

    for path in (csv_path, parquet_path, html_path, json_path, app_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    lookup.to_csv(csv_path, index=False)
    _write_parquet_with_fallback(lookup, parquet_path)
    build_postcode_json(lookup, json_path)
    _make_postcode_html(lookup, html_path, config)
    _make_postcode_html(lookup, app_path, config)
    release_dir = app_path.parent.parent
    build_downloads_html(release_dir, app_path.parent / "downloads.html")
    write_help_html(app_path.parent / "help.html", home_href="postcode_app.html", downloads_href="downloads.html", reports_href="index.html")

    LOG.info("Built postcode lookup table with %s target-area rows", len(lookup))
    LOG.info("Postcode artifacts: csv=%s json=%s html=%s app=%s", csv_path, json_path, html_path, app_path)
    return lookup
