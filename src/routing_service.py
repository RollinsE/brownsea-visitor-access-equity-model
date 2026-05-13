# -*- coding: utf-8 -*-
"""OpenRouteService routing and ferry access metrics."""

import logging
import os
import sys
from pathlib import Path
import time
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.constants import FerryConstants, GeographicConstants, RoutingConstants
from src.nt_sites import load_nt_sites
from src.route_cache import (
    cache_stats,
    expected_route_cache_metadata,
    load_route_cache,
    save_route_cache,
)
from src.utils import calculate_haversine_distance

LOG = logging.getLogger("Brownsea_Equity_Analysis")


TARGET_PREFIXES = tuple(GeographicConstants.BCP_DORSET_POSTCODES)


def _route_cache_key(start_coords, end_coords, route_scope: str) -> str:
    """Build the coordinate-level route-cache key used for ORS routes."""
    return f"{route_scope}|{start_coords[1]:.5f}_{start_coords[0]:.5f}_{end_coords[1]:.5f}_{end_coords[0]:.5f}"


def _route_cached(cache: dict, start_coords, end_coords, route_scope: str) -> bool:
    """Return True if the exact route already exists in the supplied cache."""
    return _route_cache_key(start_coords, end_coords, route_scope) in cache


def _should_consider_sandbanks_chain_ferry(district_name: object) -> bool:
    """Return True for Purbeck/Swanage-side districts where Sandbanks needs the chain ferry."""
    prefixes = tuple(FerryConstants.SANDBANKS_CHAIN_FERRY.get('purbeck_district_prefixes', []))
    return str(district_name or '').upper().replace(' ', '').startswith(prefixes)


def _sandbanks_chain_ferry_option(client, district_coords, cache, district_name: str, sandbanks_terminal: dict) -> dict | None:
    """Build a Sandbanks access candidate that explicitly includes the chain ferry.

    ORS is still used for the road leg to Shell Bay, but the harbour crossing is
    modelled as a fixed planning allowance. This prevents the app from treating
    Swanage/Studland-to-Sandbanks as direct road access.
    """
    chain = FerryConstants.SANDBANKS_CHAIN_FERRY
    if not chain.get('enabled', True):
        return None

    south = chain['south_landing']
    road_duration, road_distance = get_driving_time(
        client,
        district_coords,
        [float(south['lon']), float(south['lat'])],
        cache,
        district_name,
        south.get('name', 'Shell Bay chain ferry landing'),
        'brownsea_chain_ferry_access',
    )
    if road_duration is None or road_distance is None:
        return None

    chain_allowance = float(chain.get('allowance_minutes', 10))
    transfer = float(chain.get('terminal_transfer_minutes', 0))
    journey_to_terminal = road_duration + chain_allowance + transfer
    return {
        'terminal': sandbanks_terminal['name'],
        'driving_time': journey_to_terminal,
        'road_driving_time': road_duration,
        'chain_ferry_allowance_min': chain_allowance,
        'terminal_transfer_min': transfer,
        'access_route_mode': 'Sandbanks via chain ferry',
        'chain_ferry_used': True,
        'crossing_time': sandbanks_terminal['crossing_time_minutes'],
        'total_time': journey_to_terminal + sandbanks_terminal['crossing_time_minutes'],
        'distance': road_distance,
        'crossing_distance': sandbanks_terminal.get('crossing_distance_km', 0),
    }


def get_driving_time(client, start_coords, end_coords, cache, district_name, target_name, route_scope):
    """Get driving time between two points using OpenRouteService with caching."""
    cache_key = _route_cache_key(start_coords, end_coords, route_scope)
    if cache_key in cache:
        cached = cache[cache_key]
        return cached['duration'], cached['distance']

    try:
        routes = client.directions(
            coordinates=[start_coords, end_coords],
            profile=RoutingConstants.PROFILE,
            format='geojson',
            validate=False,
            options={"avoid_features": ["ferries"]},
            radiuses=[2000, 2000],
        )
        if routes and 'features' in routes and routes['features']:
            feature = routes['features'][0]
            if 'properties' in feature and 'segments' in feature['properties']:
                segment = feature['properties']['segments'][0]
                duration = segment['duration'] / 60
                distance = segment['distance'] / 1000
                cache[cache_key] = {
                    'duration': duration,
                    'distance': distance,
                    'timestamp': datetime.now().isoformat(),
                }
                time.sleep(RoutingConstants.REQUEST_DELAY)
                return duration, distance
    except Exception as exc:
        LOG.debug(f"Routing failed for {district_name} to {target_name}: {exc}")
    return None, None


def calculate_ferry_access_metrics(district_lat: float, district_lon: float) -> dict:
    """Calculate lightweight accessibility metrics using straight-line approximations."""
    if pd.isna(district_lat) or pd.isna(district_lon) or district_lat == 0 or district_lon == 0:
        return {
            'nearest_terminal': 'Unknown',
            'road_distance_to_terminal_km': np.nan,
            'travel_time_to_terminal_min': np.nan,
            'ferry_crossing_time_min': np.nan,
            'total_journey_time_min': np.nan,
            'accessibility_score': np.nan,
            'alternative_terminal': 'Unknown',
            'alt_total_time_min': np.nan,
        }

    terminal_distances = {}
    for terminal_id, terminal in FerryConstants.FERRY_TERMINALS.items():
        straight_distance = calculate_haversine_distance(district_lat, district_lon, terminal['lat'], terminal['lon'])
        road_distance = straight_distance * FerryConstants.ROAD_DISTANCE_FACTOR
        travel_time = (road_distance / FerryConstants.CAR_SPEED_KMPH) * 60
        terminal_distances[terminal_id] = {
            'terminal_name': terminal['name'],
            'straight_distance_km': round(straight_distance, 2),
            'road_distance_km': round(road_distance, 2),
            'travel_time_min': round(travel_time, 1),
            'crossing_time_min': terminal['crossing_time_minutes'],
            'total_time_min': round(travel_time + terminal['crossing_time_minutes'], 1),
        }

    best_terminal = min(terminal_distances.items(), key=lambda x: x[1]['total_time_min'])
    best = best_terminal[1]
    max_acceptable_time = FerryConstants.MAX_ACCEPTABLE_TIME
    total_time = best['total_time_min']
    accessibility_score = 0 if total_time >= max_acceptable_time else 100 * (1 - total_time / max_acceptable_time)
    alt_terminal_id = 'poole_quay' if best_terminal[0] == 'sandbanks' else 'sandbanks'
    alt_terminal = terminal_distances[alt_terminal_id]

    return {
        'nearest_terminal': best['terminal_name'],
        'straight_distance_km': best['straight_distance_km'],
        'road_distance_to_terminal_km': best['road_distance_km'],
        'travel_time_to_terminal_min': best['travel_time_min'],
        'ferry_crossing_time_min': best['crossing_time_min'],
        'total_journey_time_min': best['total_time_min'],
        'accessibility_score': round(accessibility_score, 1),
        'alternative_terminal': alt_terminal['terminal_name'],
        'alt_total_time_min': alt_terminal['total_time_min'],
    }


def _candidate_competitors(district_lat: float, district_lon: float, competitor_sites: pd.DataFrame, shortlist_size: int) -> pd.DataFrame:
    candidates = competitor_sites.copy()
    candidates = candidates[~candidates['site_name'].astype(str).str.contains('brownsea', case=False, na=False)].copy()
    candidates['crowfly_km'] = candidates.apply(
        lambda r: calculate_haversine_distance(district_lat, district_lon, float(r['lat']), float(r['lon'])),
        axis=1,
    )
    return candidates.nsmallest(shortlist_size, 'crowfly_km').reset_index(drop=True)


def _apply_barrier_adjustment(district_features: pd.DataFrame, idx, district_name: str, base_accessibility: float,
                              imd_data: Optional[pd.DataFrame], district_lsoa_map: Optional[pd.DataFrame]) -> int:
    if imd_data is None or district_lsoa_map is None:
        district_features.loc[idx, 'accessibility_score'] = round(base_accessibility, 1)
        return 0

    try:
        district_lsoas = district_lsoa_map[district_lsoa_map['District'] == district_name]['lsoa21cd'].tolist()
        if not district_lsoas:
            district_features.loc[idx, 'accessibility_score'] = round(base_accessibility, 1)
            return 0

        district_imd = imd_data[imd_data['LSOA code (2021)'].isin(district_lsoas)].copy()
        if district_imd.empty:
            district_features.loc[idx, 'accessibility_score'] = round(base_accessibility, 1)
            return 0

        pop_col = 'Total population: mid 2022'
        if pop_col not in district_imd.columns:
            pop_candidates = [c for c in district_imd.columns if 'population' in c.lower()]
            if not pop_candidates:
                district_features.loc[idx, 'accessibility_score'] = round(base_accessibility, 1)
                return 0
            pop_col = pop_candidates[0]

        district_imd[pop_col] = pd.to_numeric(district_imd[pop_col], errors='coerce').fillna(0)
        tot_pop = district_imd[pop_col].sum()

        col_geo = 'Geographical Barriers Sub-domain Decile (where 1 is most deprived 10% of LSOAs)'
        col_wider = 'Wider Barriers Sub-domain Decile (where 1 is most deprived 10% of LSOAs)'
        col_inc = 'Income Decile (where 1 is most deprived 10% of LSOAs)'

        for col in [col_geo, col_wider, col_inc]:
            district_imd[col] = pd.to_numeric(district_imd[col], errors='coerce')

        if tot_pop > 0:
            geo_barriers = (district_imd[col_geo] * district_imd[pop_col]).sum() / tot_pop
            wider_barriers = (district_imd[col_wider] * district_imd[pop_col]).sum() / tot_pop
            income = (district_imd[col_inc] * district_imd[pop_col]).sum() / tot_pop
        else:
            geo_barriers = district_imd[col_geo].mean()
            wider_barriers = district_imd[col_wider].mean()
            income = district_imd[col_inc].mean()

        district_features.loc[idx, 'geo_barriers_decile'] = geo_barriers
        district_features.loc[idx, 'wider_barriers_decile'] = wider_barriers
        district_features.loc[idx, 'income_decile'] = income

        geo_access = 1 - (geo_barriers / 10) if not pd.isna(geo_barriers) else 1
        wider_access = 1 - (wider_barriers / 10) if not pd.isna(wider_barriers) else 1
        affordability = 1 - (income / 10) if not pd.isna(income) else 1
        barriers_score = max(0, min(1, (0.4 * geo_access + 0.3 * wider_access + 0.3 * affordability)))
        district_features.loc[idx, 'accessibility_score'] = round(base_accessibility * barriers_score, 1)
        return 1
    except Exception as exc:
        LOG.debug(f"Barrier adjustment failed for {district_name}: {exc}")
        district_features.loc[idx, 'accessibility_score'] = round(base_accessibility, 1)
        return 0


def _route_cache_coverage(target_districts: pd.DataFrame, competitor_sites: pd.DataFrame, shortlist_size: int,
                          brownsea_cache: dict, competitor_cache: dict) -> dict:
    """Count required cached/missing routes before making any ORS calls."""
    terminals = FerryConstants.FERRY_TERMINALS
    brownsea_total = brownsea_hits = 0
    competitor_total = competitor_hits = 0

    for _, row in target_districts.iterrows():
        if pd.isna(row.get('avg_lat')) or pd.isna(row.get('avg_long')) or row['avg_lat'] == 0 or row['avg_long'] == 0:
            continue

        district_lat = float(row['avg_lat'])
        district_lon = float(row['avg_long'])
        district_coords = [district_lon, district_lat]

        district_name = row['District'] if 'District' in row else row.name
        for _, term in terminals.items():
            brownsea_total += 1
            if _route_cached(brownsea_cache, district_coords, [term['lon'], term['lat']], 'brownsea'):
                brownsea_hits += 1

        if _should_consider_sandbanks_chain_ferry(district_name):
            south = FerryConstants.SANDBANKS_CHAIN_FERRY['south_landing']
            brownsea_total += 1
            if _route_cached(brownsea_cache, district_coords, [south['lon'], south['lat']], 'brownsea_chain_ferry_access'):
                brownsea_hits += 1

        try:
            shortlisted = _candidate_competitors(district_lat, district_lon, competitor_sites, shortlist_size)
            for _, comp_data in shortlisted.iterrows():
                competitor_total += 1
                if _route_cached(competitor_cache, district_coords, [float(comp_data['lon']), float(comp_data['lat'])], 'competitor'):
                    competitor_hits += 1
        except Exception as exc:
            LOG.debug(f"Could not estimate competitor route cache coverage: {exc}")

    return {
        'brownsea_total': brownsea_total,
        'brownsea_hits': brownsea_hits,
        'brownsea_missing': brownsea_total - brownsea_hits,
        'competitor_total': competitor_total,
        'competitor_hits': competitor_hits,
        'competitor_missing': competitor_total - competitor_hits,
    }


def calculate_ors_ferry_metrics(district_features: pd.DataFrame,
                                imd_data: Optional[pd.DataFrame] = None,
                                district_lsoa_map: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Calculate ferry access metrics using ORS, with competitor shortlist routing."""
    LOG.info("Calculating ferry access metrics with OpenRouteService")

    district_features['geo_barriers_decile'] = np.nan
    district_features['wider_barriers_decile'] = np.nan
    district_features['income_decile'] = np.nan
    district_features['nearest_competitor_drive_min'] = np.nan
    district_features['competitor_context'] = ""

    if 'District' in district_features.columns:
        target_mask = district_features['District'].astype(str).str.startswith(TARGET_PREFIXES, na=False)
    else:
        target_mask = district_features.index.astype(str).str.startswith(TARGET_PREFIXES, na=False)
    target_districts = district_features[target_mask].copy()
    LOG.info(f"Found {len(target_districts)} target districts in BH/DT/SP areas")

    district_features['total_journey_min'] = FerryConstants.MAX_ACCEPTABLE_TIME
    district_features['accessibility_score'] = 0
    district_features['nearest_ferry_terminal'] = 'Outside Target Area'
    district_features['driving_time_min'] = 0
    district_features['road_drive_to_departure_terminal_min'] = np.nan
    district_features['chain_ferry_used'] = False
    district_features['chain_ferry_allowance_min'] = 0
    district_features['access_route_mode'] = 'Road to terminal'
    district_features['ferry_crossing_min'] = 0
    district_features['distance_to_terminal_km'] = 0

    if target_districts.empty:
        LOG.warning("No target districts found - using defaults")
        return district_features

    try:
        import openrouteservice
        client = openrouteservice.Client(key=RoutingConstants.ORS_API_KEY)
        LOG.info("OpenRouteService client initialized")
    except Exception as exc:
        LOG.error(f"Failed to initialize ORS client: {exc}")
        return district_features

    brownsea_meta = expected_route_cache_metadata(
        scope='brownsea',
        profile=RoutingConstants.PROFILE,
        cache_version=RoutingConstants.CACHE_VERSION,
    )
    brownsea_cache, brownsea_cache_hit = load_route_cache(
        RoutingConstants.BROWNSEA_CACHE_FILE,
        RoutingConstants.BROWNSEA_CACHE_META_FILE,
        brownsea_meta,
    )
    LOG.info(f"Loaded Brownsea route cache: {cache_stats(brownsea_cache)}")

    competitor_sites = load_nt_sites()
    competitor_meta = expected_route_cache_metadata(
        scope='competitor',
        profile=RoutingConstants.PROFILE,
        cache_version=RoutingConstants.CACHE_VERSION,
        competitor_file=None,
        shortlist_size=RoutingConstants.COMPETITOR_SHORTLIST_SIZE,
    )
    # use the explicit reference path if available from environment/default loader
    try:
        from src.nt_sites import get_default_nt_sites_path
        competitor_meta = expected_route_cache_metadata(
            scope='competitor',
            profile=RoutingConstants.PROFILE,
            cache_version=RoutingConstants.CACHE_VERSION,
            competitor_file=get_default_nt_sites_path(),
            shortlist_size=RoutingConstants.COMPETITOR_SHORTLIST_SIZE,
        )
    except Exception:
        pass
    competitor_cache, competitor_cache_hit = load_route_cache(
        RoutingConstants.COMPETITOR_CACHE_FILE,
        RoutingConstants.COMPETITOR_CACHE_META_FILE,
        competitor_meta,
    )
    LOG.info(f"Loaded competitor route cache: {cache_stats(competitor_cache)}")
    shortlist_size = getattr(RoutingConstants, 'COMPETITOR_SHORTLIST_SIZE', 12)
    LOG.info(f"Loaded {len(competitor_sites)} active NT competitor sites from reference file")
    LOG.info(f"Using shortlist size {shortlist_size} for competitor ORS routing")

    coverage = _route_cache_coverage(target_districts, competitor_sites, shortlist_size, brownsea_cache, competitor_cache)
    LOG.info(
        "Route cache coverage: Brownsea %s/%s cached; competitor %s/%s cached",
        coverage['brownsea_hits'],
        coverage['brownsea_total'],
        coverage['competitor_hits'],
        coverage['competitor_total'],
    )
    if coverage['brownsea_missing'] or coverage['competitor_missing']:
        LOG.info(
            "Missing route-cache entries: Brownsea=%s, competitor=%s. Missing routes will be requested once and saved to the shared cache.",
            coverage['brownsea_missing'],
            coverage['competitor_missing'],
        )
    else:
        LOG.info("All required route-cache entries already available; routing should complete without ORS calls.")

    terminals = FerryConstants.FERRY_TERMINALS
    successful_routes = 0
    barrier_success = 0
    competitor_successful = 0
    LOG.info(f"Calculating routes for {len(target_districts)} districts")

    progress_setting = os.environ.get('BROWSEA_PROGRESS', '').strip().lower()
    show_progress = progress_setting in {'force', 'always'} or (progress_setting in {'1', 'true', 'yes', 'y'} and sys.stdout.isatty())
    route_iter = tqdm(
        target_districts.iterrows(),
        total=len(target_districts),
        desc='Routing',
        disable=not show_progress,
        leave=False,
    )

    for counter, (idx, row) in enumerate(route_iter, start=1):
        district_name = row['District'] if 'District' in row else idx
        if pd.isna(row.get('avg_lat')) or pd.isna(row.get('avg_long')) or row['avg_lat'] == 0 or row['avg_long'] == 0:
            LOG.debug(f"Skipping {district_name} due to invalid centroid coordinates")
            continue

        district_lat = float(row['avg_lat'])
        district_lon = float(row['avg_long'])
        district_coords = [district_lon, district_lat]

        # 1) Brownsea terminal routing - always computed first and never overwritten by competitor failures.
        terminal_times = []
        for terminal_id, term in terminals.items():
            duration, distance = get_driving_time(
                client,
                district_coords,
                [term['lon'], term['lat']],
                brownsea_cache,
                district_name,
                term['name'],
                'brownsea'
            )
            if duration is not None and distance is not None:
                terminal_times.append({
                    'terminal': term['name'],
                    'driving_time': duration,
                    'road_driving_time': duration,
                    'chain_ferry_allowance_min': 0,
                    'terminal_transfer_min': 0,
                    'access_route_mode': 'Road to terminal',
                    'chain_ferry_used': False,
                    'crossing_time': term['crossing_time_minutes'],
                    'total_time': duration + term['crossing_time_minutes'],
                    'distance': distance,
                    'crossing_distance': term.get('crossing_distance_km', 0),
                })

            if terminal_id == 'sandbanks' and _should_consider_sandbanks_chain_ferry(district_name):
                chain_option = _sandbanks_chain_ferry_option(client, district_coords, brownsea_cache, district_name, term)
                if chain_option is not None:
                    terminal_times.append(chain_option)

        if not terminal_times:
            LOG.debug(f"No Brownsea terminal routes returned for {district_name}")
            continue

        best = min(terminal_times, key=lambda x: x['total_time'])
        successful_routes += 1

        district_features.loc[idx, 'nearest_ferry_terminal'] = best['terminal']
        district_features.loc[idx, 'driving_time_min'] = round(best['driving_time'], 1)
        district_features.loc[idx, 'road_drive_to_departure_terminal_min'] = round(best.get('road_driving_time', best['driving_time']), 1)
        district_features.loc[idx, 'chain_ferry_used'] = bool(best.get('chain_ferry_used', False))
        district_features.loc[idx, 'chain_ferry_allowance_min'] = round(float(best.get('chain_ferry_allowance_min', 0)), 1)
        district_features.loc[idx, 'access_route_mode'] = best.get('access_route_mode', 'Road to terminal')
        district_features.loc[idx, 'ferry_crossing_min'] = best['crossing_time']
        district_features.loc[idx, 'total_journey_min'] = round(best['total_time'], 1)
        district_features.loc[idx, 'distance_to_terminal_km'] = round(best['distance'], 1)

        base_accessibility = 0 if best['total_time'] >= FerryConstants.MAX_ACCEPTABLE_TIME else 100 * (1 - best['total_time'] / FerryConstants.MAX_ACCEPTABLE_TIME)
        barrier_success += _apply_barrier_adjustment(district_features, idx, district_name, base_accessibility, imd_data, district_lsoa_map)

        # 2) Competitor routing on shortlisted candidates only.
        try:
            shortlisted = _candidate_competitors(district_lat, district_lon, competitor_sites, shortlist_size)
            competitor_info = []
            competitor_times = []
            for _, comp_data in shortlisted.iterrows():
                comp_duration, comp_distance = get_driving_time(
                    client, district_coords, [float(comp_data['lon']), float(comp_data['lat'])], competitor_cache, district_name, comp_data['site_name'], 'competitor'
                )
                if comp_duration is not None and comp_distance is not None:
                    competitor_times.append(comp_duration)
                    competitor_info.append((comp_data['site_name'], comp_distance, comp_duration, comp_data.get('crowfly_km', np.nan)))

            if competitor_times:
                competitor_successful += 1
                district_features.loc[idx, 'nearest_competitor_drive_min'] = round(min(competitor_times), 1)
                closer_comps = [c for c in competitor_info if c[2] < best['total_time']]
                closer_comps.sort(key=lambda x: x[1])
                if not closer_comps:
                    closer_comps = sorted(competitor_info, key=lambda x: x[1])[:3]
                context_parts = [f"{c_name}: {round(c_dist)}Km" for c_name, c_dist, _, _ in closer_comps[:8]]
                total_bi_distance = best['distance'] + best['crossing_distance']
                context_parts.append(f"Brownsea Island: {round(total_bi_distance)}Km")
                district_features.loc[idx, 'competitor_context'] = ' [' + '; '.join(context_parts) + ']'
            else:
                total_bi_distance = best['distance'] + best['crossing_distance']
                district_features.loc[idx, 'competitor_context'] = f" [Brownsea Island: {round(total_bi_distance)}Km]"
        except Exception as exc:
            LOG.debug(f"Competitor routing failed for {district_name}: {exc}")
            total_bi_distance = best['distance'] + best['crossing_distance']
            district_features.loc[idx, 'competitor_context'] = f" [Brownsea Island: {round(total_bi_distance)}Km]"

        # Persist after each district so an interrupted Colab run keeps almost all newly fetched routes.
        save_route_cache(brownsea_cache, RoutingConstants.BROWNSEA_CACHE_FILE, RoutingConstants.BROWNSEA_CACHE_META_FILE, brownsea_meta)
        save_route_cache(competitor_cache, RoutingConstants.COMPETITOR_CACHE_FILE, RoutingConstants.COMPETITOR_CACHE_META_FILE, competitor_meta)

    save_route_cache(brownsea_cache, RoutingConstants.BROWNSEA_CACHE_FILE, RoutingConstants.BROWNSEA_CACHE_META_FILE, brownsea_meta)
    save_route_cache(competitor_cache, RoutingConstants.COMPETITOR_CACHE_FILE, RoutingConstants.COMPETITOR_CACHE_META_FILE, competitor_meta)
    LOG.info(f"Routes calculated for {successful_routes} districts")
    LOG.info(f"Barrier adjustments applied to {barrier_success} districts")
    LOG.info(f"Competitor shortlist routing succeeded for {competitor_successful} districts")
    return district_features
