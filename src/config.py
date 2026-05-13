# -*- coding: utf-8 -*-
"""Configuration management with intelligent path resolution."""

import copy
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from src.constants import (
    DeprivationConstants,
    FerryConstants,
    GeographicConstants,
    ModelConstants,
    RoutingConstants,
    VisualizationConstants,
)

LOG = logging.getLogger("Brownsea_Equity_Analysis")

BASE_CONFIG: Dict[str, Any] = {
    "file_paths": {
        "school_info": "2024-2025_england_school_information.csv",
        "school_census": "2024-2025_england_census.csv",
        "imd_decile": "File_7_IoD2025_All_Ranks_Scores_Deciles_Population_Denominators.csv",
        "lad_names": "Local_Authority_Districts_May_2024_Boundaries_UK_BFC_8116196853881618041.csv",
        "lad_regions": "Local_Authority_District_to_Region_(April_2025)_Lookup_in_EN_v2.csv",
        "membership": "BI visiting members information by post code 2025.csv",
        "ons_data": "ONSPD_August_2025.csv",
        "district_place_names": "district_place_names.csv",
    },
    "BI_coordinates": {
        "latitude": GeographicConstants.BI_LATITUDE,
        "longitude": GeographicConstants.BI_LONGITUDE,
    },
    "deprivation_categories": {
        "most_deprived": DeprivationConstants.MOST_DEPRIVED_DECILES,
        "moderately_deprived": DeprivationConstants.MODERATELY_DEPRIVED_DECILES,
        "least_deprived": DeprivationConstants.LEAST_DEPRIVED_DECILES,
    },
    "deprivation_thresholds": {
        "population_most_deprived": DeprivationConstants.HIGH_DEPRIVATION_POPULATION,
        "population_least_deprived": DeprivationConstants.LOW_DEPRIVATION_POPULATION,
    },
    "intersection_thresholds": {
        "high_fsm": DeprivationConstants.HIGH_FSM_THRESHOLD,
        "medium_fsm": DeprivationConstants.MEDIUM_FSM_THRESHOLD,
        "low_visit_rate": DeprivationConstants.LOW_VISIT_RATE_THRESHOLD,
        "medium_visit_rate": DeprivationConstants.MEDIUM_VISIT_RATE_THRESHOLD,
        "high_visit_rate": DeprivationConstants.HIGH_VISIT_RATE_THRESHOLD,
        "target_visit_rate": DeprivationConstants.TARGET_VISIT_RATE,
    },
    "intervention_thresholds": {
        "strategic_deprivation": 40,
        "strategic_fsm": 15,
    },
    "ferry_constants": {
        "car_speed_kmph": FerryConstants.CAR_SPEED_KMPH,
        "road_distance_factor": FerryConstants.ROAD_DISTANCE_FACTOR,
        "max_acceptable_time": FerryConstants.MAX_ACCEPTABLE_TIME,
    },
    "visualization": {
        "geojson_repo_url": VisualizationConstants.GEOJSON_REPO_URL,
        "geojson_local_path": VisualizationConstants.GEOJSON_LOCAL_PATH,
        "map_center_lat": VisualizationConstants.MAP_CENTER_LAT,
        "map_center_lon": VisualizationConstants.MAP_CENTER_LON,
        "map_zoom": VisualizationConstants.MAP_ZOOM,
        "dorset_postcode_areas": GeographicConstants.DORSET_POSTCODE_AREAS,
    },
    "model_params": {
        "test_size": ModelConstants.TEST_SIZE,
        "random_state": ModelConstants.RANDOM_STATE,
        "n_splits_cv": ModelConstants.N_SPLITS_CV,
        "n_iter_tuning": ModelConstants.N_ITER_TUNING,
        "optuna_trials": 30,
        "optuna_pruning": True,
        "eval_metric": "mae",
    },
    "selected_features": [
        "total_journey_min",
        "nearest_competitor_drive_min",
        "imd_decile_mean",
        "geo_barriers_decile",
        "wider_barriers_decile",
        "income_decile",
        "avg_fsm%",
        "pop%_most_deprived",
        "pop%_moderately_deprived",
        "pop%_least_deprived",
    ],
    "reference_files": {
        "nt_sites": "nt_sites.csv",
    },
    "output_files": {
        "ml_ready_data": "ml_ready_district_data.csv",
        "three_way_intersection": "three_way_intersection_analysis_v2.csv",
        "geocoding_cache": "geocoded_districts_cache.json",
        "postcode_lookup_csv": "postcode_lookup.csv",
        "postcode_lookup_parquet": "postcode_lookup.parquet",
        "postcode_lookup_html": "postcode_lookup.html",
        "postcode_lookup_json": "postcode_lookup.json",
        "postcode_app_html": "postcode_app.html",
        "run_manifest": "run_manifest.json",
    },
    "sensitivity_scenarios": [
        {
            "feature": "total_journey_min",
            "delta": -15,
            "strategy_name": "Direct Transport Links (Train Station Shuttles & Fast Ferries)",
        },
        {
            "feature": "geo_barriers_decile",
            "delta": 2,
            "strategy_name": "Community Transport Partnerships (Funded Minibus Excursions)",
        },
        {
            "feature": "income_decile",
            "delta": 2,
            "strategy_name": "Inclusive Ticketing (Subsidized Family Passes & Travel Grants)",
        },
    ],
}


def find_data_directory() -> Path:
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    env_data_dir = os.environ.get("BROWSEA_DATA_DIR", "").strip()

    search_paths = [
        Path(env_data_dir) if env_data_dir else None,
        Path("/content/brownsea_pipeline/data"),
        Path("/content/data"),
        project_root / "data",
        Path.cwd() / "data",
        project_root.parent / "data",
        Path.home() / "brownsea_data",
    ]

    expected_files = {
        "File_7_IoD2025_All_Ranks_Scores_Deciles_Population_Denominators.csv",
        "2024-2025_england_school_information.csv",
        "ONSPD_August_2025.csv",
    }

    for path in search_paths:
        if not path or not path.exists() or not path.is_dir():
            continue
        if any((path / expected).exists() for expected in expected_files):
            LOG.info(f"Found data directory: {path}")
            return path

    default_path = project_root / "data"
    default_path.mkdir(parents=True, exist_ok=True)
    LOG.warning(f"No data directory found. Created: {default_path}")
    return default_path


def resolve_file_path(filename: str, data_dir: Path) -> Path:
    search_locations = [
        data_dir / filename,
        data_dir / filename.replace(" ", "_"),
        data_dir / filename.lower(),
        Path.cwd() / filename,
        Path.cwd() / "data" / filename,
        Path("/content") / "data" / filename,
        Path("/content") / filename,
    ]
    for location in search_locations:
        if location.exists() and location.is_file():
            LOG.debug(f"Resolved {filename} -> {location}")
            return location
    return data_dir / filename


def resolve_reference_path(filename: str) -> Path:
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    env_path = os.environ.get("BROWSEA_NT_SITES_PATH", "").strip()
    search_locations = [
        Path(env_path) if env_path else None,
        project_root / "data" / "reference" / filename,
        Path.cwd() / "data" / "reference" / filename,
        Path.cwd() / filename,
    ]
    for location in search_locations:
        if location and location.exists() and location.is_file():
            return location
    return project_root / "data" / "reference" / filename


def detect_runtime() -> str:
    try:
        import sys

        if "google.colab" in sys.modules:
            return "colab"
    except Exception:
        pass

    try:
        from IPython import get_ipython

        if "google.colab" in str(get_ipython()):
            return "colab"
    except Exception:
        pass

    if os.environ.get("BROWSEA_ENV") == "production":
        return "production"
    if Path("/data").exists() and os.environ.get("BROWSEA_ENV") != "local":
        return "production"
    return "local"


def apply_output_directory(config: Dict[str, Any], output_dir: str) -> Dict[str, Any]:
    build_dir = Path(output_dir)

    artifact_dir = build_dir / "artifacts"
    report_dir = build_dir / "reports"
    checkpoint_dir = build_dir / "checkpoints"
    log_dir = build_dir / "logs"

    # Shared cache should live at the output-root level, not inside each build.
    # If output_dir looks like <output_root>/builds/<run_id>, use <output_root>/cache/route_cache
    # Otherwise fall back to <output_dir>/cache/route_cache.
    if build_dir.parent.name == "builds" and build_dir.parent.parent != build_dir.parent:
        shared_output_root = build_dir.parent.parent
    else:
        shared_output_root = build_dir

    shared_cache_dir = shared_output_root / "cache" / "route_cache"

    for path in (build_dir, artifact_dir, report_dir, checkpoint_dir, log_dir, shared_cache_dir):
        path.mkdir(parents=True, exist_ok=True)

    config["output_dir"] = str(build_dir)
    config["artifact_dir"] = str(artifact_dir)
    config["report_dir"] = str(report_dir)
    config["checkpoint_dir"] = str(checkpoint_dir)
    config["log_dir"] = str(log_dir)

    # New shared cache fields
    config["output_root"] = str(shared_output_root)
    config["shared_cache_dir"] = str(shared_output_root / "cache")
    config["route_cache_dir"] = str(shared_cache_dir)

    config["output_files"] = {
        "ml_ready_data": str(artifact_dir / "ml_ready_district_data.csv"),
        "three_way_intersection": str(artifact_dir / "three_way_intersection_analysis_v2.csv"),
        "geocoding_cache": str(artifact_dir / "geocoded_districts_cache.json"),
        "postcode_lookup_csv": str(artifact_dir / "postcode_lookup.csv"),
        "postcode_lookup_parquet": str(artifact_dir / "postcode_lookup.parquet"),
        "postcode_lookup_html": str(report_dir / "postcode_lookup.html"),
        "postcode_lookup_json": str(artifact_dir / "postcode_lookup.json"),
        "postcode_app_html": str(report_dir / "postcode_app.html"),
        "run_manifest": str(build_dir / "run_manifest.json"),
    }

    # Route cache now lives outside the build folder and is reused across builds
    RoutingConstants.CACHE_DIR = str(shared_cache_dir)
    RoutingConstants.BROWNSEA_CACHE_FILE = str(shared_cache_dir / "brownsea_routes.json")
    RoutingConstants.BROWNSEA_CACHE_META_FILE = str(shared_cache_dir / "brownsea_routes.metadata.json")
    RoutingConstants.COMPETITOR_CACHE_FILE = str(shared_cache_dir / "competitor_routes.json")
    RoutingConstants.COMPETITOR_CACHE_META_FILE = str(shared_cache_dir / "competitor_routes.metadata.json")

    return config

def get_config(runtime: Optional[str] = None) -> Dict[str, Any]:
    runtime = runtime or detect_runtime()
    config = copy.deepcopy(BASE_CONFIG)
    config["runtime"] = runtime

    data_dir = find_data_directory()
    config["data_directory"] = str(data_dir)

    resolved_paths: Dict[str, str] = {}
    for key, filename in config["file_paths"].items():
        resolved_paths[key] = str(resolve_file_path(filename, data_dir))
    config["file_paths"] = resolved_paths

    reference_paths: Dict[str, str] = {}
    for key, filename in config.get("reference_files", {}).items():
        reference_paths[key] = str(resolve_reference_path(filename))
    config["reference_paths"] = reference_paths

    LOG.info(f"Runtime: {runtime}")
    LOG.info(f"Data directory: {data_dir}")
    for key, path in resolved_paths.items():
        if Path(path).exists():
            LOG.debug(f"  FOUND {key}: {Path(path).name}")
        else:
            LOG.warning(f"  MISSING {key}: {Path(path).name} (not found)")
    for key, path in reference_paths.items():
        if Path(path).exists():
            LOG.info(f"Reference data: {key} -> {path}")
        else:
            LOG.warning(f"  MISSING reference {key}: {Path(path).name} (not found)")

    ors_key = os.environ.get("ORS_API_KEY", "").strip()
    if ors_key:
        RoutingConstants.ORS_API_KEY = ors_key
        LOG.info("ORS API key loaded from environment")
    else:
        RoutingConstants.ORS_API_KEY = ""

    output_dir = os.environ.get("BROWSEA_OUTPUT_DIR", "outputs")
    return apply_output_directory(config, output_dir)


def validate_runtime_configuration(config: Dict[str, Any]) -> None:
    missing = []
    present = []
    for name, path in config["file_paths"].items():
        if name == "district_place_names":
            continue
        if Path(path).exists():
            present.append(name)
        else:
            missing.append(f"{name}: {path}")

    ref_missing = []
    ref_present = []
    for name, path in config.get("reference_paths", {}).items():
        if Path(path).exists():
            ref_present.append(name)
        else:
            ref_missing.append(f"{name}: {path}")

    LOG.info(
        "Input validation summary: %s/%s required data files found; %s/%s reference files found",
        len(present),
        len(present) + len(missing),
        len(ref_present),
        len(ref_present) + len(ref_missing),
    )

    errors = []
    if missing:
        errors.append("Missing required input files:\n- " + "\n- ".join(missing))
    if ref_missing:
        errors.append("Missing required reference files:\n- " + "\n- ".join(ref_missing))
    if errors:
        raise FileNotFoundError("\n".join(errors))

    if not RoutingConstants.ORS_API_KEY:
        raise EnvironmentError(
            "ORS_API_KEY is required. Set it in the environment or .env before running the pipeline."
        )


def init_environment(runtime: Optional[str] = None) -> Dict[str, Any]:
    import warnings

    os.environ["OPTUNA_DISABLE_MULTIPROCESSING"] = "1"
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    os.environ["OMP_NUM_THREADS"] = "1"
    warnings.filterwarnings("ignore")

    runtime = runtime or detect_runtime()

    import plotly.io as pio

    if runtime == "colab":
        try:
            from google.colab import output
            output.enable_custom_widget_manager()
            LOG.debug("Custom widget manager enabled")
        except Exception as exc:
            LOG.debug(f"Widget manager not available: {exc}")

        try:
            pio.renderers.default = "png"
            LOG.info("Plotly renderer set to: png")
        except Exception:
            pass
    else:
        # CLI/production runs are file-first. Avoid opening browser windows from subprocesses.
        pio.renderers.default = "json"

    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    return get_config(runtime)
