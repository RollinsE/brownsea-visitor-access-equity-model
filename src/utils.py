# -*- coding: utf-8 -*-
"""Shared utility functions for logging, caching, and data preparation."""

import logging
import json
import os
import time
from typing import Dict, Optional, Any
from datetime import datetime
import numpy as np
import pandas as pd

# Setup logging
class SafeStreamHandler(logging.StreamHandler):
    """Stream handler that does not dump traceback noise when Colab disconnects stdout/stderr.

    Google Colab occasionally detaches the notebook output stream while a long-running
    subprocess is still alive. The standard logging StreamHandler reports that as a
    large "--- Logging error ---" traceback for every log message. The pipeline should
    keep running and continue writing the file log instead.
    """

    def __init__(self, stream=None):
        super().__init__(stream)
        self._stream_broken = False

    def emit(self, record):
        if self._stream_broken:
            return
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except (BrokenPipeError, OSError, ValueError):
            self._stream_broken = True
        except Exception:
            # Do not allow console logging failures to interrupt or pollute pipeline runs.
            self._stream_broken = True

    def flush(self):
        if self._stream_broken:
            return
        try:
            super().flush()
        except (BrokenPipeError, OSError, ValueError):
            self._stream_broken = True


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Configure robust file-first logging for the pipeline."""
    import sys

    level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)-8s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Avoid logging.basicConfig(force=True), which installs a standard stream handler
    # that can explode with repeated tracebacks in Colab subprocess output.
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(logging.WARNING)

    LOG = logging.getLogger("Brownsea_Equity_Analysis")
    for handler in list(LOG.handlers):
        LOG.removeHandler(handler)
    LOG.setLevel(level)
    LOG.propagate = False

    console_handler = SafeStreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    LOG.addHandler(console_handler)

    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        LOG.addHandler(file_handler)

    return LOG


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_deprivation_category(imd_decile: int) -> str:
    """Categorize IMD decile into deprivation tiers."""
    from src.constants import DeprivationConstants, TierLabels
    
    if pd.isna(imd_decile):
        return 'unknown'
    imd_decile = int(imd_decile)
    if imd_decile in DeprivationConstants.MOST_DEPRIVED_DECILES:
        return TierLabels.MOST_DEPRIVED
    elif imd_decile in DeprivationConstants.MODERATELY_DEPRIVED_DECILES:
        return TierLabels.MODERATELY_DEPRIVED
    elif imd_decile in DeprivationConstants.LEAST_DEPRIVED_DECILES:
        return TierLabels.LEAST_DEPRIVED
    else:
        return 'unknown'


def get_deprivation_tier(row):
    """Determine deprivation tier based on population percentages."""
    from src.constants import DeprivationConstants, TierLabels
    
    if pd.isna(row['pop%_most_deprived']):
        return 'Unknown'
    most = row['pop%_most_deprived']
    least = row['pop%_least_deprived']
    moderate = row['pop%_moderately_deprived']

    if (most >= DeprivationConstants.HIGH_DEPRIVATION_POPULATION and
        least < DeprivationConstants.LOW_DEPRIVATION_POPULATION):
        return TierLabels.HIGH_DEPRIVATION
    elif (least >= DeprivationConstants.LOW_DEPRIVATION_POPULATION and
          most < DeprivationConstants.HIGH_DEPRIVATION_POPULATION):
        return TierLabels.LOW_DEPRIVATION
    elif (moderate >= DeprivationConstants.MODERATE_DEPRIVATION_THRESHOLD):
        return TierLabels.MODERATE_DEPRIVATION
    else:
        return TierLabels.MODERATE_DEPRIVATION


def calculate_haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two points in kilometers."""
    R = 6371
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    return R * c


def get_outward_code(postcode: str) -> str:
    """Extract outward code (postcode district) from full postcode."""
    if pd.isna(postcode):
        return None
    canonical_pc = "".join(str(postcode).upper().split())
    return canonical_pc[:-3] if len(canonical_pc) > 3 else canonical_pc


def setup_routing_cache(cache_file: str = 'routing_cache.json') -> Dict:
    """Initialize or load routing cache."""
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            return json.load(f)
    return {}


def save_routing_cache(cache: Dict, cache_file: str = 'routing_cache.json'):
    """Save routing cache to file."""
    with open(cache_file, 'w') as f:
        json.dump(cache, f, indent=2)


def create_checkpoint(data: Any, checkpoint_path: str, stage_name: str) -> bool:
    """Save checkpoint for resumable pipeline."""
    try:
        os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
        
        if isinstance(data, pd.DataFrame):
            data.to_parquet(checkpoint_path, index=False)
        elif isinstance(data, dict):
            import joblib
            joblib.dump(data, checkpoint_path)
        else:
            with open(checkpoint_path, 'w') as f:
                json.dump(data, f, default=str)
        
        LOG = logging.getLogger("Brownsea_Equity_Analysis")
        LOG.info(f"Checkpoint saved: {stage_name} -> {checkpoint_path}")
        return True
    except Exception as e:
        LOG.error(f"Failed to save checkpoint {stage_name}: {e}")
        return False


def load_checkpoint(checkpoint_path: str) -> Optional[Any]:
    """Load checkpoint if it exists and is recent."""
    if not os.path.exists(checkpoint_path):
        return None
    
    try:
        if checkpoint_path.endswith('.parquet'):
            return pd.read_parquet(checkpoint_path)
        elif checkpoint_path.endswith('.joblib'):
            import joblib
            return joblib.load(checkpoint_path)
        else:
            with open(checkpoint_path, 'r') as f:
                return json.load(f)
    except Exception as e:
        LOG = logging.getLogger("Brownsea_Equity_Analysis")
        LOG.warning(f"Failed to load checkpoint {checkpoint_path}: {e}")
        return None


def get_timestamp() -> str:
    """Get formatted timestamp for file naming."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")