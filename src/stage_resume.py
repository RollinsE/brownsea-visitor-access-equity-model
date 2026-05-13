# -*- coding: utf-8 -*-
"""Helpers for controlled stage-specific reruns and resume builds.

These helpers deliberately read persisted artifacts from a previous build and do
not change modelling or routing logic. New runs still write to their own build
folder; the resume build is an input source only.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

LOG = logging.getLogger("Brownsea_Equity_Analysis")


STAGE_TITLES = {
    1: "Data pipeline",
    2: "Machine-learning modelling",
    3: "Strategic framework",
    4: "Strategic analysis",
    5: "Postcode lookup",
}


@dataclass(frozen=True)
class StagePlan:
    from_stage: int = 1
    to_stage: int = 5

    @property
    def stages(self) -> list[int]:
        return list(range(self.from_stage, self.to_stage + 1))

    def includes(self, stage: int) -> bool:
        return self.from_stage <= stage <= self.to_stage


class ResumeArtifactError(RuntimeError):
    """Raised when a requested rerun cannot be satisfied by persisted artifacts."""


def resolve_stage_plan(*, only_stage: Optional[int] = None, from_stage: int = 1, to_stage: int = 5) -> StagePlan:
    """Resolve and validate user supplied stage-run controls."""
    if only_stage is not None:
        from_stage = to_stage = int(only_stage)
    from_stage = int(from_stage)
    to_stage = int(to_stage)
    if from_stage < 1 or from_stage > 5 or to_stage < 1 or to_stage > 5:
        raise ValueError("Stages must be between 1 and 5")
    if from_stage > to_stage:
        raise ValueError("--from-stage cannot be greater than --to-stage")
    return StagePlan(from_stage=from_stage, to_stage=to_stage)


def require_resume_for_skipped_dependencies(plan: StagePlan, resume_build: str | None) -> None:
    """Require a resume build when the requested range skips upstream stages."""
    if plan.from_stage > 1 and not resume_build:
        raise ResumeArtifactError(
            "Stage-specific reruns starting after Stage 1 require --resume-build so upstream artifacts can be loaded."
        )
    if plan.from_stage == 5 and not resume_build:
        raise ResumeArtifactError("Stage 5-only reruns require --resume-build with Stage 1 and Stage 4 artifacts.")


def validate_resume_build(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    build = Path(path).expanduser().resolve()
    if not build.exists() or not build.is_dir():
        raise ResumeArtifactError(f"Resume build does not exist: {build}")
    if not (build / "artifacts").exists() and not (build / "checkpoints").exists():
        raise ResumeArtifactError(f"Resume build is missing artifacts/checkpoints directories: {build}")
    return build


def _read_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise ResumeArtifactError(f"Missing required {label}: {path}")
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise ResumeArtifactError(f"Could not read {label} at {path}: {exc}") from exc


def _read_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise ResumeArtifactError(f"Missing required {label}: {path}")
    try:
        return pd.read_csv(path)
    except Exception as exc:
        raise ResumeArtifactError(f"Could not read {label} at {path}: {exc}") from exc


def _load_joblib(path: Path, label: str) -> Any:
    if not path.exists():
        raise ResumeArtifactError(f"Missing required {label}: {path}")
    try:
        import joblib

        return joblib.load(path)
    except Exception as exc:
        raise ResumeArtifactError(f"Could not read {label} at {path}: {exc}") from exc


def load_stage1_outputs(resume_build: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, None, pd.DataFrame]:
    """Load persisted Stage 1 outputs needed by later stages.

    Returns the same tuple shape as execute_data_pipeline:
    (ml_dataset, lsoa_master_df, district_lsoa_map, school_df, ons_df).
    school_df is intentionally returned as None because later stages do not need it.
    """
    build = validate_resume_build(resume_build)
    assert build is not None
    checkpoint_dir = build / "checkpoints"
    artifact_dir = build / "artifacts"

    features_path = checkpoint_dir / "engineered_features.parquet"
    if features_path.exists():
        ml_dataset = _read_parquet(features_path, "engineered features checkpoint")
    else:
        ml_dataset = _read_csv(artifact_dir / "ml_ready_district_data.csv", "ML-ready data artifact")

    lsoa_master_df = _read_parquet(checkpoint_dir / "lsoa_master.parquet", "LSOA master checkpoint")
    district_lsoa_map = _read_parquet(checkpoint_dir / "district_lsoa_map.parquet", "district-LSOA map checkpoint")
    ons_df = _read_parquet(checkpoint_dir / "ons_clean.parquet", "ONS checkpoint")

    LOG.info("Loaded Stage 1 outputs from resume build: %s", build)
    return ml_dataset, lsoa_master_df, district_lsoa_map, None, ons_df


def load_model_bundle(resume_build: str | Path) -> tuple[pd.DataFrame, dict, dict, bool, pd.Series]:
    """Load persisted Stage 2 model bundle for Stage 4 reruns."""
    build = validate_resume_build(resume_build)
    assert build is not None
    bundle_path = build / "checkpoints" / "model_bundle.joblib"
    bundle = _load_joblib(bundle_path, "model bundle checkpoint")

    required = ["X", "best_model_info", "model_dict", "used_log_transform", "population"]
    missing = [key for key in required if key not in bundle]
    if missing:
        raise ResumeArtifactError(f"Model bundle is missing keys {missing}: {bundle_path}")

    LOG.info("Loaded Stage 2 model bundle from resume build: %s", bundle_path)
    return (
        bundle["X"],
        bundle["best_model_info"],
        bundle["model_dict"],
        bool(bundle["used_log_transform"]),
        bundle["population"],
    )


def load_stage4_outputs(resume_build: str | Path) -> pd.DataFrame:
    """Load persisted Stage 4 strategic analysis output for Stage 5-only reruns.

    New builds write the canonical three_way_intersection_analysis_v2.csv file.
    Older builds may only contain the same data as analysis_table.csv under
    artifacts/tables, so keep a read-only fallback to avoid forcing a full rerun.
    """
    build = validate_resume_build(resume_build)
    assert build is not None
    candidate_paths = [
        build / "artifacts" / "three_way_intersection_analysis_v2.csv",
        build / "artifacts" / "tables" / "analysis_table.csv",
        build / "artifacts" / "tables" / "district_analysis_export.csv",
    ]
    for analysis_path in candidate_paths:
        if analysis_path.exists():
            analysis_df = _read_csv(analysis_path, "three-way strategic analysis artifact")
            LOG.info("Loaded Stage 4 analysis from resume build: %s", analysis_path)
            return analysis_df
    searched = ", ".join(str(path) for path in candidate_paths)
    raise ResumeArtifactError(f"Missing Stage 4 analysis artifact. Searched: {searched}")




def validate_reference_inputs(config: dict, *, require_nt_sites: bool = False) -> None:
    """Validate only the lightweight reference files needed by resume-only stages."""
    if not require_nt_sites:
        return
    nt_path = Path(config.get("reference_paths", {}).get("nt_sites", ""))
    if not nt_path.exists():
        raise ResumeArtifactError(f"Stage 5 requires the NT sites reference file: {nt_path}")
    LOG.info("Reference validation summary: nt_sites found at %s", nt_path)

def write_resume_manifest(config: dict, *, plan: StagePlan, resume_build: Path | None) -> None:
    """Write a small manifest describing rerun controls into the current build."""
    path = Path(config.get("output_dir", ".")) / "stage_run_manifest.json"
    payload = {
        "from_stage": plan.from_stage,
        "to_stage": plan.to_stage,
        "stages": plan.stages,
        "resume_build": str(resume_build) if resume_build else None,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOG.info("Saved stage run manifest: %s", path)
