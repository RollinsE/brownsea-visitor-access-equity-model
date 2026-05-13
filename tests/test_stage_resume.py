from pathlib import Path

import pandas as pd
import pytest

from src.stage_resume import (
    ResumeArtifactError,
    load_stage1_outputs,
    load_stage4_outputs,
    resolve_stage_plan,
    require_resume_for_skipped_dependencies,
    validate_resume_build,
)


def test_resolve_only_stage_collapses_range():
    plan = resolve_stage_plan(only_stage=5, from_stage=1, to_stage=4)
    assert plan.from_stage == 5
    assert plan.to_stage == 5
    assert plan.stages == [5]


def test_invalid_stage_range_raises():
    with pytest.raises(ValueError):
        resolve_stage_plan(from_stage=4, to_stage=2)


def test_resume_required_when_skipping_upstream_stages():
    plan = resolve_stage_plan(from_stage=4, to_stage=5)
    with pytest.raises(ResumeArtifactError):
        require_resume_for_skipped_dependencies(plan, None)


def test_load_stage_outputs_from_resume_build(tmp_path):
    build = tmp_path / "outputs" / "builds" / "run_001"
    checkpoints = build / "checkpoints"
    artifacts = build / "artifacts"
    checkpoints.mkdir(parents=True)
    artifacts.mkdir(parents=True)

    ml = pd.DataFrame({"District": ["BH1"], "Population": [100], "Visits": [5]})
    lsoa = pd.DataFrame({"lsoa21cd": ["E1"], "district": ["BH1"]})
    dmap = pd.DataFrame({"District": ["BH1"], "lsoa21cd": ["E1"]})
    ons = pd.DataFrame({"pcds": ["BH1 1AA"], "lsoa21cd": ["E1"]})
    analysis = pd.DataFrame({"District": ["BH1"], "priority_zone": ["Monitor"]})

    # CSV fallback for ML-ready data is enough for later-stage reruns.
    ml.to_csv(artifacts / "ml_ready_district_data.csv", index=False)
    try:
        lsoa.to_parquet(checkpoints / "lsoa_master.parquet", index=False)
        dmap.to_parquet(checkpoints / "district_lsoa_map.parquet", index=False)
        ons.to_parquet(checkpoints / "ons_clean.parquet", index=False)
    except Exception:
        pytest.skip("parquet engine unavailable")
    analysis.to_csv(artifacts / "three_way_intersection_analysis_v2.csv", index=False)

    assert validate_resume_build(build) == build.resolve()
    loaded_ml, loaded_lsoa, loaded_dmap, school_df, loaded_ons = load_stage1_outputs(build)
    loaded_analysis = load_stage4_outputs(build)

    assert school_df is None
    assert loaded_ml.shape[0] == 1
    assert loaded_lsoa.shape[0] == 1
    assert loaded_dmap.shape[0] == 1
    assert loaded_ons.shape[0] == 1
    assert loaded_analysis.loc[0, "priority_zone"] == "Monitor"


def test_load_stage4_outputs_accepts_alias_analysis_table(tmp_path):
    build = tmp_path / "outputs" / "builds" / "run_alias"
    alias_dir = build / "artifacts" / "tables"
    alias_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"District": ["BH2"], "priority_zone": ["Watch"]}).to_csv(
        alias_dir / "analysis_table.csv", index=False
    )

    loaded = load_stage4_outputs(build)

    assert loaded.loc[0, "District"] == "BH2"
    assert loaded.loc[0, "priority_zone"] == "Watch"
