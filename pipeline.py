#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main pipeline orchestrator with build, checkpoint, and release support."""

import argparse
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
separator = "=" * 80

sys.path.insert(0, str(Path(__file__).parent))

from src.analysis_engine import (
    analyze_three_way_intersection,
    display_strategic_framework_definitions,
)
from src.config import (
    apply_output_directory,
    detect_runtime,
    init_environment,
    validate_runtime_configuration,
)
from src.constants import (
    INTERVENTION_CATEGORIES,
    NEED_TIER_DEFINITIONS,
    PRIORITY_MATRIX_CATEGORIES,
)
from src.data_pipeline import execute_data_pipeline
# Stage 2 modelling imports are intentionally lazy so `python cli.py --help`
# works even before optional ML dependencies are installed.
from src.postcode_lookup import build_postcode_lookup_artifacts
from src.reporting import build_reports_index
from src.release_manager import prepare_build_directory, promote_release, write_release_pointer, write_promoted_release_manifest
from src.stage_resume import (
    ResumeArtifactError,
    load_model_bundle,
    load_stage1_outputs,
    load_stage4_outputs,
    require_resume_for_skipped_dependencies,
    resolve_stage_plan,
    validate_resume_build,
    validate_reference_inputs,
    write_resume_manifest,
)
from src.utils import get_timestamp, setup_logging


def _stage_start(stage_no: int, title: str, log) -> None:
    message = f"Stage {stage_no}: {title}"
    print(f"\n{separator}")
    print(message.upper())
    print(separator)
    log.info(message)


def _stage_done(stage_no: int, title: str, log, **summary) -> None:
    clean_summary = {k: v for k, v in summary.items() if v is not None}
    suffix = ""
    if clean_summary:
        suffix = ": " + ", ".join(f"{k}={v}" for k, v in clean_summary.items())
    message = f"Stage {stage_no} complete: {title}{suffix}"
    print(message)
    log.info(message)


def _artifact_message(config: dict, log) -> None:
    artifact_dir = config.get("artifact_dir")
    report_dir = config.get("report_dir")
    route_cache_dir = config.get("route_cache_dir")
    print("\nArtifacts written:")
    for label, value in (
        ("build", config.get("output_dir")),
        ("artifacts", artifact_dir),
        ("reports", report_dir),
        ("shared route cache", route_cache_dir),
    ):
        if value:
            print(f"  - {label}: {value}")
            log.info("%s: %s", label, value)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Brownsea Island Visitor Analysis Pipeline")
    parser.add_argument("--mode", choices=["colab", "production", "local"], default=None,
                       help="Runtime mode (auto-detected if not specified)")
    parser.add_argument("--skip-checkpoints", action="store_true",
                       help="Skip loading from checkpoints")
    parser.add_argument("--output-dir", default="outputs",
                       help="Output root directory for builds and releases")
    parser.add_argument("--data-dir", default=None,
                       help="Override data directory (persistent path recommended)")
    parser.add_argument("--promote-release", action="store_true",
                       help="Promote successful build to releases/<release-name>")
    parser.add_argument("--release-name", default="latest",
                       help="Release name to promote on success")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    parser.add_argument("--show-plots", action="store_true",
                       help="Allow inline plot display for interactive notebook use. CLI runs are file-first by default.")
    parser.add_argument("--resume-build", default=None,
                       help="Load upstream artifacts/checkpoints from an existing build. Outputs still go to a new build.")
    parser.add_argument("--from-stage", type=int, default=1, choices=[1, 2, 3, 4, 5],
                       help="First stage to execute. Requires --resume-build when skipping upstream dependencies.")
    parser.add_argument("--to-stage", type=int, default=5, choices=[1, 2, 3, 4, 5],
                       help="Last stage to execute.")
    parser.add_argument("--only-stage", type=int, default=None, choices=[1, 2, 3, 4, 5],
                       help="Run one stage only. Equivalent to --from-stage N --to-stage N.")
    return parser


def write_run_manifest(
    config: dict,
    runtime: str,
    output_root: str | None = None,
    run_id: str | None = None,
    release_name: str | None = None,
    stage_plan=None,
    resume_build: Path | None = None,
) -> None:
    manifest_path = Path(config["output_files"]["run_manifest"])
    manifest = {
        "runtime": runtime,
        "data_directory": config.get("data_directory"),
        "output_dir": config.get("output_dir"),
        "artifact_dir": config.get("artifact_dir"),
        "report_dir": config.get("report_dir"),
        "checkpoint_dir": config.get("checkpoint_dir"),
        "selected_features": config.get("selected_features", []),
        "input_files": config.get("file_paths", {}),
        "reference_files": config.get("reference_paths", {}),
        "routing": {
            "profile": getattr(__import__("src.constants", fromlist=["RoutingConstants"]).RoutingConstants, "PROFILE", None),
            "competitor_shortlist_size": getattr(__import__("src.constants", fromlist=["RoutingConstants"]).RoutingConstants, "COMPETITOR_SHORTLIST_SIZE", None),
            "cache_version": getattr(__import__("src.constants", fromlist=["RoutingConstants"]).RoutingConstants, "CACHE_VERSION", None),
        },
        "generated_at": get_timestamp(),
        "output_root": output_root,
        "run_id": run_id,
        "release_name": release_name,
        "stage_plan": {
            "from_stage": getattr(stage_plan, "from_stage", 1),
            "to_stage": getattr(stage_plan, "to_stage", 5),
            "stages": getattr(stage_plan, "stages", [1, 2, 3, 4, 5]),
        },
        "resume_build": str(resume_build) if resume_build else None,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main(args: argparse.Namespace | None = None):
    parser = create_parser()
    if args is None:
        args = parser.parse_args()
    elif not isinstance(args, argparse.Namespace):
        args = parser.parse_args(args)

    try:
        stage_plan = resolve_stage_plan(
            only_stage=getattr(args, "only_stage", None),
            from_stage=getattr(args, "from_stage", 1),
            to_stage=getattr(args, "to_stage", 5),
        )
        resume_build = validate_resume_build(getattr(args, "resume_build", None))
        require_resume_for_skipped_dependencies(stage_plan, str(resume_build) if resume_build else None)
    except (ValueError, ResumeArtifactError) as exc:
        parser.error(str(exc))

    runtime = args.mode or detect_runtime()
    run_id = get_timestamp()
    output_root = Path(args.output_dir)
    build_dir = prepare_build_directory(output_root, run_id)

    log_dir = build_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"brownsea_pipeline_{run_id}.log"
    LOG = setup_logging(args.log_level, str(log_file))

    print("Brownsea pipeline run")
    print("-" * 80)
    print(f"  runtime: {runtime}")
    print(f"  build:   {build_dir}")
    print(f"  stages:  {stage_plan.from_stage}-{stage_plan.to_stage}")
    if resume_build:
        print(f"  resume:  {resume_build}")
    LOG.info("Starting Brownsea Island Visitor Analysis Pipeline")
    LOG.info(f"Runtime mode: {runtime}")
    LOG.info(f"Output root: {output_root}")
    LOG.info(f"Build directory: {build_dir}")
    LOG.info("Stage plan: %s-%s", stage_plan.from_stage, stage_plan.to_stage)
    if resume_build:
        LOG.info("Resume build: %s", resume_build)

    try:
        if args.data_dir:
            os.environ["BROWSEA_DATA_DIR"] = args.data_dir
        config = init_environment(runtime)
        config = apply_output_directory(config, str(build_dir))
        config["output_root"] = str(output_root)
        config["run_id"] = run_id
        config["enable_inline_display"] = bool(getattr(args, "show_plots", False))
        if stage_plan.includes(1):
            validate_runtime_configuration(config)
        else:
            validate_reference_inputs(config, require_nt_sites=stage_plan.includes(5))
        write_run_manifest(
            config,
            runtime,
            output_root=str(output_root),
            run_id=run_id,
            release_name=(args.release_name if args.promote_release else None),
            stage_plan=stage_plan,
            resume_build=resume_build,
        )
        write_resume_manifest(config, plan=stage_plan, resume_build=resume_build)

        ml_dataset = lsoa_master_df = district_lsoa_map = school_df = ons_df = None
        X = best_model_info = model_dict = used_log_transform = population = None
        processed_analysis_df = None

        if stage_plan.includes(1):
            _stage_start(1, "Data pipeline", LOG)
            ml_dataset, lsoa_master_df, district_lsoa_map, school_df, ons_df = execute_data_pipeline(
                config, skip_checkpoints=args.skip_checkpoints
            )
            _stage_done(1, "Data pipeline", LOG, rows=(len(ml_dataset) if ml_dataset is not None else 0))
        elif resume_build and (stage_plan.includes(2) or stage_plan.includes(4) or stage_plan.includes(5)):
            print(f"Loading Stage 1 outputs from: {resume_build}")
            ml_dataset, lsoa_master_df, district_lsoa_map, school_df, ons_df = load_stage1_outputs(resume_build)

        if stage_plan.includes(2):
            if ml_dataset is None:
                raise RuntimeError("Stage 2 requires Stage 1 data. Use --resume-build or include Stage 1.")
            _stage_start(2, "Machine-learning modelling", LOG)
            from src.model_training import execute_modeling_pipeline

            X, best_model_info, model_dict, used_log_transform, population = execute_modeling_pipeline(
                ml_dataset, config
            )
            _stage_done(2, "Machine-learning modelling", LOG, best_model=(best_model_info or {}).get("name"), features=(len(X.columns) if X is not None else 0))
        elif resume_build and stage_plan.includes(4):
            print(f"Loading Stage 2 model bundle from: {resume_build}")
            X, best_model_info, model_dict, used_log_transform, population = load_model_bundle(resume_build)

        if stage_plan.includes(3):
            _stage_start(3, "Strategic framework", LOG)
            display_strategic_framework_definitions(
                PRIORITY_MATRIX_CATEGORIES, INTERVENTION_CATEGORIES, NEED_TIER_DEFINITIONS, config
            )
            _stage_done(3, "Strategic framework", LOG)

        if stage_plan.includes(4):
            _stage_start(4, "Strategic analysis", LOG)
            if ml_dataset is not None and best_model_info is not None and X is not None:
                processed_analysis_df = analyze_three_way_intersection(
                    ml_dataset,
                    best_model_info,
                    X,
                    used_log_transform,
                    population,
                    lsoa_master_df,
                    district_lsoa_map,
                    config,
                )
                _stage_done(4, "Strategic analysis", LOG, districts=(len(processed_analysis_df) if processed_analysis_df is not None else 0))
            else:
                raise RuntimeError("Stage 4 requires Stage 1 data and Stage 2 model bundle. Use --resume-build or include earlier stages.")
        elif resume_build and stage_plan.includes(5):
            print(f"Loading Stage 4 analysis from: {resume_build}")
            processed_analysis_df = load_stage4_outputs(resume_build)

        if stage_plan.includes(5):
            if ons_df is None or lsoa_master_df is None or processed_analysis_df is None:
                raise RuntimeError("Stage 5 requires ONS, LSOA, and Stage 4 analysis artifacts. Use --resume-build or include earlier stages.")
            _stage_start(5, "Postcode lookup", LOG)
            postcode_lookup_df = build_postcode_lookup_artifacts(ons_df, lsoa_master_df, processed_analysis_df, config)
            build_reports_index(config)
            _stage_done(5, "Postcode lookup", LOG, rows=len(postcode_lookup_df))
        else:
            build_reports_index(config)

        if args.promote_release:
            release_dir = promote_release(build_dir, output_root, args.release_name)
            pointer = write_release_pointer(output_root, args.release_name, run_id)
            release_manifest = write_promoted_release_manifest(
                release_dir,
                output_root=output_root,
                release_name=args.release_name,
                run_id=run_id,
                source_build=build_dir,
                route_cache_dir=config.get("route_cache_dir"),
            )
            print(f"Release promoted: {release_dir}")
            print(f"Release manifest: {release_manifest}")
            LOG.info(f"Promoted build to release: {release_dir}")
            LOG.info(f"Updated release pointer: {pointer}")
            LOG.info(f"Wrote release manifest: {release_manifest}")

        _artifact_message(config, LOG)
        print(f"\n{separator}")
        print("Pipeline completed successfully")

    except Exception as exc:
        LOG.error(f"Pipeline failed: {exc}", exc_info=True)
        LOG.error("Pipeline terminated")
        sys.exit(1)


if __name__ == "__main__":
    main()
