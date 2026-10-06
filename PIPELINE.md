# Running the pipeline

Notes for rebuilding the analysis from source data. The full pipeline needs the private visit data, so it cannot be run from the public repository alone. To run the app on the published results, see the README.

## Colab setup

Mount Google Drive, install dependencies, set the ORS API key, and run the pipeline:

```python
from google.colab import drive
drive.mount("/content/drive")
```

```python
%cd /content/brownsea_pipeline
!pip install -r requirements/colab.txt
```

```python
import os
os.environ["ORS_API_KEY"] = "YOUR_KEY_HERE"
```

## Run the full pipeline

The main pipeline entrypoint is `cli.py`.

For Colab with Google Drive outputs:

```python
!python cli.py --mode colab \
  --data-dir /content/brownsea_pipeline/data \
  --output-dir /content/drive/MyDrive/brownsea/outputs \
  --promote-release
```

For a local run:

```bash
python cli.py --mode local \
  --data-dir data \
  --output-dir outputs \
  --promote-release
```

A successful run creates a timestamped build under `outputs/builds/`. When `--promote-release` is used, the completed build is promoted to:

```text
outputs/releases/latest/
```

The app and release QA tools read from this promoted release.

## Outputs

Pipeline runs are written to timestamped build folders:

```text
outputs/
├── builds/<run_id>/
│   ├── artifacts/
│   ├── checkpoints/
│   ├── reports/
│   └── run_manifest.json
├── cache/
│   └── route_cache/
├── releases/
│   └── latest/
└── release_pointer.json
```

Each build keeps its own artifacts, reports, checkpoints, and manifest. The ORS route cache is shared across builds so route work is not repeated unnecessarily.

Typical release artifacts include:

```text
artifacts/postcode_lookup.csv
artifacts/postcode_shards/
artifacts/model_performance.csv
artifacts/model_performance_summary.json
artifacts/three_way_intersection_analysis_v2.csv
reports/index.html
reports/postcode_lookup.html
reports/model_performance.html
release_manifest.json
run_manifest.json
```

## Release QA

After a successful promoted run, validate the release without rerunning the pipeline:

```bash
python scripts/qa_release.py outputs --release-name latest
python scripts/smoke_app.py outputs --release-name latest
python scripts/freeze_release.py outputs --release-name latest
python scripts/doctor.py outputs --release-name latest
```

In Colab:

```python
!python scripts/qa_release.py /content/drive/MyDrive/brownsea/outputs --release-name latest
!python scripts/smoke_app.py /content/drive/MyDrive/brownsea/outputs --release-name latest
!python scripts/freeze_release.py /content/drive/MyDrive/brownsea/outputs --release-name latest
!python scripts/doctor.py /content/drive/MyDrive/brownsea/outputs --release-name latest
```

The QA scripts check release completeness, app readiness, release freeze status, and shared route-cache status without rerunning the main pipeline.

## Stage-specific reruns

Stage reruns write to a fresh build directory and load upstream inputs from a previous build or release.

```bash
python cli.py --only-stage 5 --resume-build outputs/releases/latest
python cli.py --from-stage 4 --to-stage 5 --resume-build outputs/releases/latest
python cli.py --from-stage 2 --resume-build outputs/builds/<run_id>
```

Stage 4 reruns require a model bundle checkpoint:

```text
outputs/builds/<run_id>/checkpoints/model_bundle.joblib
```

## Viewing saved outputs

Pipeline execution is file-first. The CLI saves reports, figures, CSVs, and app artifacts rather than trying to display notebook output during execution.

To view saved outputs after a run, open:

```text
notebooks/01_view_saved_outputs.ipynb
```

or call:

```python
from src.notebook_viewer import display_saved_outputs

display_saved_outputs("/content/drive/MyDrive/brownsea/outputs", release_name="latest")
```

## Exporting the static app

After a release has passed QA, export the static app to `docs/`:

```bash
python scripts/export_static_staff_app.py outputs --release-name latest --target docs
```

In Colab:

```python
!python scripts/export_static_staff_app.py /content/drive/MyDrive/brownsea/outputs --release-name latest --target docs
```

Then publish from GitHub Pages:

```text
Settings > Pages > Deploy from a branch > main > /docs
```

## Launching the Flask app in Colab

To launch the Flask app in Colab, use:

```python
from src.colab_app import launch_postcode_app

launch_postcode_app(
    outputs_root="/content/drive/MyDrive/brownsea/outputs",
    port=8000,
    open_mode="window",
)
```

If the browser blocks the popup, use:

```python
from google.colab import output
output.serve_kernel_port_as_iframe(8000, height=900)
```

The direct Flask URLs printed by the server, such as `127.0.0.1:8000`, are internal to the Colab runtime. Use the Colab proxy window or iframe instead.
