# Brownsea Island Visitor Analysis Pipeline

A professional Python pipeline for Brownsea Island membership analysis that runs cleanly in Colab and produces browser-friendly postcode lookup outputs.

## What this pipeline does

It processes the required raw inputs through five stages:

1. Stage 1: data pipeline and feature engineering
2. Stage 2: model training, tuning, and hybrid ensemble selection
3. Stage 3: strategic framework definitions
4. Stage 4: strategic analysis and reporting artifacts
5. Stage 5: postcode lookup artifacts for BH / DT / SP postcodes

The pipeline uses a fixed selected feature set and a controlled model-training workflow.

## Repository layout

```text
brownsea_pipeline/
├── src/
├── requirements/
├── tests/
├── pipeline.py
├── cli.py
├── Makefile
├── Dockerfile
└── README.md
```

## Colab quick start

### 1. Put the repo and your data in the Colab runtime

Expected data folder:

```text
/content/brownsea_pipeline/data/
```

### 2. Install dependencies

```python
!pip install -r requirements/colab.txt
```

### 3. Set your ORS key

```python
import os
os.environ["ORS_API_KEY"] = "YOUR_KEY_HERE"
```

### 4. Run the pipeline

```python
!python cli.py --mode colab --output-dir /content/outputs
```

## Local / production run

```bash
python cli.py --mode local --output-dir ./outputs
python cli.py --mode production --output-dir ./outputs
```

## Outputs

The pipeline writes to a structured output directory:

```text
outputs/
├── logs/
├── checkpoints/
├── artifacts/
└── reports/
```

Important artifacts:

- `artifacts/ml_ready_district_data.csv`
- `artifacts/postcode_lookup.csv`
- `artifacts/postcode_lookup.parquet`
- `reports/postcode_lookup.html`
- `run_manifest.json`

## Postcode lookup output

The postcode lookup artifact is designed for non-technical users. A user can enter a BH / DT / SP postcode and get:

- matched postcode and district
- authority and region
- LSOA deprivation context and FSM context
- Brownsea departure terminal
- drive time to departure terminal
- Brownsea crossing time
- total Brownsea journey time
- nearest competing NT site and time comparison
- district visit rate
- district predicted visit rate
- priority zone
- intervention type
- strategic narrative

## Hard validation rules

The pipeline now fails fast if:

- required source files are missing
- the ORS API key is missing
- Stage 2 selected features are missing
- prediction inputs do not match the trained feature contract

This is deliberate. Silent zero-filling of missing modeling features is not allowed.


## Editable install

```bash
cd brownsea_pipeline_pro
pip install -e .
```


## Web UI MVP

The pipeline now generates a static postcode search app:

- `reports/postcode_app.html`
- `artifacts/postcode_lookup.json`
- `artifacts/postcode_lookup.csv`

This app is backed by precomputed lookup data and is intended for non-technical users.


## National Trust competitor reference

The pipeline now uses `data/reference/nt_sites.csv` as the maintained competitor reference for routing and postcode lookup. Brownsea remains the target anchor; competitors are loaded from this CSV rather than hardcoded in Python.


## Run the postcode web app

After the pipeline has generated `outputs/artifacts/postcode_lookup.json`, run:

```bash
python run_postcode_app.py --lookup outputs/artifacts/postcode_lookup.json --outputs-root outputs --port 8000
```

Then open `http://localhost:8000/`.


## Route cache

The pipeline persists ORS routing results under the shared cache directory `outputs/cache/route_cache/` using separate Brownsea and competitor cache files. Timestamped builds remain separate under `outputs/builds/<run_id>/`, while ORS route results are reused across builds. Route caches are invalidated only for compatibility changes such as route scope, ORS profile, or cache schema version.


## Build and release workflow

The pipeline now writes each run into a timestamped build directory under `outputs/builds/<run_id>/`.

To promote a successful run to a stable app/data release, use:

```bash
python cli.py --mode colab --data-dir "/content/drive/MyDrive/brownsea/data" --output-dir "/content/drive/MyDrive/brownsea/outputs" --promote-release
```

This updates `outputs/releases/latest/`, which is the default place the lightweight app reads from.

Run the app against the latest release with:

```bash
python run_postcode_app.py --outputs-root "/content/drive/MyDrive/brownsea/outputs" --port 8000
```

## Controlled reruns and notebook viewing

Pipeline execution is file-first. Run the CLI to generate artifacts, then use the
separate notebook viewer to display saved outputs.

### Stage-specific reruns

All stage-specific reruns write to a fresh build directory and load upstream
inputs from a previous build supplied with `--resume-build`.

Examples:

```bash
# Rerun Stage 5 only from a previous full build
python cli.py --only-stage 5 --resume-build outputs/builds/<run_id>

# Rerun Stage 4 and Stage 5 from a previous build
python cli.py --from-stage 4 --to-stage 5 --resume-build outputs/builds/<run_id>

# Rerun Stage 2 onward using Stage 1 artifacts from a previous build
python cli.py --from-stage 2 --resume-build outputs/builds/<run_id>
```

Stage 4 reruns require a model bundle checkpoint. New runs save this at:

```text
outputs/builds/<run_id>/checkpoints/model_bundle.joblib
```

### Notebook viewing after a run

Open:

```text
notebooks/01_view_saved_outputs.ipynb
```

or call the helper directly:

```python
from src.notebook_viewer import display_saved_outputs

display_saved_outputs('/content/drive/MyDrive/brownsea/outputs', release_name='latest')
```

This keeps `!python cli.py` clean and avoids mixing pipeline execution with
notebook rendering.

## Release-candidate QA without rerunning the pipeline

After one full successful run and release promotion, validate the promoted release without rerunning any pipeline stage:

```bash
python scripts/qa_release.py outputs --release-name latest
```

or:

```bash
make qa-release OUTPUT_DIR=outputs RELEASE_NAME=latest
```

The QA command checks that the release has the app-critical postcode lookup files, reports, run manifest, and strategic analysis artifacts. It also reports recommended files for reruns and auditability, such as the model bundle checkpoint and model performance outputs.

To write or refresh the auditable release manifest manually:

```bash
python scripts/qa_release.py outputs --release-name latest --write-manifest
```

A promoted release created by `python cli.py --promote-release` automatically writes:

```text
outputs/releases/latest/release_manifest.json
```

This manifest records the source build, run id, route cache path, QA status, and file hashes for the release contents.

## App smoke test and Colab launch

Before opening the Flask app, smoke-test the promoted release without rerunning the pipeline:

```bash
python scripts/smoke_app.py outputs --release-name latest
```

In Colab with Drive outputs:

```python
!python scripts/smoke_app.py /content/drive/MyDrive/brownsea/outputs --release-name latest
```

Expected result:

```text
App smoke test: PASS
```

To launch the app in Colab, use the helper notebook:

```text
notebooks/02_launch_app_colab.ipynb
```

or run this in a Colab cell:

```python
from src.colab_app import launch_postcode_app

launch_postcode_app(
    outputs_root="/content/drive/MyDrive/brownsea/outputs",
    port=8000,
    open_mode="window",
)
```

If the browser blocks the popup, run:

```python
from google.colab import output
output.serve_kernel_port_as_iframe(8000, height=900)
```

The direct Flask URLs printed by the server, such as `127.0.0.1:8000`, are internal to the Colab runtime. Use the Colab proxy window or iframe instead.

## Freeze and project doctor checks

Once release QA and the app smoke test pass, freeze the release candidate without rerunning the pipeline:

```bash
python scripts/freeze_release.py outputs --release-name latest
```

In Colab with Drive outputs:

```python
!python scripts/freeze_release.py /content/drive/MyDrive/brownsea/outputs --release-name latest
```

This writes:

```text
outputs/releases/latest/release_lock.json
```

The lock records file hashes for the release candidate. It does not make Google Drive read-only, but it lets you check whether anything has changed since freeze:

```bash
python scripts/freeze_release.py outputs --release-name latest --check
```

Use the project doctor for a quick status view without rerunning the pipeline:

```bash
python scripts/doctor.py outputs --release-name latest
```

In Colab:

```python
!python scripts/doctor.py /content/drive/MyDrive/brownsea/outputs --release-name latest
```

The doctor reports release QA status, freeze status, release pointer status, and shared route-cache counts.

Recommended final release-candidate sequence:

```python
!python scripts/qa_release.py /content/drive/MyDrive/brownsea/outputs --release-name latest
!python scripts/smoke_app.py /content/drive/MyDrive/brownsea/outputs --release-name latest
!python scripts/freeze_release.py /content/drive/MyDrive/brownsea/outputs --release-name latest
!python scripts/doctor.py /content/drive/MyDrive/brownsea/outputs --release-name latest
```

---

## Portfolio and deployment options

This repository supports two app delivery modes:

1. **Static staff app for GitHub Pages** - free, no server required.
2. **Flask app** - useful for local demos, internal servers, or later cloud deployment.

### What should not be committed

The repo is configured to ignore private/runtime files such as:

- `.env` and API keys
- `outputs/`
- raw/intermediate/private data folders
- model checkpoints such as `*.joblib`
- route caches and logs

Before pushing to GitHub, check:

```bash
git status
```

Do not commit private visitor/member data or ORS API keys.

## Export the free static staff app

After a release has passed QA, export a GitHub Pages-ready static app to `docs/`:

```bash
python scripts/export_static_staff_app.py outputs --release-name latest --target docs
```

In Colab, use your Drive outputs path:

```python
!python scripts/export_static_staff_app.py /content/drive/MyDrive/brownsea/outputs --release-name latest --target docs
```

This writes:

```text
docs/
  index.html
  downloads.html
  reports/
  artifacts/
  reports.zip
  README_STAFF_APP.md
```

Commit the `docs/` folder, then enable GitHub Pages in the repository settings:

```text
Settings > Pages > Deploy from a branch > main > /docs
```

Staff can then use the GitHub Pages URL as the web app. They do not need Python, Colab, or the pipeline.

Shortcut:

```bash
make export-static-app OUTPUT_DIR=outputs RELEASE_NAME=latest
```

## Run the Flask app locally

Install the lightweight app dependencies:

```bash
pip install -r requirements/app.txt
```

Run the Flask app against a completed outputs folder:

```bash
python run_postcode_app.py --outputs-root outputs --port 8000
```

Then open:

```text
http://localhost:8000
```

Shortcut:

```bash
make run-app OUTPUT_DIR=outputs
```

## Run the Flask app with Docker

Build the lightweight app image:

```bash
docker build -f Dockerfile.app -t brownsea-postcode-app:latest .
```

Run it with a completed outputs folder mounted read-only:

```bash
docker run --rm -p 8000:8000 \
  -v "$PWD/outputs:/app/outputs:ro" \
  -e BROWNSEA_OUTPUTS_ROOT=/app/outputs \
  brownsea-postcode-app:latest
```

Then open:

```text
http://localhost:8000
```

Shortcut:

```bash
make docker-build-app
make docker-run-app OUTPUT_DIR=outputs
```

## Recommended final portfolio workflow

```bash
python scripts/qa_release.py outputs --release-name latest
python scripts/smoke_app.py outputs --release-name latest
python scripts/freeze_release.py outputs --release-name latest
python scripts/export_static_staff_app.py outputs --release-name latest --target docs
```

Then commit and push the repository, including `docs/`, but excluding private data and runtime outputs.
