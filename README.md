# Brownsea Visitor Access & Equity Model

A postcode-level decision-support tool for Brownsea Island that highlights access, equity, and outreach opportunities. It combines journey modelling, deprivation and local context, ML-based expected visit rates, National Trust comparisons, reports, and a staff-facing web app. The hosted version also has an AI assistant that answers plain-English questions about the results.

**Live app with AI assistant:** https://brownsea-visitor-access.onrender.com

**Static version (no assistant):** https://rollinse.github.io/brownsea-visitor-access-equity-model/

The first link runs on a free hosting plan and goes to sleep when nobody is using it, so the first load can take about a minute.

**About this version.** This is a public, data-safe copy of a tool I built for the National Trust. It runs on the same code as the original, but it contains no visitor or member records. Everything shown here is summarised at postcode-district level, so no individual or household can be identified. Shared with the National Trust's agreement.

---

## Overview

This project analyses visitor access to Brownsea Island across BH, DT, and SP postcode areas. It combines local socioeconomic context, journey estimates, National Trust competitor comparisons, and machine learning outputs to identify areas where engagement may be lower than expected.

The project includes a reproducible data pipeline, release QA tooling, and two app delivery options:

1. A static staff-facing app for GitHub Pages
2. A Flask app, hosted on Render, which adds the AI assistant

The GitHub Pages app allows non-technical users to search a postcode, view Brownsea access context, compare nearby National Trust alternatives, and download reports.

## Key features

* End-to-end data pipeline for Brownsea visitor access analysis
* Feature engineering from postcode, deprivation, education, population, and journey-time data
* Machine learning workflow for expected visit-rate modelling
* National Trust competitor comparison
* Shared ORS route cache across builds
* Stage-specific reruns and release promotion
* Release QA, smoke testing, and freeze checks
* Static GitHub Pages app for non-technical users
* Flask app for local demos and the hosted version
* AI assistant (Google Gemini) that answers questions using read-only tools over the published results
* Reports, plots, downloads, and help definitions

## Pipeline stages

The pipeline runs in five stages:

1. **Data pipeline and feature engineering**
2. **Model training, tuning, and ensemble selection**
3. **Strategic framework definitions**
4. **Strategic analysis and reporting artifacts**
5. **Postcode lookup artifacts for BH, DT, and SP postcodes**

The workflow validates required inputs and feature contracts before training and prediction.

## Repository layout

```text
brownsea_pipeline/
├── app/                 # Flask app templates and server
├── data/reference/      # Public reference data
├── docs/                # Static staff app for GitHub Pages
├── notebooks/           # Colab/helper notebooks
├── requirements/        # Dependency files
├── scripts/             # QA, release, export, and utility scripts
├── src/                 # Pipeline and app source code
│   └── agent/           # AI assistant: tools, prompt, and LLM connections
├── tests/               # Regression and smoke tests
├── cli.py
├── pipeline.py
├── render.yaml          # Hosting config for Render
├── run_postcode_app.py
└── README.md
```

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

## Web app

The web app supports postcode lookup for BH, DT, and SP postcodes.

A user can view:

* matched postcode and postcode district
* local authority and region
* deprivation and local context
* Brownsea departure terminal
* estimated journey to departure terminal
* Brownsea ferry crossing allowance
* total Brownsea journey time
* nearest competing National Trust site
* observed and model-expected visit rates
* district-level assessment
* reports and downloads

The app is intended for planning and decision support, not live journey planning.

## Static app for GitHub Pages

The static app is the free staff-facing version of the tool. It does not require Python, Colab, or a running server.

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

## Flask app

The Flask app can be used locally, in Colab, or on a server.

Install the lightweight app dependencies:

```bash
pip install -r requirements/app.txt
```

Run the app:

```bash
python run_postcode_app.py --port 8000
```

If there is no `outputs/` folder, the app uses the release already published in `docs/`, so it works straight after cloning. To run it against your own pipeline outputs:

```bash
python run_postcode_app.py --outputs-root outputs --port 8000
```

Then open:

```text
http://localhost:8000
```

In Colab, launch the app through the Colab port proxy after starting the server.

## AI assistant

The Flask app has an "Ask about the analysis" box. You can type a question such as "Which districts need urgent action and are under 40 minutes from Brownsea?" and get a short written answer.

![The assistant answering a question about urgent action districts](assets/assistant.png)

### How it works

The assistant is an LLM agent. It does not answer from memory. For each question it picks from a small set of read-only tools, the tools look up the published results, and the model writes its answer from what they return.

| Tool | Reads |
|---|---|
| `lookup_postcode` | the postcode lookup |
| `get_district`, `query_districts`, `aggregate_districts` | `three_way_intersection_analysis_v2.csv` |
| `get_model_performance` | `model_performance.csv`, `model_performance_summary.json` |
| `get_definitions` | the framework tables in `reports/tables/` |

A few design choices:

* Counts, totals and averages are worked out in Python by the tools, not by the model.
* The tools only read. Nothing in the dataset, the features or the trained model is changed.
* The assistant never sees visitor or member records, only the published postcode and district outputs.
* Each answer lists which data it used.

### Running it yourself

The assistant uses Google's Gemini API. You need a free key from https://aistudio.google.com/apikey. Without a key the app still runs, just without the question box.

```bash
pip install -r requirements/app.txt
export GEMINI_API_KEY=your-key
python run_postcode_app.py
```

On Windows, set the key with `$env:GEMINI_API_KEY="your-key"` in PowerShell or `set GEMINI_API_KEY=your-key` in Command Prompt.

You can also ask a question from the command line, or run the check that compares the assistant's answers with values calculated directly from the data:

```bash
python -m src.agent --show-tools "How many districts are in each priority zone?"
python scripts/eval_agent.py
```

### Limits and settings

Gemini's free limits are set per model and can be low. When I set this up, `gemini-3.8-flash` allowed 20 requests a day, and each question uses at least two. If a model is over its limit or too busy, the assistant moves on to the next model in the list.

| Setting | Default | Purpose |
|---|---|---|
| `BROWNSEA_AGENT_MODEL` | `gemini-3.8-flash` | First model to try |
| `BROWNSEA_AGENT_FALLBACK_MODELS` | `gemini-3.7-flash,gemini-3.6-flash,gemini-3.5-flash,gemini-3.5-flash-lite` | Models to fall back to, in order |
| `BROWNSEA_AGENT_RATE_PER_MIN` | `10` | Questions allowed per client per minute |
| `BROWNSEA_AGENT_ENABLED` | `1` | Set to `0` to switch the assistant off |

Questions and the results needed to answer them are sent to Google. On the free tier, Google may use that content to improve its products.

The static GitHub Pages app cannot include the assistant, because it has no server to keep the API key private.

### Hosting

The hosted version runs on Render's free plan. `render.yaml` holds the setup: in Render choose **New > Blueprint**, pick this repository, and enter `GEMINI_API_KEY` when asked. Every push to `main` redeploys it.

## Colab quick start

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

## Notebook viewing

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

## Colab app launch

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

## Data and privacy

The original tool was built using private visitor and member data. None of that data is in this repository or in either live app. The public version only includes the summarised outputs of the analysis, public reference data, and the code. The AI assistant reads those same summarised outputs and has no access to anything else.

Private visitor/member data, raw postcode datasets, route caches, model checkpoints, and runtime outputs are excluded from the public repository.

Do not commit:

```text
.env
outputs/
private visitor/member files
raw/intermediate data
route caches
model checkpoints
logs
```

The public repository includes code, tests, documentation, public reference data, and the exported static app.

## Data availability

This repository does not include the full raw datasets used to run the pipeline. Some source datasets are publicly available from UK government or open data portals, while visitor/member data is private and excluded.

The public repository includes the code, tests, documentation, public reference data, and exported static app artifacts needed to demonstrate the project. Users who want to reproduce the full pipeline should obtain the required source datasets from their official providers and place them in the expected `data/` structure.

## Testing

Run the test suite with:

```bash
pytest
```

## License

This project code is licensed under the MIT License. Private/raw datasets are not included in this repository and are not covered by this license.

## Project status

This repository is structured for portfolio demonstration, reproducible analysis, and staff-facing access through either GitHub Pages or Flask.

```
```
