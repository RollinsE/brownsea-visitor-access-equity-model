OUTPUT_DIR ?= outputs
RELEASE_NAME ?= latest

.PHONY: help install install-colab install-prod install-app test qa-release smoke-app freeze-release check-freeze doctor launch-colab-app refresh-app-ui refresh-postcode-release export-static-app clean clean-builds clean-all run run-colab run-prod run-app docker-build docker-run docker-build-app docker-run-app

help:
	@echo "Brownsea Pipeline Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make install          Install all dependencies"
	@echo "  make install-colab    Install Colab-specific dependencies"
	@echo "  make install-prod     Install production dependencies"
	@echo "  make install-app      Install minimal Flask app dependencies"
	@echo "  make test             Run tests"
	@echo "  make qa-release       Validate outputs/releases/latest without rerunning pipeline"
	@echo "  make smoke-app        Smoke-test the postcode app against a release/build"
	@echo "  make freeze-release   Freeze current release candidate after QA passes"
	@echo "  make check-freeze     Check frozen release has not drifted"
	@echo "  make doctor           Inspect project/release/cache status without rerunning"
	@echo "  make launch-colab-app Print Colab app launch guidance"
	@echo "  make refresh-app-ui  Rebuild static app HTML from release JSON"
	@echo "  make refresh-postcode-release Rebuild postcode lookup artifacts inside a release"
	@echo "  make export-static-app Export docs/ for GitHub Pages"
	@echo "  make clean            Clean build/release files but preserve shared route cache"
	@echo "  make clean-builds     Clean build/release files but preserve shared route cache"
	@echo "  make clean-all        Clean all outputs including shared route cache"
	@echo "  make run              Run pipeline (auto-detect mode)"
	@echo "  make run-colab        Run pipeline in Colab mode"
	@echo "  make run-prod         Run pipeline in production mode"
	@echo "  make run-app          Run Flask postcode app locally"
	@echo "  make docker-build     Build pipeline Docker image"
	@echo "  make docker-build-app Build lightweight Flask app Docker image"
	@echo "  make docker-run       Run Docker container"

install:
	pip install -r requirements/colab.txt

install-colab:
	pip install -r requirements/colab.txt

install-prod:
	pip install -r requirements/prod.txt

install-app:
	pip install -r requirements/app.txt

test:
	pytest tests/ -v --tb=short

qa-release:
	python scripts/qa_release.py $(OUTPUT_DIR) --release-name $(RELEASE_NAME)

smoke-app:
	python scripts/smoke_app.py $(OUTPUT_DIR) --release-name $(RELEASE_NAME)

freeze-release:
	python scripts/freeze_release.py $(OUTPUT_DIR) --release-name $(RELEASE_NAME)

check-freeze:
	python scripts/freeze_release.py $(OUTPUT_DIR) --release-name $(RELEASE_NAME) --check

doctor:
	python scripts/doctor.py $(OUTPUT_DIR) --release-name $(RELEASE_NAME)

launch-colab-app:
	python scripts/launch_colab_app.py --outputs-root $(OUTPUT_DIR) --release-name $(RELEASE_NAME) --open-mode none

clean: clean-builds

clean-builds:
	rm -rf outputs/builds/
	rm -rf outputs/releases/
	rm -rf outputs/release_pointer.json
	rm -rf checkpoints/
	rm -rf __pycache__/
	rm -rf src/__pycache__/
	rm -rf tests/__pycache__/
	rm -rf .pytest_cache/
	rm -f routing_cache.json
	rm -f *.log

clean-all: clean-builds
	rm -rf outputs/cache/
	rmdir outputs 2>/dev/null || true

run:
	python cli.py

run-colab:
	python cli.py --mode colab

run-prod:
	python cli.py --mode production --output-dir ./outputs

run-app:
	python run_postcode_app.py --outputs-root $(OUTPUT_DIR) --port 8000

docker-build:
	docker build -t brownsea-pipeline:latest .

docker-run:
	docker run --rm \
		-v $(PWD)/data:/data:ro \
		-v $(PWD)/outputs:/app/outputs \
		-e ORS_API_KEY=$(ORS_API_KEY) \
		brownsea-pipeline:latest

refresh-app-ui:
	python scripts/refresh_app_ui.py $(OUTPUT_DIR) --release-name $(RELEASE_NAME)

refresh-postcode-release:
	python scripts/refresh_release_postcode_lookup.py $(OUTPUT_DIR) --release-name $(RELEASE_NAME) --qa

export-static-app:
	python scripts/export_static_staff_app.py $(OUTPUT_DIR) --release-name $(RELEASE_NAME) --target docs


docker-build-app:
	docker build -f Dockerfile.app -t brownsea-postcode-app:latest .

docker-run-app:
	docker run --rm -p 8000:8000 \
		-v $(PWD)/$(OUTPUT_DIR):/app/outputs:ro \
		-e BROWNSEA_OUTPUTS_ROOT=/app/outputs \
		brownsea-postcode-app:latest
