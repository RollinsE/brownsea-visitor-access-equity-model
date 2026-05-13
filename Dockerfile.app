# Lightweight Flask app image for serving a completed Brownsea release.
# This image does not run the ML pipeline. Mount or copy an outputs/ folder
# containing releases/latest at runtime.
FROM python:3.10-slim

WORKDIR /app

COPY requirements/app.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/app/
COPY src/__init__.py /app/src/__init__.py
COPY src/release_manager.py /app/src/release_manager.py
COPY run_postcode_app.py /app/run_postcode_app.py

ENV PYTHONPATH=/app
ENV BROWNSEA_OUTPUTS_ROOT=/app/outputs
EXPOSE 8000

CMD ["gunicorn", "app.gunicorn_app:app", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "120"]
