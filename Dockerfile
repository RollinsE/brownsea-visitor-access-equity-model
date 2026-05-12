# Multi-stage build for production deployment
FROM python:3.10-slim as base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements/prod.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/
COPY pipeline.py /app/
COPY cli.py /app/

# Create non-root user
RUN useradd -m -u 1000 brownsea && chown -R brownsea:brownsea /app
USER brownsea

# Set environment variables
ENV PYTHONPATH=/app
ENV BROWSEA_ENV=production

# Default command
ENTRYPOINT ["python", "/app/cli.py"]
CMD ["--mode", "production"]