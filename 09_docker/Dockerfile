# ============================================================
# FILE: 09_docker/Dockerfile
# PURPOSE: Package the HR ETL pipeline into a Docker container
# ============================================================

# Base image — Python 3.11 slim (smaller size than full image)
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Why copy requirements first before code?
# Docker caches layers — if requirements don't change,
# it won't reinstall packages on every build. Faster builds.
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL pipeline code into container
COPY 02_etl_pipeline/ ./02_etl_pipeline/

# Copy .env file — credentials needed inside container
COPY .env .

# Default command — runs the full ETL pipeline
CMD ["python", "02_etl_pipeline/pipeline.py"]