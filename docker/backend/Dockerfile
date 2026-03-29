FROM python:3.11-slim

LABEL maintainer="ETL Platform Team"
LABEL description="Enterprise ETL Backend - FastAPI"

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Install Python dependencies
COPY docker/backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY backend/ /app/backend/
COPY configs/ /app/configs/

# Create directories
RUN mkdir -p /app/data /app/logs

# Non-root user for security
RUN useradd --create-home --shell /bin/bash etluser && \
    chown -R etluser:etluser /app
USER etluser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start FastAPI with Uvicorn
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--log-level", "info"]
