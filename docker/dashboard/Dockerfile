FROM python:3.11-slim

LABEL maintainer="ETL Platform Team"
LABEL description="Enterprise ETL Dashboard - Streamlit"

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/dashboard/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

COPY dashboard/ /app/dashboard/
COPY configs/ /app/configs/

RUN useradd --create-home --shell /bin/bash dashuser && \
    chown -R dashuser:dashuser /app
USER dashuser

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
