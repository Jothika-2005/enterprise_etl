# ⚙️ Enterprise Data Cleaning & ETL Orchestration Platform

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688?logo=fastapi)](https://fastapi.tiangolo.com)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.31-FF4B4B?logo=streamlit)](https://streamlit.io)
[![Plotly](https://img.shields.io/badge/Plotly-5.18-3F4F75?logo=plotly)](https://plotly.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docker.com)
[![Tests](https://img.shields.io/badge/Tests-75%20Passing-brightgreen)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

> **A production-grade, full-stack ETL automation platform** — built for enterprise data engineering. Combines a FastAPI REST backend, Apache Airflow orchestration, a universal multi-format file parser, automated data cleaning, a 5-dimension quality scoring engine, and a dark-mode SaaS-style Streamlit dashboard with live Plotly charts.

---

## ✨ What's New in v2.0

| Module | What it adds |
|---|---|
| `data_ingestion/` | Universal file parser: CSV · Excel · JSON · TXT · PDF · DOCX |
| `data_ingestion/cleaner.py` | Auto data cleaning: dedup · null fill · type coercion · name normalisation |
| `quality_engine/` | 5-dimension quality scorer: 0–100 score with grade (A–F) |
| `visualization/` | 10 Plotly chart builders: gauge · radar · heatmap · timeline · histogram |
| `ui_components/` | Full CSS library: KPI cards · step tracker · log viewer · badges |
| `dashboard/main_dashboard.py` | 7-page enterprise SaaS dashboard with dark mode |

---

## 🏗️ Architecture

```
  ┌──────────────────┐     ┌──────────────────────────────────────┐
  │   FILE UPLOAD    │     │           ETL CORE ENGINE            │
  │  CSV  Excel      │────▶│  Extract → Transform → Quality → Load│
  │  JSON TXT        │     │  Plugin extractors (CSV/SQL/API)     │
  │  PDF  DOCX       │     │  10-rule transformation engine       │
  └──────────────────┘     │  4-dimension quality scorer          │
                           └─────────────────┬────────────────────┘
  ┌──────────────────┐                       │
  │  STREAMLIT UI    │     ┌─────────────────▼────────────────────┐
  │  7-page SaaS UI  │◀───▶│         Apache Airflow 2.8           │
  │  Dark Mode       │     │  Celery Workers + Redis Broker       │
  │  Plotly Charts   │     │  Dynamic DAG Generator (YAML→DAG)    │
  │  Quality Gauge   │     └──────────────────────────────────────┘
  └──────────────────┘
        │                  ┌──────────────────────────────────────┐
        └─────────────────▶│   FastAPI + PostgreSQL + Alembic     │
                           │   11 REST endpoints · Async ORM      │
                           │   Email + Slack Alerting             │
                           └──────────────────────────────────────┘
```

---

## 🗂️ Project Structure

```
enterprise_etl/
├── backend/                   # FastAPI app + ETL core
│   ├── core/extractors/       # CSV / SQL (incremental) / REST API
│   ├── core/transformers/     # Rule engine + quality scorer
│   ├── core/loaders/          # PostgreSQL / CSV / Parquet
│   ├── services/              # Pipeline lifecycle + alerting
│   ├── api/routes/            # 11 FastAPI endpoints
│   └── migrations/            # Alembic env.py
│
├── airflow/dags/
│   ├── customer_data_pipeline.py  # Full production DAG
│   └── dynamic_dag_generator.py   # YAML → DAG factory
│
├── dashboard/
│   └── main_dashboard.py          # ★ 7-page enterprise dashboard
│
├── data_ingestion/            # ★ Universal file parser & cleaner
│   ├── file_parser.py         # Parse CSV/Excel/JSON/TXT/PDF/DOCX
│   └── cleaner.py             # Auto-cleaning + column profiler
│
├── quality_engine/            # ★ 5-dimension quality scorer
│   └── scorer.py              # 0–100 score · grade A–F · radar
│
├── visualization/             # ★ Plotly chart library (10 charts)
│   └── charts.py
│
├── ui_components/             # ★ CSS + KPI cards + log viewer
│   └── components.py
│
├── monitoring/job_tracker.py  # SLA reports + stalled pipeline detection
├── configs/pipelines/*.yaml   # Pipeline configs (2 included)
├── docker/                    # Dockerfiles + PostgreSQL schema
├── tests/unit/                # 66 unit tests
├── tests/integration/         # 9 integration tests
├── scripts/setup.sh           # One-command setup
├── docker-compose.yml         # 8 Docker services
├── requirements.txt           # All dependencies
└── .env.example
```

---

## 🚀 Quick Start

### Full Stack (Docker) — Recommended

```bash
git clone https://github.com/yourname/enterprise-etl-platform.git
cd enterprise-etl-platform

cp .env.example .env

# Generate sample data
pip install pandas numpy
python scripts/generate_sample_data.py

# Start all 8 services (build takes ~3 min first time)
./scripts/setup.sh up
```

| Service | URL | Credentials |
|---|---|---|
| **Dashboard** | http://localhost:8501 | — |
| **API Docs** | http://localhost:8000/docs | — |
| **Airflow UI** | http://localhost:8080 | airflow / airflow |
| **PostgreSQL** | localhost:5432 | etl_user / etl_pass / etl_db |

### Dashboard Only (no Docker)

```bash
pip install streamlit plotly pandas numpy pdfplumber python-docx openpyxl
streamlit run dashboard/main_dashboard.py
```

---

## 📤 Uploading Files

1. Sidebar → **📤 Upload Data**
2. Drag & drop: **CSV, Excel, JSON, TXT, PDF, or DOCX**
3. Click **🚀 Parse & Clean File**
4. View results in **🧹 Cleaning Results** · **🔬 Data Quality**
5. Click **⬇️ Download** to export cleaned data (CSV / Excel / JSON)

---

## 🔄 Triggering Pipelines

**Via Dashboard** → Sidebar → **🔄 Pipeline Monitor** → select pipeline → **▶ Run Pipeline**

**Via REST API:**
```bash
curl -X POST http://localhost:8000/api/v1/pipelines/trigger \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "customer_data_pipeline", "triggered_by": "api"}'
```

---

## ➕ Adding a New Pipeline

Create `configs/pipelines/my_pipeline.yaml` — zero code required:

```yaml
pipeline_name: my_pipeline
schedule: "@daily"
source:
  type: csv
  path: /opt/etl/data/my_data.csv
transformations:
  - name: remove_duplicates
  - name: fill_missing
    config: {strategy: constant, fill_value: "N/A"}
destination:
  type: postgresql
  table: my_clean_table
quality_threshold: 0.80
```

---

## 🧪 Running Tests

```bash
pip install -r tests/requirements-test.txt
pytest tests/unit/ -v          # 66 unit tests
pytest tests/integration/ -v   # 9 integration tests
pytest --cov=backend           # with coverage
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| API | FastAPI + Uvicorn |
| Dashboard | Streamlit 1.31 |
| Charts | Plotly 5.18 |
| Orchestration | Apache Airflow 2.8 + Celery |
| File Parsing | pdfplumber · python-docx · openpyxl |
| Data Processing | Pandas 2.2 + PyArrow |
| Database | PostgreSQL 15 + SQLAlchemy 2.0 |
| Queue | Redis 7 |
| Config | Pydantic Settings |
| Logging | Structlog (JSON) |
| Containerisation | Docker Compose (8 services) |
| Testing | pytest 8.0 — 75 tests |

---

## 📄 Resume Description

> Designed and developed an **Enterprise Data Cleaning & ETL Orchestration Platform** as a full-year final project and Infosys internship submission. The system features a Python 3.11/FastAPI REST backend, Apache Airflow 2.8 with Celery workers, a universal multi-format file ingestion engine (CSV/Excel/JSON/TXT/PDF/DOCX), automated data cleaning pipeline, and a 5-dimension data quality scoring system (0–100). Delivered a professional dark-mode Streamlit analytics dashboard with live Plotly charts, animated KPI cards, ETL execution timeline, log viewer, and system health monitor. Full infrastructure containerised with Docker Compose (8 services). 75 passing tests.

---

## 📜 License

MIT
