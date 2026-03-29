#!/usr/bin/env bash
# ============================================================
# Enterprise ETL Platform - Setup & Management Script
# ============================================================
# Usage:
#   ./scripts/setup.sh [command]
#
# Commands:
#   up        - Start all services
#   down      - Stop all services
#   logs      - Tail logs from all services
#   test      - Run test suite
#   seed      - Generate sample data
#   clean     - Remove containers, volumes, and temp files
#   reset     - clean + up (full fresh start)
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()    { echo -e "${BLUE}[ETL]${NC} $*"; }
success(){ echo -e "${GREEN}[✓]${NC} $*"; }
warn()   { echo -e "${YELLOW}[!]${NC} $*"; }
error()  { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }

cmd="${1:-help}"

case "$cmd" in
  up)
    log "Starting Enterprise ETL Platform..."

    # Check Docker
    command -v docker >/dev/null 2>&1 || error "Docker not found. Install Docker Desktop."
    command -v docker-compose >/dev/null 2>&1 || \
      docker compose version >/dev/null 2>&1 || \
      error "docker-compose not found."

    # Create .env if missing
    if [ ! -f "$PROJECT_DIR/.env" ]; then
      warn ".env not found — copying from .env.example"
      cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
    fi

    # Create required directories
    mkdir -p "$PROJECT_DIR/data"
    mkdir -p "$PROJECT_DIR/logs"
    mkdir -p "$PROJECT_DIR/airflow/logs"

    # Set Airflow UID
    export AIRFLOW_UID=$(id -u)

    log "Building and starting containers (this may take a few minutes on first run)..."
    docker compose -f "$COMPOSE_FILE" up -d --build

    log "Waiting for services to be healthy..."
    sleep 10

    # Wait for postgres
    log "Waiting for PostgreSQL..."
    until docker compose -f "$COMPOSE_FILE" exec postgres pg_isready -U postgres >/dev/null 2>&1; do
      sleep 2
    done
    success "PostgreSQL ready"

    # Wait for backend
    log "Waiting for Backend API..."
    until curl -sf http://localhost:8000/health >/dev/null 2>&1; do
      sleep 3
    done
    success "Backend API ready"

    echo ""
    success "🚀 Enterprise ETL Platform is running!"
    echo ""
    echo "  Service URLs:"
    echo "  ├─ Backend API    : http://localhost:8000"
    echo "  ├─ API Docs       : http://localhost:8000/docs"
    echo "  ├─ Dashboard      : http://localhost:8501"
    echo "  ├─ Airflow UI     : http://localhost:8080 (user: airflow / pass: airflow)"
    echo "  └─ PostgreSQL     : localhost:5432 (user: etl_user / pass: etl_pass / db: etl_db)"
    ;;

  down)
    log "Stopping all services..."
    docker compose -f "$COMPOSE_FILE" down
    success "All services stopped."
    ;;

  logs)
    SERVICE="${2:-}"
    docker compose -f "$COMPOSE_FILE" logs -f --tail=100 $SERVICE
    ;;

  test)
    log "Running test suite..."
    cd "$PROJECT_DIR"
    pip install -r tests/requirements-test.txt -q
    pytest tests/unit/ -v --tb=short
    success "Unit tests complete."
    ;;

  seed)
    log "Generating sample data..."
    cd "$PROJECT_DIR"
    python scripts/generate_sample_data.py
    success "Sample data written to ./data/"
    ;;

  clean)
    warn "This will remove all containers, volumes, and generated files."
    read -r -p "Continue? [y/N] " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || exit 0
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    rm -rf "$PROJECT_DIR/data/"*.csv "$PROJECT_DIR/data/"*.parquet
    rm -rf "$PROJECT_DIR/logs/"*
    rm -rf "$PROJECT_DIR/airflow/logs/"*
    success "Cleanup complete."
    ;;

  reset)
    bash "$0" clean
    bash "$0" seed
    bash "$0" up
    ;;

  status)
    docker compose -f "$COMPOSE_FILE" ps
    ;;

  help|*)
    echo ""
    echo "  Enterprise ETL Platform - Management Script"
    echo ""
    echo "  Usage: ./scripts/setup.sh [command]"
    echo ""
    echo "  Commands:"
    echo "    up      Start all services (builds images if needed)"
    echo "    down    Stop all services"
    echo "    logs    Tail logs (optionally specify service name)"
    echo "    test    Run the test suite"
    echo "    seed    Generate sample CSV data files"
    echo "    clean   Remove all containers and volumes"
    echo "    reset   Full clean + rebuild + start"
    echo "    status  Show container status"
    echo ""
    ;;
esac
