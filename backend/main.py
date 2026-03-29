"""
Enterprise ETL Platform - FastAPI Application Entry Point

Initializes the FastAPI app with all routers, middleware,
lifespan management, and OpenAPI configuration.
"""
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from backend.api.routes import pipelines, health, datasources, metrics
from backend.core.database import init_db
from backend.core.config import settings
from backend.utils.logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Application lifespan manager - startup and shutdown hooks."""
    logger.info("🚀 Enterprise ETL Platform starting up...")
    await init_db()
    logger.info("✅ Database initialized successfully")
    yield
    logger.info("🛑 Enterprise ETL Platform shutting down...")


def create_app() -> FastAPI:
    """
    Factory function to create and configure the FastAPI application.

    Returns:
        FastAPI: Configured application instance.
    """
    app = FastAPI(
        title="Enterprise ETL Orchestration Platform",
        description="""
        A production-grade ETL automation framework for enterprise data pipelines.

        ## Features
        * **Pipeline Management** - Trigger, monitor, and manage ETL pipelines
        * **Data Quality** - Track quality scores and metrics per pipeline run
        * **Alerting** - Email and Slack notifications on failures
        * **History** - Full run history with filtering and pagination
        * **Data Sources** - Register and test data source connections
        """,
        version="1.0.0",
        contact={
            "name": "ETL Platform Team",
            "email": "etl-team@company.com",
        },
        license_info={
            "name": "MIT",
        },
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # ── Middleware ──────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # ── Global exception handler ────────────────────────────────────────────
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error. Check logs for details."},
        )

    # ── Routers ─────────────────────────────────────────────────────────────
    app.include_router(health.router, tags=["Health"])
    app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["Pipelines"])
    app.include_router(datasources.router, prefix="/api/v1/datasources", tags=["Data Sources"])
    app.include_router(metrics.router, prefix="/api/v1/metrics", tags=["Metrics"])

    logger.info(f"FastAPI app created | Environment: {settings.ENVIRONMENT}")
    return app


app = create_app()
