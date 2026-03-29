"""
Enterprise ETL Platform - Health Check Route
"""
import time
from typing import Any, Dict

from fastapi import APIRouter
from sqlalchemy import text

from backend.core.database import async_engine
from backend.core.config import settings
from backend.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()
_start_time = time.time()


@router.get("/health", summary="Health Check")
async def health_check() -> Dict[str, Any]:
    """Return platform health status including DB connectivity."""
    db_ok = False
    try:
        async with async_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception as exc:
        logger.warning("DB health check failed", error=str(exc))

    uptime = round(time.time() - _start_time, 1)
    status = "healthy" if db_ok else "degraded"

    return {
        "status": status,
        "version": "1.0.0",
        "environment": settings.ENVIRONMENT,
        "uptime_seconds": uptime,
        "components": {
            "database": "ok" if db_ok else "error",
            "api": "ok",
        },
    }
