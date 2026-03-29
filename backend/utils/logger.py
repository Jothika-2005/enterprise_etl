"""
Enterprise ETL Platform - Logging Utility

Provides structured JSON logging using structlog with console
and optional database sink. Every module should use get_logger().
"""
import logging
import sys
from typing import Any, Dict, Optional

import structlog
from structlog.types import FilteringBoundLogger

from backend.core.config import settings


def configure_logging() -> None:
    """
    Configure structlog with processors for structured JSON output.
    Called once at application startup.
    """
    def add_logger_name(logger, method, event_dict):
        """Safe logger name processor that works with PrintLogger."""
        name = getattr(logger, "name", None) or event_dict.pop("_logger_name", None)
        if name:
            event_dict["logger"] = name
        return event_dict

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.ExceptionRenderer(),
    ]

    if settings.is_production:
        # JSON output for log aggregation (ELK, Datadog, CloudWatch)
        renderer = structlog.processors.JSONRenderer()
    else:
        # Pretty console output for development
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging for libraries
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    )


# Initialize on module import
configure_logging()


def get_logger(name: str) -> FilteringBoundLogger:
    """
    Get a named structured logger.

    Args:
        name: Logger name (typically __name__).

    Returns:
        FilteringBoundLogger: Configured structlog logger.

    Example:
        logger = get_logger(__name__)
        logger.info("Pipeline started", pipeline_name="sales_etl", run_id=str(run.id))
    """
    return structlog.get_logger(name)


def get_pipeline_logger(
    pipeline_name: str,
    run_id: Optional[str] = None,
) -> FilteringBoundLogger:
    """
    Get a logger pre-bound with pipeline context.

    Args:
        pipeline_name: Name of the ETL pipeline.
        run_id: Current pipeline run UUID.

    Returns:
        FilteringBoundLogger: Logger with pipeline context bound.
    """
    logger = get_logger("etl.pipeline")
    bound = logger.bind(pipeline_name=pipeline_name)
    if run_id:
        bound = bound.bind(run_id=run_id)
    return bound
