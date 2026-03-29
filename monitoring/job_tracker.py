"""
Enterprise ETL Platform - Job Status Tracker

Provides real-time status aggregation for pipelines.
Designed to be polled by the dashboard and alerting system.
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text

from backend.core.config import settings
from backend.utils.logger import get_logger

logger = get_logger(__name__)


class JobStatusTracker:
    """
    Queries pipeline_runs and surfaces actionable status summaries.

    Uses synchronous SQLAlchemy for compatibility with non-async
    contexts like Airflow operators and CLI scripts.
    """

    def __init__(self, db_url: Optional[str] = None) -> None:
        self._engine = create_engine(
            db_url or settings.db_url_sync,
            pool_pre_ping=True,
            pool_size=3,
        )

    def get_platform_health(self) -> Dict[str, Any]:
        """
        Returns a health snapshot of the ETL platform.

        Returns:
            dict with overall health status and component details.
        """
        with self._engine.connect() as conn:
            row = conn.execute(text("""
                SELECT
                    COUNT(*) AS total_runs,
                    COUNT(*) FILTER (WHERE status='success') AS succeeded,
                    COUNT(*) FILTER (WHERE status='failed')  AS failed,
                    COUNT(*) FILTER (WHERE status='running') AS running,
                    COUNT(*) FILTER (WHERE started_at >= NOW() - INTERVAL '24 hours') AS last_24h,
                    AVG(quality_score) AS avg_quality
                FROM pipeline_runs
            """)).one()

        success_rate = 0.0
        terminal = (row.succeeded or 0) + (row.failed or 0)
        if terminal > 0:
            success_rate = round(row.succeeded / terminal * 100, 1)

        # Determine health level
        if success_rate >= 95 or terminal == 0:
            health = "healthy"
        elif success_rate >= 80:
            health = "degraded"
        else:
            health = "critical"

        return {
            "health": health,
            "success_rate_pct": success_rate,
            "total_runs": row.total_runs,
            "runs_last_24h": row.last_24h,
            "currently_running": row.running,
            "avg_quality_score": round(float(row.avg_quality or 0), 2),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    def get_stalled_pipelines(self, threshold_hours: int = 4) -> List[Dict[str, Any]]:
        """
        Find pipelines stuck in 'running' status longer than threshold.

        Args:
            threshold_hours: Max allowed running time before flagging.

        Returns:
            List of stalled pipeline run dicts.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)
        with self._engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT run_id, pipeline_name, started_at,
                       EXTRACT(EPOCH FROM (NOW() - started_at)) / 3600 AS hours_running
                FROM pipeline_runs
                WHERE status = 'running'
                  AND started_at < :cutoff
                ORDER BY started_at ASC
            """), {"cutoff": cutoff}).fetchall()

        stalled = [
            {
                "run_id": str(r.run_id),
                "pipeline_name": r.pipeline_name,
                "started_at": r.started_at.isoformat(),
                "hours_running": round(float(r.hours_running), 1),
            }
            for r in rows
        ]

        if stalled:
            logger.warning(
                f"Found {len(stalled)} stalled pipeline(s)",
                pipelines=[s["pipeline_name"] for s in stalled],
            )

        return stalled

    def get_recent_failures(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Return all failed runs in the last N hours.

        Args:
            hours: Lookback window.

        Returns:
            List of failed run summaries.
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        with self._engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT run_id, pipeline_name, error_message, started_at, finished_at
                FROM pipeline_runs
                WHERE status = 'failed'
                  AND started_at >= :since
                ORDER BY started_at DESC
            """), {"since": since}).fetchall()

        return [
            {
                "run_id": str(r.run_id),
                "pipeline_name": r.pipeline_name,
                "error_message": r.error_message,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            }
            for r in rows
        ]

    def get_pipeline_sla_report(self) -> List[Dict[str, Any]]:
        """
        Return SLA compliance per pipeline: avg vs p95 duration.
        """
        with self._engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                    pipeline_name,
                    COUNT(*) AS runs,
                    ROUND(AVG(duration_seconds)::NUMERIC, 1) AS avg_duration,
                    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                          (ORDER BY duration_seconds)::NUMERIC, 1) AS p95_duration,
                    ROUND(MIN(duration_seconds)::NUMERIC, 1) AS min_duration,
                    ROUND(MAX(duration_seconds)::NUMERIC, 1) AS max_duration
                FROM pipeline_runs
                WHERE status = 'success'
                  AND duration_seconds IS NOT NULL
                GROUP BY pipeline_name
                ORDER BY avg_duration DESC
            """)).fetchall()

        return [
            {
                "pipeline_name": r.pipeline_name,
                "runs": r.runs,
                "avg_duration_s": float(r.avg_duration or 0),
                "p95_duration_s": float(r.p95_duration or 0),
                "min_duration_s": float(r.min_duration or 0),
                "max_duration_s": float(r.max_duration or 0),
            }
            for r in rows
        ]

    def close(self) -> None:
        """Dispose database connection pool."""
        self._engine.dispose()

    def __enter__(self) -> "JobStatusTracker":
        return self

    def __exit__(self, *args) -> None:
        self.close()
