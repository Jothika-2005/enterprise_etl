"""
Enterprise ETL Platform - Metrics API Routes

Aggregated metrics for the dashboard and monitoring.
"""
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.database import get_async_db
from backend.models.pipeline_run import PipelineRun, DataQualityMetric
from backend.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/summary", summary="Get Platform Summary Metrics")
async def get_summary(db: AsyncSession = Depends(get_async_db)) -> Dict[str, Any]:
    """Return high-level platform KPIs for the dashboard header."""
    result = await db.execute(
        select(
            func.count().label("total_runs"),
            func.count().filter(PipelineRun.status == "success").label("successful"),
            func.count().filter(PipelineRun.status == "failed").label("failed"),
            func.count().filter(PipelineRun.status == "running").label("running"),
            func.sum(PipelineRun.records_loaded).label("total_records"),
            func.avg(PipelineRun.quality_score).label("avg_quality"),
            func.avg(PipelineRun.duration_seconds).label("avg_duration"),
        )
    )
    row = result.one()

    success_rate = 0.0
    total_terminal = (row.successful or 0) + (row.failed or 0)
    if total_terminal > 0:
        success_rate = round((row.successful or 0) / total_terminal * 100, 2)

    return {
        "total_runs": row.total_runs or 0,
        "successful_runs": row.successful or 0,
        "failed_runs": row.failed or 0,
        "running_runs": row.running or 0,
        "success_rate_pct": success_rate,
        "total_records_loaded": int(row.total_records or 0),
        "avg_quality_score": round(float(row.avg_quality or 0), 2),
        "avg_duration_seconds": round(float(row.avg_duration or 0), 2),
    }


@router.get("/by-pipeline", summary="Get Metrics Grouped by Pipeline")
async def get_by_pipeline(db: AsyncSession = Depends(get_async_db)) -> List[Dict[str, Any]]:
    """Return success/failure counts and avg quality per pipeline."""
    result = await db.execute(
        select(
            PipelineRun.pipeline_name,
            func.count().label("total_runs"),
            func.count().filter(PipelineRun.status == "success").label("successful"),
            func.count().filter(PipelineRun.status == "failed").label("failed"),
            func.avg(PipelineRun.quality_score).label("avg_quality"),
            func.avg(PipelineRun.duration_seconds).label("avg_duration"),
            func.sum(PipelineRun.records_loaded).label("total_records"),
            func.max(PipelineRun.started_at).label("last_run"),
        ).group_by(PipelineRun.pipeline_name)
        .order_by(func.max(PipelineRun.started_at).desc())
    )
    rows = result.all()

    return [
        {
            "pipeline_name": r.pipeline_name,
            "total_runs": r.total_runs,
            "successful_runs": r.successful,
            "failed_runs": r.failed,
            "success_rate_pct": round(
                (r.successful / max(r.successful + r.failed, 1)) * 100, 2
            ),
            "avg_quality_score": round(float(r.avg_quality or 0), 2),
            "avg_duration_seconds": round(float(r.avg_duration or 0), 2),
            "total_records_loaded": int(r.total_records or 0),
            "last_run": r.last_run.isoformat() if r.last_run else None,
        }
        for r in rows
    ]


@router.get("/trend", summary="Get Daily Run Trend (last 30 days)")
async def get_trend(
    days: int = Query(default=30, ge=1, le=90),
    db: AsyncSession = Depends(get_async_db),
) -> List[Dict[str, Any]]:
    """Return daily run counts and success rates for sparkline charts."""
    sql = text("""
        SELECT
            DATE(started_at AT TIME ZONE 'UTC') AS run_date,
            COUNT(*) AS total_runs,
            COUNT(*) FILTER (WHERE status = 'success') AS successful,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed,
            SUM(records_loaded) AS records_loaded,
            AVG(quality_score) AS avg_quality
        FROM pipeline_runs
        WHERE started_at >= NOW() - INTERVAL :days DAY
        GROUP BY run_date
        ORDER BY run_date ASC
    """)
    result = await db.execute(sql, {"days": f"{days}"})
    rows = result.all()

    return [
        {
            "date": str(r.run_date),
            "total_runs": r.total_runs,
            "successful": r.successful,
            "failed": r.failed,
            "records_loaded": int(r.records_loaded or 0),
            "avg_quality": round(float(r.avg_quality or 0), 2),
        }
        for r in rows
    ]


@router.get("/quality", summary="Get Data Quality Metrics")
async def get_quality_metrics(
    pipeline_name: str = Query(None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_async_db),
) -> List[Dict[str, Any]]:
    """Return quality check results for analysis."""
    query = (
        select(DataQualityMetric)
        .order_by(DataQualityMetric.evaluated_at.desc())
        .limit(limit)
    )
    if pipeline_name:
        query = query.where(DataQualityMetric.pipeline_name == pipeline_name)

    result = await db.execute(query)
    metrics = result.scalars().all()

    return [
        {
            "pipeline_name": m.pipeline_name,
            "metric_name": m.metric_name,
            "metric_category": m.metric_category,
            "passed": m.passed,
            "actual_value": float(m.actual_value) if m.actual_value else None,
            "threshold": float(m.threshold) if m.threshold else None,
            "details": m.details,
            "evaluated_at": m.evaluated_at.isoformat() if m.evaluated_at else None,
        }
        for m in metrics
    ]
