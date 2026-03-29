"""
Enterprise ETL Platform - Pipeline API Routes

Endpoints for triggering pipelines, fetching run status,
and viewing run history.
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.database import get_async_db
from backend.services.pipeline_service import PipelineService
from backend.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


# ── Request / Response Models ─────────────────────────────────────────────────

class TriggerRequest(BaseModel):
    """Request body for triggering a pipeline."""
    pipeline_name: str = Field(..., description="Name of the pipeline to run")
    triggered_by: str = Field(default="api", description="Caller identifier")
    config_override: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional runtime config overrides (merged with YAML config)"
    )


class TriggerResponse(BaseModel):
    """Response after triggering a pipeline."""
    run_id: str
    pipeline_name: str
    status: str
    message: str


class RunSummaryResponse(BaseModel):
    """Summary of a single pipeline run."""
    run_id: str
    pipeline_name: str
    status: str
    triggered_by: Optional[str]
    records_extracted: Optional[int]
    records_loaded: Optional[int]
    records_failed: Optional[int]
    quality_score: Optional[float]
    duration_seconds: Optional[float]
    started_at: Optional[str]
    finished_at: Optional[str]
    error_message: Optional[str]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post(
    "/trigger",
    response_model=TriggerResponse,
    summary="Trigger a Pipeline",
    description="Synchronously trigger a named ETL pipeline. Returns after completion.",
)
async def trigger_pipeline(
    request: TriggerRequest,
    db: AsyncSession = Depends(get_async_db),
) -> TriggerResponse:
    """
    Trigger a named pipeline synchronously.

    The pipeline config must exist under configs/pipelines/{pipeline_name}.yaml
    """
    service = PipelineService(db)
    try:
        result = await service.trigger_pipeline(
            pipeline_name=request.pipeline_name,
            triggered_by=request.triggered_by,
            override_config=request.config_override,
        )
        return TriggerResponse(
            run_id=result["run_id"],
            pipeline_name=result["pipeline_name"],
            status=result["status"],
            message=f"Pipeline completed with {result['records_loaded']:,} records loaded.",
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.error("Pipeline trigger failed", pipeline=request.pipeline_name, error=str(exc))
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {str(exc)}")


@router.get(
    "/runs",
    response_model=List[RunSummaryResponse],
    summary="List Pipeline Runs",
    description="Retrieve paginated pipeline run history with optional filtering.",
)
async def list_runs(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    limit: int = Query(default=50, ge=1, le=200, description="Results per page"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    db: AsyncSession = Depends(get_async_db),
) -> List[RunSummaryResponse]:
    """Get paginated run history with optional pipeline filter."""
    service = PipelineService(db)
    runs = await service.get_run_history(
        pipeline_name=pipeline_name, limit=limit, offset=offset
    )
    return [RunSummaryResponse(**r) for r in runs]


@router.get(
    "/runs/{run_id}",
    response_model=RunSummaryResponse,
    summary="Get Run Detail",
    description="Get full detail for a single pipeline run by UUID.",
)
async def get_run(
    run_id: str,
    db: AsyncSession = Depends(get_async_db),
) -> RunSummaryResponse:
    """Retrieve a single pipeline run by its UUID."""
    service = PipelineService(db)
    run = await service.get_run_by_id(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return RunSummaryResponse(**run)


@router.get(
    "/status/{pipeline_name}",
    summary="Get Pipeline Status",
    description="Get the latest run status for a named pipeline.",
)
async def get_pipeline_status(
    pipeline_name: str,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """Get latest execution status for a pipeline."""
    service = PipelineService(db)
    runs = await service.get_run_history(pipeline_name=pipeline_name, limit=1)
    if not runs:
        return {
            "pipeline_name": pipeline_name,
            "status": "never_run",
            "last_run": None,
        }
    latest = runs[0]
    return {
        "pipeline_name": pipeline_name,
        "status": latest["status"],
        "last_run": latest["started_at"],
        "last_run_id": latest["run_id"],
        "records_loaded": latest.get("records_loaded"),
        "quality_score": latest.get("quality_score"),
    }


@router.get(
    "/list",
    summary="List Available Pipelines",
    description="List all pipeline configs found in the configs directory.",
)
async def list_pipelines() -> Dict[str, Any]:
    """Discover all available pipeline configurations."""
    from pathlib import Path
    from backend.core.config import settings

    config_dir = Path(settings.CONFIG_DIR) / "pipelines"
    pipelines = []
    if config_dir.exists():
        for f in config_dir.glob("*.yaml"):
            pipelines.append(f.stem)

    return {"pipelines": sorted(pipelines), "count": len(pipelines)}
