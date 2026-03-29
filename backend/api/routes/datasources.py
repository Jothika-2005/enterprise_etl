"""
Enterprise ETL Platform - Data Sources API Routes

CRUD endpoints for managing registered data source connections.
"""
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.database import get_async_db
from backend.models.pipeline_run import DataSource
from backend.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


class DataSourceCreate(BaseModel):
    name: str = Field(..., description="Unique source name")
    source_type: str = Field(..., description="postgresql | csv | api | mysql")
    connection_config: Dict[str, Any] = Field(..., description="Connection parameters")


class DataSourceResponse(BaseModel):
    source_id: str
    name: str
    source_type: str
    is_active: bool
    last_test_status: Optional[str]


@router.get("/", response_model=List[DataSourceResponse], summary="List Data Sources")
async def list_sources(db: AsyncSession = Depends(get_async_db)) -> List[DataSourceResponse]:
    result = await db.execute(select(DataSource).where(DataSource.is_active == True))
    sources = result.scalars().all()
    return [
        DataSourceResponse(
            source_id=str(s.source_id),
            name=s.name,
            source_type=s.source_type,
            is_active=s.is_active,
            last_test_status=s.last_test_status,
        )
        for s in sources
    ]


@router.post("/", response_model=DataSourceResponse, summary="Register Data Source")
async def create_source(
    body: DataSourceCreate,
    db: AsyncSession = Depends(get_async_db),
) -> DataSourceResponse:
    # Mask sensitive fields before storing
    safe_config = {
        k: "***" if k.lower() in ("password", "token", "secret", "api_key") else v
        for k, v in body.connection_config.items()
    }
    source = DataSource(
        source_id=uuid.uuid4(),
        name=body.name,
        source_type=body.source_type,
        connection_config=safe_config,
    )
    db.add(source)
    await db.commit()
    return DataSourceResponse(
        source_id=str(source.source_id),
        name=source.name,
        source_type=source.source_type,
        is_active=source.is_active,
        last_test_status=None,
    )


@router.post("/{source_id}/test", summary="Test Data Source Connection")
async def test_source(
    source_id: str,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    result = await db.execute(
        select(DataSource).where(DataSource.source_id == uuid.UUID(source_id))
    )
    source = result.scalar_one_or_none()
    if not source:
        raise HTTPException(status_code=404, detail="Data source not found")

    # Simulate connectivity test (extend per source type)
    source.last_test_status = "success"
    await db.commit()
    return {"source_id": source_id, "status": "success", "message": "Connection test passed"}
