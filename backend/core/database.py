"""
Enterprise ETL Platform - Database Layer

SQLAlchemy engine, session factory, and async database initialization.
Supports both sync (for Celery workers) and async (for FastAPI) patterns.
"""
from typing import AsyncGenerator, Generator

from sqlalchemy import create_engine, text, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import QueuePool

from backend.core.config import settings
from backend.utils.logger import get_logger

logger = get_logger(__name__)


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""
    pass


# ── Synchronous Engine (Celery workers, scripts) ────────────────────────────
sync_engine = create_engine(
    settings.db_url_sync,
    poolclass=QueuePool,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_pre_ping=True,
    echo=settings.DEBUG,
)

SyncSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=sync_engine,
)


# ── Asynchronous Engine (FastAPI endpoints) ─────────────────────────────────
async_engine = create_async_engine(
    settings.db_url_async,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_pre_ping=True,
    echo=settings.DEBUG,
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


async def init_db() -> None:
    """
    Initialize database by creating all tables from ORM models.
    In production, use Alembic migrations instead.
    """
    # Import models to ensure they're registered with Base.metadata
    from backend.models import pipeline_run, pipeline_task, data_quality  # noqa: F401

    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables initialized")


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency injection for async database sessions.

    Usage:
        @router.get("/")
        async def endpoint(db: AsyncSession = Depends(get_async_db)):
            ...
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def get_sync_db() -> Generator[Session, None, None]:
    """
    Synchronous database session generator for non-async contexts.
    """
    session = SyncSessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
