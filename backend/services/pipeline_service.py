"""
Enterprise ETL Platform - Pipeline Service

Orchestrates the full ETL lifecycle: extract → transform → quality check
→ load → persist run metrics → alert. This is the central service
that all API endpoints and Airflow operators call.
"""
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.core.config import settings
from backend.core.extractors.factory import get_extractor
from backend.core.transformers.engine import TransformationEngine
from backend.core.transformers.quality import DataQualityScorer
from backend.core.loaders.loader import get_loader
from backend.models.pipeline_run import (
    PipelineRun, PipelineTask, DataQualityMetric, ETLCheckpoint
)
from backend.services.alert_service import alert_service
from backend.utils.logger import get_pipeline_logger


class PipelineService:
    """
    Full ETL pipeline orchestrator.

    Responsibilities:
    - Load and validate pipeline configuration from YAML
    - Execute extraction, transformation, quality, and loading phases
    - Persist run metrics to database
    - Emit alerts on failure or quality violations
    - Manage incremental load checkpoints
    """

    def __init__(self, db: AsyncSession) -> None:
        self.db = db
        self.config_dir = Path(settings.CONFIG_DIR)

    async def trigger_pipeline(
        self,
        pipeline_name: str,
        triggered_by: str = "api",
        override_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Trigger a named pipeline by loading its YAML config and executing.

        Args:
            pipeline_name: Name matching a YAML config file.
            triggered_by: Trigger source (api | scheduler | airflow).
            override_config: Optional runtime config overrides.

        Returns:
            Dict with run_id, status, and summary metrics.
        """
        config = self._load_config(pipeline_name, override_config)
        return await self.execute_pipeline(pipeline_name, config, triggered_by)

    async def execute_pipeline(
        self,
        pipeline_name: str,
        config: Dict[str, Any],
        triggered_by: str = "scheduler",
        dag_run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Core pipeline execution engine.

        Phases: extract → transform → quality_check → load

        Args:
            pipeline_name: Human-readable pipeline identifier.
            config: Full pipeline configuration dictionary.
            triggered_by: Source that triggered this run.
            dag_run_id: Airflow DAG run ID if triggered from Airflow.

        Returns:
            Run summary dict.
        """
        run_id = str(uuid.uuid4())
        logger = get_pipeline_logger(pipeline_name, run_id)
        run = await self._create_run(pipeline_name, run_id, triggered_by, dag_run_id, config)

        logger.info("Pipeline execution started", triggered_by=triggered_by)

        try:
            # ── Phase 1: Extract ────────────────────────────────────────────
            logger.info("Phase 1/4: Extraction starting")
            checkpoint = await self._get_checkpoint(pipeline_name, config)
            extract_task = await self._create_task(run.run_id, "extract", "extract")

            extractor_config = {**config.get("source", {}), "name": pipeline_name}
            extractor = get_extractor(extractor_config)
            with extractor:
                extract_result = extractor.extract(checkpoint_value=checkpoint)

            await self._finish_task(
                extract_task,
                "success",
                records_out=extract_result.records_count,
            )
            run.records_extracted = extract_result.records_count
            logger.info("Extraction complete", records=extract_result.records_count)

            # ── Phase 2: Transform ──────────────────────────────────────────
            logger.info("Phase 2/4: Transformation starting")
            transform_task = await self._create_task(run.run_id, "transform", "transform")
            engine = TransformationEngine(pipeline_name)
            rules = config.get("transformations", [])
            transform_result = engine.apply(extract_result.data, rules, run_id)

            await self._finish_task(
                transform_task,
                "success",
                records_in=transform_result.records_in,
                records_out=transform_result.records_out,
            )
            run.records_transformed = transform_result.records_out
            logger.info(
                "Transformation complete",
                records_in=transform_result.records_in,
                records_out=transform_result.records_out,
                rules=transform_result.rules_applied,
            )

            # ── Phase 3: Quality Check ──────────────────────────────────────
            logger.info("Phase 3/4: Quality evaluation starting")
            quality_config = config.get("quality", {})
            scorer = DataQualityScorer(pipeline_name, quality_config)
            quality_report = scorer.evaluate(transform_result.data)

            await self._persist_quality_metrics(run.run_id, pipeline_name, quality_report)
            run.quality_score = float(quality_report.composite_score * 100)

            threshold = config.get("quality_threshold", settings.DEFAULT_QUALITY_THRESHOLD)
            if quality_report.composite_score < threshold:
                logger.warning(
                    "Quality check BELOW threshold",
                    score=quality_report.composite_score,
                    threshold=threshold,
                )
                alert_service.send_quality_alert(
                    pipeline_name=pipeline_name,
                    run_id=run_id,
                    quality_score=quality_report.composite_score,
                    threshold=threshold,
                    details={"failed_checks": quality_report.failed_checks},
                )

            logger.info(
                "Quality evaluation complete",
                composite_score=quality_report.composite_score,
                passed=quality_report.passed_checks,
                failed=quality_report.failed_checks,
            )

            # ── Phase 4: Load ───────────────────────────────────────────────
            logger.info("Phase 4/4: Loading starting")
            load_task = await self._create_task(run.run_id, "load", "load")
            destination_config = {
                **config.get("destination", {}),
                "name": pipeline_name,
            }
            if "connection_string" not in destination_config:
                destination_config["connection_string"] = settings.DATABASE_URL

            loader = get_loader(destination_config)
            load_result = loader.load(transform_result.data)

            if not load_result.success:
                raise RuntimeError(f"Load failed: {load_result.error}")

            await self._finish_task(
                load_task,
                "success",
                records_in=transform_result.records_out,
                records_out=load_result.records_loaded,
            )
            run.records_loaded = load_result.records_loaded
            run.records_failed = load_result.records_failed

            # ── Save Checkpoint ─────────────────────────────────────────────
            if extract_result.checkpoint_value:
                await self._save_checkpoint(
                    pipeline_name, config, extract_result.checkpoint_value
                )

            # ── Finalize Run ────────────────────────────────────────────────
            await self._finish_run(run, "success")
            logger.info(
                "Pipeline completed successfully",
                records_loaded=run.records_loaded,
                quality_score=run.quality_score,
                duration_seconds=float(run.duration_seconds or 0),
            )

            return self._build_summary(run, quality_report)

        except Exception as exc:
            tb = traceback.format_exc()
            logger.error("Pipeline failed", error=str(exc), exc_info=True)
            await self._finish_run(run, "failed", error_message=str(exc))
            alert_service.send_pipeline_failure(
                pipeline_name=pipeline_name,
                run_id=run_id,
                error=str(exc),
                traceback=tb,
            )
            raise

    # ── Database helpers ─────────────────────────────────────────────────────

    async def _create_run(
        self,
        pipeline_name: str,
        run_id: str,
        triggered_by: str,
        dag_run_id: Optional[str],
        config: Dict[str, Any],
    ) -> PipelineRun:
        run = PipelineRun(
            run_id=uuid.UUID(run_id),
            pipeline_name=pipeline_name,
            dag_run_id=dag_run_id,
            status="running",
            triggered_by=triggered_by,
            config_snapshot=config,
            started_at=datetime.now(timezone.utc),
        )
        self.db.add(run)
        await self.db.flush()
        return run

    async def _create_task(
        self, run_id: uuid.UUID, task_name: str, task_type: str
    ) -> PipelineTask:
        task = PipelineTask(
            run_id=run_id,
            task_name=task_name,
            task_type=task_type,
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        self.db.add(task)
        await self.db.flush()
        return task

    async def _finish_task(
        self,
        task: PipelineTask,
        status: str,
        records_in: int = 0,
        records_out: int = 0,
        error: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        task.status = status
        task.finished_at = now
        task.records_in = records_in
        task.records_out = records_out
        task.error_message = error
        if task.started_at:
            task.duration_seconds = (now - task.started_at).total_seconds()
        await self.db.flush()

    async def _finish_run(
        self,
        run: PipelineRun,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        run.status = status
        run.finished_at = now
        run.error_message = error_message
        if run.started_at:
            run.duration_seconds = (now - run.started_at).total_seconds()
        await self.db.commit()

    async def _persist_quality_metrics(
        self, run_id: uuid.UUID, pipeline_name: str, quality_report
    ) -> None:
        for check in quality_report.checks:
            metric = DataQualityMetric(
                run_id=run_id,
                pipeline_name=pipeline_name,
                metric_name=check.name,
                metric_category=check.category,
                passed=check.passed,
                expected_value=check.threshold,
                actual_value=check.actual_value,
                threshold=check.threshold,
                details=check.details,
            )
            self.db.add(metric)
        await self.db.flush()

    async def _get_checkpoint(
        self, pipeline_name: str, config: Dict[str, Any]
    ) -> Optional[str]:
        if not config.get("incremental"):
            return None
        source_name = config.get("source", {}).get("name", "default")
        result = await self.db.execute(
            select(ETLCheckpoint).where(
                ETLCheckpoint.pipeline_name == pipeline_name,
                ETLCheckpoint.source_name == source_name,
                ETLCheckpoint.checkpoint_key == "watermark",
            )
        )
        checkpoint = result.scalar_one_or_none()
        return checkpoint.checkpoint_value if checkpoint else None

    async def _save_checkpoint(
        self, pipeline_name: str, config: Dict[str, Any], value: str
    ) -> None:
        source_name = config.get("source", {}).get("name", "default")
        result = await self.db.execute(
            select(ETLCheckpoint).where(
                ETLCheckpoint.pipeline_name == pipeline_name,
                ETLCheckpoint.source_name == source_name,
                ETLCheckpoint.checkpoint_key == "watermark",
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            existing.checkpoint_value = value
            existing.updated_at = datetime.now(timezone.utc)
        else:
            self.db.add(ETLCheckpoint(
                pipeline_name=pipeline_name,
                source_name=source_name,
                checkpoint_key="watermark",
                checkpoint_value=value,
            ))
        await self.db.flush()

    # ── Config loading ────────────────────────────────────────────────────────

    def _load_config(
        self,
        pipeline_name: str,
        override: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        config_file = self.config_dir / "pipelines" / f"{pipeline_name}.yaml"
        if not config_file.exists():
            raise FileNotFoundError(
                f"No config found for pipeline '{pipeline_name}' at {config_file}"
            )
        with open(config_file) as f:
            config = yaml.safe_load(f)
        if override:
            config.update(override)
        return config

    def _build_summary(self, run: PipelineRun, quality_report) -> Dict[str, Any]:
        return {
            "run_id": str(run.run_id),
            "pipeline_name": run.pipeline_name,
            "status": run.status,
            "records_extracted": run.records_extracted,
            "records_transformed": run.records_transformed,
            "records_loaded": run.records_loaded,
            "records_failed": run.records_failed,
            "quality_score": run.quality_score,
            "duration_seconds": float(run.duration_seconds or 0),
            "quality_details": quality_report.to_dict(),
        }

    # ── Read operations ───────────────────────────────────────────────────────

    async def get_run_history(
        self,
        pipeline_name: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Retrieve paginated run history."""
        query = select(PipelineRun).order_by(PipelineRun.started_at.desc())
        if pipeline_name:
            query = query.where(PipelineRun.pipeline_name == pipeline_name)
        query = query.limit(limit).offset(offset)
        result = await self.db.execute(query)
        runs = result.scalars().all()
        return [self._run_to_dict(r) for r in runs]

    async def get_run_by_id(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a single run by UUID."""
        result = await self.db.execute(
            select(PipelineRun).where(PipelineRun.run_id == uuid.UUID(run_id))
        )
        run = result.scalar_one_or_none()
        return self._run_to_dict(run) if run else None

    def _run_to_dict(self, run: PipelineRun) -> Dict[str, Any]:
        return {
            "run_id": str(run.run_id),
            "pipeline_name": run.pipeline_name,
            "status": run.status,
            "triggered_by": run.triggered_by,
            "records_extracted": run.records_extracted,
            "records_loaded": run.records_loaded,
            "records_failed": run.records_failed,
            "quality_score": float(run.quality_score) if run.quality_score else None,
            "duration_seconds": float(run.duration_seconds) if run.duration_seconds else None,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "error_message": run.error_message,
        }
