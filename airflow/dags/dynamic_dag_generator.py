"""
Enterprise ETL Platform - Dynamic DAG Generator

Reads pipeline YAML configurations and programmatically generates
Airflow DAGs at import time. Adding a new YAML config automatically
creates a new DAG — zero boilerplate code required.

This implements the enterprise "config-driven pipelines" pattern.
"""
import glob
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

CONFIG_DIR = os.environ.get("AIRFLOW_CONFIG_DIR", "/opt/etl/configs/pipelines")


def _make_extract_fn(pipeline_name: str, source_config: Dict):
    """Factory: create extraction task function for a given pipeline."""

    def extract_task(**context):
        import sys
        import pandas as pd
        sys.path.insert(0, "/opt/etl")

        from backend.core.extractors.factory import get_extractor

        cfg = {**source_config, "name": pipeline_name}
        extractor = get_extractor(cfg)
        with extractor:
            result = extractor.extract()

        tmp_path = f"/tmp/{pipeline_name}_{context['run_id']}_raw.parquet"
        result.data.to_parquet(tmp_path, index=False)
        context["task_instance"].xcom_push(key="raw_path", value=tmp_path)
        context["task_instance"].xcom_push(key="records_in", value=result.records_count)

        log.info(f"[{pipeline_name}] Extracted {result.records_count} records")

    extract_task.__name__ = f"extract_{pipeline_name}"
    return extract_task


def _make_transform_fn(pipeline_name: str, rules: list):
    """Factory: create transformation task function."""

    def transform_task(**context):
        import sys
        import pandas as pd
        sys.path.insert(0, "/opt/etl")

        from backend.core.transformers.engine import TransformationEngine

        ti = context["task_instance"]
        raw_path = ti.xcom_pull(task_ids="extract", key="raw_path")
        df = pd.read_parquet(raw_path)

        engine = TransformationEngine(pipeline_name)
        result = engine.apply(df, rules or [], run_id=context["run_id"])

        out_path = f"/tmp/{pipeline_name}_{context['run_id']}_clean.parquet"
        result.data.to_parquet(out_path, index=False)
        ti.xcom_push(key="clean_path", value=out_path)
        ti.xcom_push(key="records_out", value=result.records_out)

        log.info(f"[{pipeline_name}] Transformed → {result.records_out} records")

    transform_task.__name__ = f"transform_{pipeline_name}"
    return transform_task


def _make_load_fn(pipeline_name: str, destination_config: Dict):
    """Factory: create loader task function."""

    def load_task(**context):
        import sys
        import os
        import pandas as pd
        sys.path.insert(0, "/opt/etl")

        from backend.core.loaders.loader import get_loader

        ti = context["task_instance"]
        clean_path = ti.xcom_pull(task_ids="transform", key="clean_path")
        df = pd.read_parquet(clean_path)

        dest_cfg = {**destination_config}
        if "connection_string" not in dest_cfg:
            dest_cfg["connection_string"] = os.environ.get(
                "ETL_DB_URL", "postgresql://etl_user:etl_pass@postgres:5432/etl_db"
            )

        loader = get_loader(dest_cfg)
        result = loader.load(df)

        if not result.success:
            raise RuntimeError(f"Load failed: {result.error}")

        ti.xcom_push(key="records_loaded", value=result.records_loaded)
        log.info(f"[{pipeline_name}] Loaded {result.records_loaded} records")

    load_task.__name__ = f"load_{pipeline_name}"
    return load_task


def _generate_dag(pipeline_name: str, config: Dict[str, Any]) -> DAG:
    """
    Generate a complete Airflow DAG from a pipeline config dict.

    Args:
        pipeline_name: Pipeline identifier (also becomes dag_id).
        config: Loaded YAML configuration dictionary.

    Returns:
        Configured DAG instance.
    """
    schedule = config.get("schedule", "@daily")
    description = config.get("description", f"Auto-generated DAG for {pipeline_name}")
    tags = config.get("tags", ["etl", "generated"])

    default_args = {
        "owner": config.get("owner", "etl-team"),
        "retries": config.get("retries", 2),
        "retry_delay": timedelta(minutes=config.get("retry_delay_minutes", 5)),
        "email_on_failure": False,
        "depends_on_past": config.get("depends_on_past", False),
    }

    dag = DAG(
        dag_id=pipeline_name,
        description=description,
        default_args=default_args,
        schedule=schedule,
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
        tags=tags,
    )

    with dag:
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        extract = PythonOperator(
            task_id="extract",
            python_callable=_make_extract_fn(pipeline_name, config.get("source", {})),
            provide_context=True,
        )

        transform = PythonOperator(
            task_id="transform",
            python_callable=_make_transform_fn(
                pipeline_name, config.get("transformations", [])
            ),
            provide_context=True,
        )

        load = PythonOperator(
            task_id="load",
            python_callable=_make_load_fn(
                pipeline_name, config.get("destination", {"type": "csv", "path": f"/tmp/{pipeline_name}_out.csv"})
            ),
            provide_context=True,
        )

        start >> extract >> transform >> load >> end

    return dag


def _load_all_configs() -> Dict[str, Dict]:
    """Discover and load all YAML pipeline configs from CONFIG_DIR."""
    configs = {}
    if not os.path.exists(CONFIG_DIR):
        log.warning(f"Config directory not found: {CONFIG_DIR}")
        return configs

    for config_file in glob.glob(os.path.join(CONFIG_DIR, "*.yaml")):
        pipeline_name = os.path.splitext(os.path.basename(config_file))[0]
        try:
            with open(config_file) as f:
                cfg = yaml.safe_load(f)
            # Only generate dynamic DAG if not already handled by a dedicated DAG file
            if cfg.get("dynamic_dag", True):
                configs[pipeline_name] = cfg
                log.info(f"Loaded dynamic DAG config: {pipeline_name}")
        except Exception as exc:
            log.error(f"Failed to load config {config_file}: {exc}")

    return configs


# ── Generate DAGs at import time ───────────────────────────────────────────────
# Airflow discovers DAGs by finding DAG objects in module globals.
_all_configs = _load_all_configs()
for _pipeline_name, _config in _all_configs.items():
    try:
        globals()[_pipeline_name] = _generate_dag(_pipeline_name, _config)
        log.info(f"Dynamically registered DAG: {_pipeline_name}")
    except Exception as _exc:
        log.error(f"Failed to generate DAG '{_pipeline_name}': {_exc}")
