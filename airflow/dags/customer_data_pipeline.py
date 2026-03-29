"""
Enterprise ETL Platform - Customer Data Pipeline DAG

Full ETL DAG for customer data ingestion:
Extract CSV → Transform → Quality Check → Load → Alert

Features:
- Retry policies with exponential backoff
- SLA monitoring
- Email/Slack on failure
- Database run tracking via ETLOperator
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Default DAG Arguments ─────────────────────────────────────────────────────
default_args: Dict[str, Any] = {
    "owner": "etl-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,  # handled by our AlertService
    "email_on_retry": False,
    "execution_timeout": timedelta(hours=2),
}

# ── Pipeline Configuration ─────────────────────────────────────────────────────
PIPELINE_NAME = "customer_data_pipeline"


def run_extraction(**context) -> Dict[str, Any]:
    """
    Airflow task: Extract data from source.

    Pushes extracted record count to XCom for downstream tasks.
    """
    import sys
    sys.path.insert(0, "/opt/etl")

    from backend.core.extractors.factory import get_extractor
    from backend.core.config import settings

    log.info(f"Starting extraction for pipeline: {PIPELINE_NAME}")

    extractor_config = {
        "type": "csv",
        "name": PIPELINE_NAME,
        "path": "/opt/etl/data/sample_customers.csv",
    }

    extractor = get_extractor(extractor_config)
    with extractor:
        result = extractor.extract()

    # Store DataFrame as parquet in temp location for downstream tasks
    tmp_path = f"/tmp/{PIPELINE_NAME}_{context['run_id']}_raw.parquet"
    result.data.to_parquet(tmp_path, index=False)

    context["task_instance"].xcom_push(key="raw_data_path", value=tmp_path)
    context["task_instance"].xcom_push(key="records_extracted", value=result.records_count)

    log.info(f"Extraction complete: {result.records_count} records → {tmp_path}")
    return {"records_extracted": result.records_count, "path": tmp_path}


def run_transformation(**context) -> Dict[str, Any]:
    """Airflow task: Apply transformation rules to extracted data."""
    import sys
    import pandas as pd
    sys.path.insert(0, "/opt/etl")

    from backend.core.transformers.engine import TransformationEngine

    ti = context["task_instance"]
    raw_path = ti.xcom_pull(task_ids="extract", key="raw_data_path")

    df = pd.read_parquet(raw_path)
    log.info(f"Transforming {len(df)} records")

    rules = [
        {"name": "remove_duplicates"},
        {"name": "fill_missing", "config": {"strategy": "constant", "fill_value": "UNKNOWN"}},
        {"name": "normalize_text"},
        {"name": "validate_email", "config": {"column": "email"}},
        {"name": "add_audit_columns", "config": {"pipeline_name": PIPELINE_NAME}},
    ]

    engine = TransformationEngine(PIPELINE_NAME)
    result = engine.apply(df, rules, run_id=context["run_id"])

    tmp_path = f"/tmp/{PIPELINE_NAME}_{context['run_id']}_clean.parquet"
    result.data.to_parquet(tmp_path, index=False)

    ti.xcom_push(key="clean_data_path", value=tmp_path)
    ti.xcom_push(key="records_transformed", value=result.records_out)
    ti.xcom_push(key="records_dropped", value=result.records_dropped)

    log.info(f"Transformation complete: {result.records_out} records")
    return {"records_out": result.records_out, "rules_applied": result.rules_applied}


def run_quality_check(**context) -> Dict[str, Any]:
    """Airflow task: Score data quality and push metrics."""
    import sys
    import pandas as pd
    sys.path.insert(0, "/opt/etl")

    from backend.core.transformers.quality import DataQualityScorer

    ti = context["task_instance"]
    clean_path = ti.xcom_pull(task_ids="transform", key="clean_data_path")

    df = pd.read_parquet(clean_path)

    quality_config = {
        "max_null_ratio": 0.15,
        "max_duplicate_ratio": 0.02,
        "min_records": 10,
        "required_columns": ["email"],
    }

    scorer = DataQualityScorer(PIPELINE_NAME, quality_config)
    report = scorer.evaluate(df)

    ti.xcom_push(key="quality_score", value=float(report.composite_score))
    ti.xcom_push(key="quality_passed", value=report.passed_checks)
    ti.xcom_push(key="quality_failed", value=report.failed_checks)

    log.info(
        f"Quality score: {report.composite_score:.4f} | "
        f"Passed: {report.passed_checks} | Failed: {report.failed_checks}"
    )

    # Fail task if quality is critically low
    if report.composite_score < 0.50:
        raise ValueError(
            f"Quality score {report.composite_score:.2%} is critically low. Halting pipeline."
        )

    return report.to_dict()


def run_load(**context) -> Dict[str, Any]:
    """Airflow task: Load clean data to destination."""
    import sys
    import pandas as pd
    sys.path.insert(0, "/opt/etl")

    import os
    from backend.core.loaders.loader import get_loader

    ti = context["task_instance"]
    clean_path = ti.xcom_pull(task_ids="transform", key="clean_data_path")

    df = pd.read_parquet(clean_path)

    loader_config = {
        "type": "postgresql",
        "connection_string": os.environ.get(
            "ETL_DB_URL", "postgresql://etl_user:etl_pass@postgres:5432/etl_db"
        ),
        "table": "clean_customers",
        "mode": "append",
        "chunksize": 5000,
    }

    loader = get_loader(loader_config)
    result = loader.load(df)

    if not result.success:
        raise RuntimeError(f"Load failed: {result.error}")

    ti.xcom_push(key="records_loaded", value=result.records_loaded)
    log.info(f"Load complete: {result.records_loaded} records → {result.destination}")

    return {"records_loaded": result.records_loaded}


def persist_run_metrics(**context) -> None:
    """Airflow task: Persist final run metrics to etl_db."""
    import sys
    sys.path.insert(0, "/opt/etl")

    import os
    from sqlalchemy import create_engine, text

    ti = context["task_instance"]
    records_extracted = ti.xcom_pull(task_ids="extract", key="records_extracted") or 0
    records_transformed = ti.xcom_pull(task_ids="transform", key="records_transformed") or 0
    records_loaded = ti.xcom_pull(task_ids="load", key="records_loaded") or 0
    quality_score = ti.xcom_pull(task_ids="quality_check", key="quality_score") or 0.0

    db_url = os.environ.get("ETL_DB_URL", "postgresql://etl_user:etl_pass@postgres:5432/etl_db")
    engine = create_engine(db_url)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_runs (
                    pipeline_name, dag_id, dag_run_id, status, triggered_by,
                    records_extracted, records_transformed, records_loaded,
                    quality_score, finished_at, duration_seconds
                ) VALUES (
                    :pipeline_name, :dag_id, :dag_run_id, 'success', 'airflow',
                    :records_extracted, :records_transformed, :records_loaded,
                    :quality_score, NOW(),
                    EXTRACT(EPOCH FROM (NOW() - :start_date))
                )
            """),
            {
                "pipeline_name": PIPELINE_NAME,
                "dag_id": context["dag"].dag_id,
                "dag_run_id": context["run_id"],
                "records_extracted": records_extracted,
                "records_transformed": records_transformed,
                "records_loaded": records_loaded,
                "quality_score": quality_score * 100,
                "start_date": context["data_interval_start"],
            },
        )
    engine.dispose()
    log.info("Run metrics persisted to database")


def handle_pipeline_failure(**context) -> None:
    """Airflow task: Triggered on failure — sends alert."""
    import sys
    sys.path.insert(0, "/opt/etl")

    from backend.services.alert_service import alert_service

    alert_service.send_pipeline_failure(
        pipeline_name=PIPELINE_NAME,
        run_id=context["run_id"],
        error=str(context.get("exception", "Unknown error")),
        details={
            "dag_id": context["dag"].dag_id,
            "execution_date": str(context.get("data_interval_start")),
        },
    )
    log.error(f"Pipeline {PIPELINE_NAME} FAILED. Alert dispatched.")


# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="customer_data_pipeline",
    description="End-to-end ETL for customer data: CSV → clean → PostgreSQL",
    default_args=default_args,
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "customer", "production"],
    doc_md="""
    ## Customer Data Pipeline

    Ingests raw customer CSV data, applies quality cleaning rules,
    scores data quality, and loads to the `clean_customers` PostgreSQL table.

    ### Schedule
    Runs daily at midnight UTC.

    ### SLAs
    - Total runtime: < 2 hours
    - Records loaded per run: > 100
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract",
        python_callable=run_extraction,
        provide_context=True,
        doc_md="Extract raw customer CSV into staging parquet.",
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=run_transformation,
        provide_context=True,
        doc_md="Apply cleaning and normalization transformations.",
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=run_quality_check,
        provide_context=True,
        doc_md="Score data quality. Fail if composite score < 50%.",
    )

    load = PythonOperator(
        task_id="load",
        python_callable=run_load,
        provide_context=True,
        doc_md="Load clean records to PostgreSQL clean_customers table.",
    )

    persist_metrics = PythonOperator(
        task_id="persist_metrics",
        python_callable=persist_run_metrics,
        provide_context=True,
        doc_md="Write final run stats to pipeline_runs table.",
    )

    end = EmptyOperator(task_id="end")

    on_failure = PythonOperator(
        task_id="on_failure_alert",
        python_callable=handle_pipeline_failure,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,
        doc_md="Send failure alert via Email/Slack.",
    )

    # ── Task Dependencies ──────────────────────────────────────────────────
    (
        start
        >> extract
        >> transform
        >> quality_check
        >> load
        >> persist_metrics
        >> end
    )

    # Failure alert runs if ANY task fails
    [extract, transform, quality_check, load] >> on_failure
