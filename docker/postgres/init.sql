-- ============================================================
-- Enterprise ETL Platform - PostgreSQL Schema
-- ============================================================

-- Create dedicated ETL user and database
CREATE USER etl_user WITH PASSWORD 'etl_pass';
CREATE DATABASE etl_db OWNER etl_user;
CREATE DATABASE airflow OWNER airflow;

-- Connect to etl_db
\c etl_db;

GRANT ALL PRIVILEGES ON DATABASE etl_db TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO etl_user;

-- ============================================================
-- PIPELINE RUNS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_id          UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    pipeline_name   VARCHAR(255) NOT NULL,
    dag_id          VARCHAR(255),
    dag_run_id      VARCHAR(255),
    status          VARCHAR(50) NOT NULL DEFAULT 'pending',
    -- Statuses: pending, running, success, failed, skipped, retrying
    triggered_by    VARCHAR(100) DEFAULT 'scheduler',
    config_snapshot JSONB,
    records_extracted   BIGINT DEFAULT 0,
    records_transformed BIGINT DEFAULT 0,
    records_loaded      BIGINT DEFAULT 0,
    records_failed      BIGINT DEFAULT 0,
    quality_score       NUMERIC(5,2),
    error_message   TEXT,
    started_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at     TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10,2),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_pipeline_runs_name ON pipeline_runs(pipeline_name);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX idx_pipeline_runs_started ON pipeline_runs(started_at DESC);
CREATE INDEX idx_pipeline_runs_run_id ON pipeline_runs(run_id);

-- ============================================================
-- PIPELINE TASKS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_tasks (
    id              SERIAL PRIMARY KEY,
    task_id         UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    task_name       VARCHAR(255) NOT NULL,
    task_type       VARCHAR(100),
    -- Types: extract, transform, load, validate, alert
    status          VARCHAR(50) NOT NULL DEFAULT 'pending',
    attempt_number  INTEGER DEFAULT 1,
    records_in      BIGINT DEFAULT 0,
    records_out     BIGINT DEFAULT 0,
    error_message   TEXT,
    metadata        JSONB,
    started_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at     TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10,2),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_pipeline_tasks_run_id ON pipeline_tasks(run_id);
CREATE INDEX idx_pipeline_tasks_status ON pipeline_tasks(status);

-- ============================================================
-- DATA QUALITY METRICS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id              SERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    pipeline_name   VARCHAR(255) NOT NULL,
    metric_name     VARCHAR(255) NOT NULL,
    metric_category VARCHAR(100),
    -- Categories: completeness, uniqueness, validity, consistency, timeliness
    passed          BOOLEAN,
    expected_value  NUMERIC,
    actual_value    NUMERIC,
    threshold       NUMERIC,
    details         JSONB,
    evaluated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_dq_metrics_run_id ON data_quality_metrics(run_id);
CREATE INDEX idx_dq_metrics_pipeline ON data_quality_metrics(pipeline_name);
CREATE INDEX idx_dq_metrics_category ON data_quality_metrics(metric_category);

-- ============================================================
-- PIPELINE LOGS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_logs (
    id              SERIAL PRIMARY KEY,
    run_id          UUID REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    task_id         UUID REFERENCES pipeline_tasks(task_id) ON DELETE SET NULL,
    pipeline_name   VARCHAR(255),
    log_level       VARCHAR(20) NOT NULL,
    -- Levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
    logger_name     VARCHAR(255),
    message         TEXT NOT NULL,
    extra_data      JSONB,
    logged_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_pipeline_logs_run_id ON pipeline_logs(run_id);
CREATE INDEX idx_pipeline_logs_level ON pipeline_logs(log_level);
CREATE INDEX idx_pipeline_logs_logged_at ON pipeline_logs(logged_at DESC);

-- ============================================================
-- ALERT HISTORY TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS alert_history (
    id              SERIAL PRIMARY KEY,
    run_id          UUID REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    alert_type      VARCHAR(50) NOT NULL,
    -- Types: email, slack, pagerduty
    severity        VARCHAR(20) NOT NULL,
    -- Severities: info, warning, error, critical
    subject         VARCHAR(500),
    message         TEXT,
    recipient       VARCHAR(255),
    sent_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    success         BOOLEAN DEFAULT TRUE,
    error_message   TEXT
);

-- ============================================================
-- DATA SOURCES REGISTRY
-- ============================================================
CREATE TABLE IF NOT EXISTS data_sources (
    id              SERIAL PRIMARY KEY,
    source_id       UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL UNIQUE,
    source_type     VARCHAR(100) NOT NULL,
    -- Types: postgresql, mysql, csv, api, s3, sftp
    connection_config JSONB NOT NULL,
    -- Sensitive fields should be stored as references to secrets manager
    is_active       BOOLEAN DEFAULT TRUE,
    last_tested_at  TIMESTAMP WITH TIME ZONE,
    last_test_status VARCHAR(50),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================
-- PIPELINE CONFIGURATIONS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_configs (
    id              SERIAL PRIMARY KEY,
    config_id       UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    pipeline_name   VARCHAR(255) NOT NULL UNIQUE,
    description     TEXT,
    config          JSONB NOT NULL,
    version         INTEGER DEFAULT 1,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================
-- CHECKPOINTS TABLE (for incremental loads)
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_checkpoints (
    id              SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(255) NOT NULL,
    source_name     VARCHAR(255) NOT NULL,
    checkpoint_key  VARCHAR(255) NOT NULL,
    checkpoint_value TEXT NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(pipeline_name, source_name, checkpoint_key)
);

-- ============================================================
-- VIEWS for easy querying
-- ============================================================

CREATE OR REPLACE VIEW v_pipeline_summary AS
SELECT
    pipeline_name,
    COUNT(*) AS total_runs,
    COUNT(*) FILTER (WHERE status = 'success') AS successful_runs,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
    COUNT(*) FILTER (WHERE status = 'running') AS running_runs,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'success')::NUMERIC
        / NULLIF(COUNT(*) FILTER (WHERE status IN ('success','failed')), 0) * 100,
    2) AS success_rate_pct,
    AVG(duration_seconds) FILTER (WHERE status = 'success') AS avg_duration_seconds,
    SUM(records_loaded) FILTER (WHERE status = 'success') AS total_records_loaded,
    MAX(started_at) AS last_run_at
FROM pipeline_runs
GROUP BY pipeline_name;

CREATE OR REPLACE VIEW v_recent_runs AS
SELECT
    pr.run_id,
    pr.pipeline_name,
    pr.status,
    pr.triggered_by,
    pr.records_extracted,
    pr.records_loaded,
    pr.records_failed,
    pr.quality_score,
    pr.duration_seconds,
    pr.started_at,
    pr.finished_at,
    pr.error_message
FROM pipeline_runs pr
ORDER BY pr.started_at DESC
LIMIT 100;

-- ============================================================
-- Trigger to auto-update updated_at
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_pipeline_runs_updated_at
    BEFORE UPDATE ON pipeline_runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_data_sources_updated_at
    BEFORE UPDATE ON data_sources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_pipeline_configs_updated_at
    BEFORE UPDATE ON pipeline_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- Seed sample pipeline configs
-- ============================================================
INSERT INTO pipeline_configs (pipeline_name, description, config) VALUES
(
    'customer_data_pipeline',
    'Extracts, cleans, and loads customer data from CSV source',
    '{
        "source": {"type": "csv", "path": "/opt/etl/data/sample_customers.csv"},
        "transformations": ["remove_duplicates", "fill_missing", "normalize_text", "validate_email"],
        "destination": {"type": "postgresql", "table": "clean_customers"},
        "schedule": "@daily",
        "quality_threshold": 0.85
    }'::JSONB
),
(
    'sales_metrics_pipeline',
    'Aggregates daily sales metrics from transactional DB',
    '{
        "source": {"type": "postgresql", "query": "SELECT * FROM raw_sales WHERE date >= :checkpoint"},
        "transformations": ["remove_duplicates", "fill_missing", "aggregate_daily"],
        "destination": {"type": "postgresql", "table": "sales_metrics"},
        "schedule": "0 1 * * *",
        "quality_threshold": 0.90,
        "incremental": true,
        "checkpoint_column": "date"
    }'::JSONB
);
