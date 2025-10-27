-- ================================================================
-- Monitoring & Observability Schema
-- Purpose: Track pipeline execution metrics and data quality issues
-- Lakehouse: hs_monitoring_dev (dev), hs_monitoring_test (test), hs_monitoring_prod (prod)
-- ================================================================

-- ================================================================
-- 1. Pipeline Metrics Table
-- Tracks performance and data flow metrics across all stages
-- ================================================================
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    -- Identity
    metric_id STRING NOT NULL,
    run_id STRING NOT NULL,
    pipeline_name STRING,

    -- Stage & Source
    stage STRING NOT NULL,              -- 'bronze_to_silver', 'silver_to_gold'
    source_table STRING NOT NULL,       -- 'contact', 'session', 'booking'
    target_table STRING NOT NULL,

    -- Performance Metrics
    records_read LONG,
    records_written LONG,
    records_failed LONG,
    duration_sec DOUBLE,

    -- Quality Metrics
    error_rate DOUBLE,                  -- records_failed / records_read
    dq_score_avg DOUBLE,                -- Average data quality score

    -- SCD Metrics (for Gold layer)
    scd_inserts LONG,                   -- New records
    scd_updates LONG,                   -- Changed records (Type 2)
    scd_no_changes LONG,                -- Unchanged records

    -- Timestamps
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Pipeline execution metrics across all medallion stages';

-- Add optimizations
ALTER TABLE pipeline_metrics
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);


-- ================================================================
-- 2. Quarantine Records Table
-- Stores failed records for analysis and reprocessing
-- ================================================================
CREATE TABLE IF NOT EXISTS quarantine_records (
    -- Identity
    quarantine_id STRING NOT NULL,

    -- Source Information
    source_table STRING NOT NULL,
    pipeline_run_id STRING NOT NULL,

    -- Error Details
    error_type STRING NOT NULL,         -- 'email_invalid', 'phone_invalid', 'null_constraint', etc.
    error_message STRING,
    error_field STRING,                 -- Which field caused the error

    -- Data
    raw_record STRING NOT NULL,         -- JSON of the failed record

    -- Status & Resolution
    status STRING DEFAULT 'new',        -- 'new', 'reviewed', 'fixed', 'ignored'
    resolution_notes STRING,
    resolved_by STRING,
    resolved_at TIMESTAMP,

    -- Metadata
    created_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Failed records quarantine for data quality issues';

-- Add optimizations
ALTER TABLE quarantine_records
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);


-- ================================================================
-- 3. Data Quality Rules Table
-- Stores validation rules and their execution results
-- ================================================================
CREATE TABLE IF NOT EXISTS dq_rules (
    -- Identity
    rule_id STRING NOT NULL,
    rule_name STRING NOT NULL,

    -- Rule Definition
    table_name STRING NOT NULL,
    field_name STRING,
    rule_type STRING NOT NULL,          -- 'regex', 'not_null', 'range', 'enum', 'custom'
    rule_definition STRING NOT NULL,    -- JSON with rule details

    -- Severity & Action
    severity STRING DEFAULT 'error',    -- 'error', 'warning', 'info'
    error_action STRING DEFAULT 'quarantine', -- 'quarantine', 'reject', 'flag'

    -- Status
    active BOOLEAN DEFAULT true,

    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    created_by STRING
)
USING DELTA
COMMENT 'Data quality validation rules configuration';


-- ================================================================
-- 4. Pipeline Run History Table
-- High-level pipeline execution tracking
-- ================================================================
CREATE TABLE IF NOT EXISTS pipeline_run_history (
    -- Identity
    run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,

    -- Execution Info
    run_status STRING NOT NULL,         -- 'running', 'succeeded', 'failed', 'cancelled'
    run_type STRING,                    -- 'scheduled', 'manual', 'retry'

    -- Timing
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_sec DOUBLE,

    -- Results Summary
    total_records_processed LONG,
    total_records_failed LONG,
    tables_processed INT,

    -- Error Info
    error_message STRING,
    error_stack_trace STRING,

    -- Metadata
    triggered_by STRING,
    environment STRING,                 -- 'dev', 'test', 'prod'
    fabric_workspace STRING,

    created_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'High-level pipeline execution history';

-- Add optimizations and partitioning
ALTER TABLE pipeline_run_history
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);


-- ================================================================
-- 5. Table Lineage Tracking
-- Track data flow and dependencies between tables
-- ================================================================
CREATE TABLE IF NOT EXISTS table_lineage (
    -- Identity
    lineage_id STRING NOT NULL,

    -- Lineage Info
    source_table STRING NOT NULL,
    target_table STRING NOT NULL,
    transformation_type STRING,         -- 'dq_check', 'scd2', 'aggregate', 'join'

    -- Execution Info
    run_id STRING,
    records_transferred LONG,

    -- Timestamps
    created_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Data lineage tracking across medallion architecture';


-- ================================================================
-- Views for Common Queries
-- ================================================================

-- View: Latest Pipeline Metrics (Last 7 Days)
CREATE OR REPLACE VIEW v_pipeline_metrics_recent AS
SELECT
    DATE(start_time) as run_date,
    stage,
    source_table,
    SUM(records_read) as total_records_read,
    SUM(records_written) as total_records_written,
    SUM(records_failed) as total_records_failed,
    AVG(error_rate) as avg_error_rate,
    AVG(duration_sec) as avg_duration_sec,
    COUNT(*) as run_count
FROM pipeline_metrics
WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(start_time), stage, source_table
ORDER BY run_date DESC, source_table;


-- View: Quarantine Summary by Error Type
CREATE OR REPLACE VIEW v_quarantine_summary AS
SELECT
    source_table,
    error_type,
    status,
    COUNT(*) as error_count,
    MIN(created_at) as first_occurrence,
    MAX(created_at) as last_occurrence
FROM quarantine_records
WHERE created_at >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY source_table, error_type, status
ORDER BY error_count DESC;


-- View: Pipeline Success Rate (Last 30 Days)
CREATE OR REPLACE VIEW v_pipeline_success_rate AS
SELECT
    pipeline_name,
    DATE(start_time) as run_date,
    COUNT(*) as total_runs,
    SUM(CASE WHEN run_status = 'succeeded' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN run_status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(SUM(CASE WHEN run_status = 'succeeded' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_pct
FROM pipeline_run_history
WHERE start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY pipeline_name, DATE(start_time)
ORDER BY run_date DESC, pipeline_name;


-- ================================================================
-- Sample Queries for Power BI / Monitoring Dashboards
-- ================================================================

/*
-- Query 1: Pipeline Funnel (Bronze → Silver → Gold)
SELECT
    source_table,
    stage,
    SUM(records_read) as input_records,
    SUM(records_written) as output_records,
    SUM(records_failed) as failed_records,
    ROUND(AVG(error_rate) * 100, 2) as error_rate_pct
FROM pipeline_metrics
WHERE DATE(start_time) = CURRENT_DATE()
GROUP BY source_table, stage
ORDER BY source_table,
    CASE stage
        WHEN 'bronze_to_silver' THEN 1
        WHEN 'silver_to_gold' THEN 2
    END;


-- Query 2: Top Errors Needing Attention
SELECT
    source_table,
    error_type,
    COUNT(*) as error_count,
    MAX(created_at) as last_seen
FROM quarantine_records
WHERE status = 'new'
  AND created_at >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY source_table, error_type
ORDER BY error_count DESC
LIMIT 10;


-- Query 3: SCD Activity Dashboard
SELECT
    DATE(start_time) as activity_date,
    source_table,
    SUM(scd_inserts) as new_records,
    SUM(scd_updates) as changed_records,
    SUM(scd_no_changes) as unchanged_records,
    SUM(scd_inserts + scd_updates) as total_changes
FROM pipeline_metrics
WHERE stage = 'silver_to_gold'
  AND start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY DATE(start_time), source_table
ORDER BY activity_date DESC, source_table;
*/
