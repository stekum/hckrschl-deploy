# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Monitoring Tables
# MAGIC
# MAGIC **Purpose**: Create monitoring and observability tables in Fabric Lakehouse
# MAGIC
# MAGIC **IMPORTANT**: Run this notebook in Microsoft Fabric with lakehouse attached
# MAGIC **Lakehouse**: hs_monitoring_dev (dev), hs_monitoring_test (test), hs_monitoring_prod (prod)
# MAGIC
# MAGIC **Author**: Generated for Fabric compatibility
# MAGIC **Last Updated**: 2025-10-27

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set lakehouse context - will use attached lakehouse
lakehouse_name = spark.conf.get("spark.lakehouse.name", "hs_monitoring_dev")
print(f"Creating tables in lakehouse: {lakehouse_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline Metrics Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS pipeline_metrics (
# MAGIC     -- Identity
# MAGIC     metric_id STRING NOT NULL,
# MAGIC     run_id STRING NOT NULL,
# MAGIC     pipeline_name STRING,
# MAGIC
# MAGIC     -- Stage & Source
# MAGIC     stage STRING NOT NULL,
# MAGIC     source_table STRING NOT NULL,
# MAGIC     target_table STRING NOT NULL,
# MAGIC
# MAGIC     -- Performance Metrics
# MAGIC     records_read LONG,
# MAGIC     records_written LONG,
# MAGIC     records_failed LONG,
# MAGIC     duration_sec DOUBLE,
# MAGIC
# MAGIC     -- Quality Metrics
# MAGIC     error_rate DOUBLE,
# MAGIC     dq_score_avg DOUBLE,
# MAGIC
# MAGIC     -- SCD Metrics (for Gold layer)
# MAGIC     scd_inserts LONG,
# MAGIC     scd_updates LONG,
# MAGIC     scd_no_changes LONG,
# MAGIC
# MAGIC     -- Timestamps
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC     created_at TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Pipeline execution metrics across all medallion stages'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

print("✅ pipeline_metrics table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quarantine Records Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS quarantine_records (
# MAGIC     -- Identity
# MAGIC     quarantine_id STRING NOT NULL,
# MAGIC
# MAGIC     -- Source Information
# MAGIC     source_table STRING NOT NULL,
# MAGIC     pipeline_run_id STRING NOT NULL,
# MAGIC
# MAGIC     -- Error Details
# MAGIC     error_type STRING NOT NULL,
# MAGIC     error_message STRING,
# MAGIC     error_field STRING,
# MAGIC
# MAGIC     -- Data
# MAGIC     raw_record STRING NOT NULL,
# MAGIC
# MAGIC     -- Status & Resolution (NO DEFAULT - set in application)
# MAGIC     status STRING,
# MAGIC     resolution_notes STRING,
# MAGIC     resolved_by STRING,
# MAGIC     resolved_at TIMESTAMP,
# MAGIC
# MAGIC     -- Metadata
# MAGIC     created_at TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Failed records quarantine for data quality issues'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

print("✅ quarantine_records table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Rules Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dq_rules (
# MAGIC     -- Identity
# MAGIC     rule_id STRING NOT NULL,
# MAGIC     rule_name STRING NOT NULL,
# MAGIC
# MAGIC     -- Rule Definition
# MAGIC     table_name STRING NOT NULL,
# MAGIC     field_name STRING,
# MAGIC     rule_type STRING NOT NULL,
# MAGIC     rule_definition STRING NOT NULL,
# MAGIC
# MAGIC     -- Severity & Action
# MAGIC     severity STRING,
# MAGIC     error_action STRING,
# MAGIC
# MAGIC     -- Status
# MAGIC     active BOOLEAN,
# MAGIC
# MAGIC     -- Metadata
# MAGIC     created_at TIMESTAMP NOT NULL,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     created_by STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Data quality validation rules configuration'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

print("✅ dq_rules table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pipeline Run History Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS pipeline_run_history (
# MAGIC     -- Identity
# MAGIC     run_id STRING NOT NULL,
# MAGIC     pipeline_name STRING NOT NULL,
# MAGIC
# MAGIC     -- Execution Info
# MAGIC     run_status STRING NOT NULL,
# MAGIC     run_type STRING,
# MAGIC
# MAGIC     -- Timing
# MAGIC     start_time TIMESTAMP NOT NULL,
# MAGIC     end_time TIMESTAMP,
# MAGIC     duration_sec DOUBLE,
# MAGIC
# MAGIC     -- Results Summary
# MAGIC     total_records_processed LONG,
# MAGIC     total_records_failed LONG,
# MAGIC     tables_processed INT,
# MAGIC
# MAGIC     -- Error Info
# MAGIC     error_message STRING,
# MAGIC     error_stack_trace STRING,
# MAGIC
# MAGIC     -- Metadata
# MAGIC     triggered_by STRING,
# MAGIC     environment STRING,
# MAGIC     fabric_workspace STRING,
# MAGIC     created_at TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'High-level pipeline execution history'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

print("✅ pipeline_run_history table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Table Lineage Tracking

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS table_lineage (
# MAGIC     -- Identity
# MAGIC     lineage_id STRING NOT NULL,
# MAGIC
# MAGIC     -- Lineage Info
# MAGIC     source_table STRING NOT NULL,
# MAGIC     target_table STRING NOT NULL,
# MAGIC     transformation_type STRING,
# MAGIC
# MAGIC     -- Execution Info
# MAGIC     run_id STRING,
# MAGIC     records_transferred LONG,
# MAGIC
# MAGIC     -- Timestamps
# MAGIC     created_at TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Data lineage tracking across medallion architecture'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

print("✅ table_lineage table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views for Common Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: Latest Pipeline Metrics (Last 7 Days)
# MAGIC CREATE OR REPLACE VIEW v_pipeline_metrics_recent AS
# MAGIC SELECT
# MAGIC     DATE(start_time) as run_date,
# MAGIC     stage,
# MAGIC     source_table,
# MAGIC     SUM(records_read) as total_records_read,
# MAGIC     SUM(records_written) as total_records_written,
# MAGIC     SUM(records_failed) as total_records_failed,
# MAGIC     AVG(error_rate) as avg_error_rate,
# MAGIC     AVG(duration_sec) as avg_duration_sec,
# MAGIC     COUNT(*) as run_count
# MAGIC FROM pipeline_metrics
# MAGIC WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY DATE(start_time), stage, source_table
# MAGIC ORDER BY run_date DESC, source_table;

# COMMAND ----------

print("✅ v_pipeline_metrics_recent view created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: Quarantine Summary by Error Type
# MAGIC CREATE OR REPLACE VIEW v_quarantine_summary AS
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     error_type,
# MAGIC     status,
# MAGIC     COUNT(*) as error_count,
# MAGIC     MIN(created_at) as first_occurrence,
# MAGIC     MAX(created_at) as last_occurrence
# MAGIC FROM quarantine_records
# MAGIC WHERE created_at >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY source_table, error_type, status
# MAGIC ORDER BY error_count DESC;

# COMMAND ----------

print("✅ v_quarantine_summary view created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: Pipeline Success Rate (Last 30 Days)
# MAGIC CREATE OR REPLACE VIEW v_pipeline_success_rate AS
# MAGIC SELECT
# MAGIC     pipeline_name,
# MAGIC     DATE(start_time) as run_date,
# MAGIC     COUNT(*) as total_runs,
# MAGIC     SUM(CASE WHEN run_status = 'succeeded' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC     SUM(CASE WHEN run_status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
# MAGIC     ROUND(SUM(CASE WHEN run_status = 'succeeded' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_pct
# MAGIC FROM pipeline_run_history
# MAGIC WHERE start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY pipeline_name, DATE(start_time)
# MAGIC ORDER BY run_date DESC, pipeline_name;

# COMMAND ----------

print("✅ v_pipeline_success_rate view created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all tables created
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify table properties
# MAGIC DESCRIBE EXTENDED pipeline_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ MONITORING TABLES SETUP COMPLETE")
print("="*60)
print("\nTables Created:")
print("  1. pipeline_metrics")
print("  2. quarantine_records")
print("  3. dq_rules")
print("  4. pipeline_run_history")
print("  5. table_lineage")
print("\nViews Created:")
print("  1. v_pipeline_metrics_recent")
print("  2. v_quarantine_summary")
print("  3. v_pipeline_success_rate")
print("="*60)

# Return success
dbutils.notebook.exit("success")
