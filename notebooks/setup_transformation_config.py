# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Transformation Configuration Tables
# MAGIC
# MAGIC **Purpose**: Create config-driven transformation framework tables
# MAGIC
# MAGIC **IMPORTANT**: Run this notebook in Microsoft Fabric with lakehouse attached
# MAGIC **Lakehouse**: hs_silver_dev (dev), hs_silver_test (test), hs_silver_prod (prod)
# MAGIC
# MAGIC **Author**: Generated for Fabric compatibility
# MAGIC **Last Updated**: 2025-10-27

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

lakehouse_name = spark.conf.get("spark.lakehouse.name", "hs_silver_dev")
print(f"Creating tables in lakehouse: {lakehouse_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Transformation Config Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS transformation_config (
# MAGIC     -- Identity
# MAGIC     config_id STRING NOT NULL,
# MAGIC
# MAGIC     -- Source & Target
# MAGIC     source_table STRING NOT NULL,
# MAGIC     target_table STRING NOT NULL,
# MAGIC     target_layer STRING NOT NULL,
# MAGIC
# MAGIC     -- Transformation Type
# MAGIC     rule_type STRING NOT NULL,
# MAGIC     rule_definition STRING NOT NULL,
# MAGIC
# MAGIC     -- Execution
# MAGIC     priority INT,
# MAGIC     active BOOLEAN,
# MAGIC     batch_size INT,
# MAGIC
# MAGIC     -- Dependencies
# MAGIC     depends_on STRING,
# MAGIC
# MAGIC     -- Metadata
# MAGIC     created_at TIMESTAMP NOT NULL,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     created_by STRING,
# MAGIC     description STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Configuration for data transformations across medallion layers'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

print("✅ transformation_config table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Quality Whitelist Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dq_whitelist (
# MAGIC     -- Identity
# MAGIC     whitelist_id STRING NOT NULL,
# MAGIC
# MAGIC     -- Rule Info
# MAGIC     table_name STRING NOT NULL,
# MAGIC     field_name STRING,
# MAGIC     rule_type STRING NOT NULL,
# MAGIC
# MAGIC     -- Whitelist Pattern
# MAGIC     pattern STRING NOT NULL,
# MAGIC     reason STRING,
# MAGIC
# MAGIC     -- Status
# MAGIC     active BOOLEAN,
# MAGIC     expires_at TIMESTAMP,
# MAGIC
# MAGIC     -- Metadata
# MAGIC     created_at TIMESTAMP NOT NULL,
# MAGIC     created_by STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Whitelist for data quality rule exceptions'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

print("✅ dq_whitelist table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ TRANSFORMATION CONFIG TABLES SETUP COMPLETE")
print("="*60)
print("\nTables Created:")
print("  1. transformation_config")
print("  2. dq_whitelist")
print("="*60)
print("\nNext Step: Run 'seed_transformation_config' notebook to load initial data")

dbutils.notebook.exit("success")
