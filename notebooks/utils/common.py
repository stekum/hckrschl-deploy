# Databricks notebook source
"""
Common Utility Functions for Medallion Architecture

This module provides reusable functions for:
- Configuration management
- Data quality validation
- Hash calculation
- Metrics collection
- Logging

Author: Generated from deployment plan
Last Updated: 2025-10-26
"""

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import *
from datetime import datetime
import hashlib
import json
import uuid
import re


# COMMAND ----------

# ================================================================
# Configuration Management
# ================================================================

def load_config(environment="dev"):
    """
    Load configuration for specified environment
    In real implementation, this would read from config files
    For now, returns hardcoded values
    """
    configs = {
        "dev": {
            "bronze_lakehouse": "hs_bronze_dev",
            "silver_lakehouse": "hs_silver_dev",
            "gold_lakehouse": "hs_gold_dev",
            "monitoring_lakehouse": "hs_monitoring_dev",
            "error_threshold": 0.10,
            "batch_size": 1000
        },
        "test": {
            "bronze_lakehouse": "hs_bronze_test",
            "silver_lakehouse": "hs_silver_test",
            "gold_lakehouse": "hs_gold_test",
            "monitoring_lakehouse": "hs_monitoring_test",
            "error_threshold": 0.05,
            "batch_size": 5000
        },
        "prod": {
            "bronze_lakehouse": "hs_bronze_prod",
            "silver_lakehouse": "hs_silver_prod",
            "gold_lakehouse": "hs_gold_prod",
            "monitoring_lakehouse": "hs_monitoring_prod",
            "error_threshold": 0.02,
            "batch_size": 10000
        }
    }

    return configs.get(environment, configs["dev"])


def get_table_path(lakehouse, table_name):
    """
    Construct full table path
    """
    return f"{lakehouse}.{table_name}"


# COMMAND ----------

# ================================================================
# Data Quality Validation
# ================================================================

def validate_email(email):
    """
    Validate email address format
    """
    if email is None or email == "":
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, str(email)))


def validate_phone_de(phone):
    """
    Validate German phone number format
    Optional field - returns True if None
    """
    if phone is None or phone == "":
        return True
    phone_clean = str(phone).replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    pattern = r'^(\+49|0)[1-9]\d{8,14}$'
    return bool(re.match(pattern, phone_clean))


def validate_not_null(value):
    """
    Check if value is not null or empty
    """
    if value is None or str(value).strip() == "":
        return False
    return True


def validate_enum(value, allowed_values):
    """
    Check if value is in allowed list
    """
    if value is None:
        return True  # NULL is allowed unless explicitly checked with validate_not_null
    return str(value) in allowed_values


def validate_date_range(start_date, end_date):
    """
    Check if start_date < end_date
    """
    if start_date is None or end_date is None:
        return False
    return start_date < end_date


# Register UDFs
def register_validation_udfs(spark):
    """
    Register all validation functions as Spark UDFs
    """
    spark.udf.register("validate_email", validate_email, BooleanType())
    spark.udf.register("validate_phone_de", validate_phone_de, BooleanType())
    spark.udf.register("validate_not_null", validate_not_null, BooleanType())

    return {
        "validate_email": F.udf(validate_email, BooleanType()),
        "validate_phone": F.udf(validate_phone_de, BooleanType()),
        "validate_not_null": F.udf(validate_not_null, BooleanType())
    }


# COMMAND ----------

# ================================================================
# Hash Calculation
# ================================================================

def calculate_hash(df, columns):
    """
    Calculate MD5 hash of specified columns for change detection
    Used in SCD Type 2 implementations

    Args:
        df: DataFrame
        columns: List of column names to include in hash

    Returns:
        DataFrame with 'hash_value' column added
    """
    # Concatenate all columns with | separator, handling nulls
    concat_expr = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns])

    # Calculate MD5 hash
    return df.withColumn("hash_value", F.md5(concat_expr))


def calculate_row_hash(row_dict):
    """
    Calculate hash for a single row (Python dict)
    Useful for testing or small datasets
    """
    values = [str(row_dict.get(k, "NULL")) for k in sorted(row_dict.keys())]
    concat_str = "|".join(values)
    return hashlib.md5(concat_str.encode()).hexdigest()


# COMMAND ----------

# ================================================================
# Data Transformations
# ================================================================

def apply_standard_transformations(df, transformations_config):
    """
    Apply standard transformations based on config

    Example config:
    [
        {"field": "email", "operations": ["trim", "lowercase"]},
        {"field": "name", "operations": ["trim"]}
    ]
    """
    result_df = df

    for trans in transformations_config:
        field = trans["field"]
        operations = trans.get("operations", [])

        for op in operations:
            if op == "trim":
                result_df = result_df.withColumn(field, F.trim(F.col(field)))
            elif op == "lowercase":
                result_df = result_df.withColumn(field, F.lower(F.col(field)))
            elif op == "uppercase":
                result_df = result_df.withColumn(field, F.upper(F.col(field)))
            elif op == "remove_whitespace":
                result_df = result_df.withColumn(field, F.regexp_replace(F.col(field), r"\s+", ""))

    return result_df


def deduplicate_dataframe(df, key_columns, order_by_column, order="desc"):
    """
    Deduplicate DataFrame keeping latest/first record per key

    Args:
        df: Input DataFrame
        key_columns: List of columns that define uniqueness
        order_by_column: Column to determine which record to keep
        order: 'desc' (latest) or 'asc' (first)
    """
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy(*key_columns).orderBy(
        F.desc(order_by_column) if order == "desc" else F.asc(order_by_column)
    )

    return df.withColumn("_row_num", F.row_number().over(window_spec)) \
             .filter(F.col("_row_num") == 1) \
             .drop("_row_num")


# COMMAND ----------

# ================================================================
# Metrics Collection
# ================================================================

def create_metrics_record(run_id, pipeline_name, stage, source_table, target_table,
                          records_read, records_written, records_failed,
                          start_time, end_time,
                          scd_inserts=None, scd_updates=None, scd_no_changes=None):
    """
    Create a metrics record dictionary

    Returns:
        Dictionary ready to be converted to DataFrame
    """
    duration_sec = (end_time - start_time).total_seconds()
    error_rate = records_failed / records_read if records_read > 0 else 0.0

    return {
        "metric_id": str(uuid.uuid4()),
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "stage": stage,
        "source_table": source_table,
        "target_table": target_table,
        "records_read": records_read,
        "records_written": records_written,
        "records_failed": records_failed,
        "duration_sec": duration_sec,
        "error_rate": error_rate,
        "dq_score_avg": 1.0 - error_rate,
        "scd_inserts": scd_inserts,
        "scd_updates": scd_updates,
        "scd_no_changes": scd_no_changes,
        "start_time": start_time,
        "end_time": end_time,
        "created_at": datetime.now()
    }


def write_metrics(spark, metrics_dict, metrics_table):
    """
    Write metrics to monitoring table
    """
    try:
        metrics_df = spark.createDataFrame([metrics_dict])
        metrics_df.write.mode("append").saveAsTable(metrics_table)
        return True
    except Exception as e:
        print(f"⚠️  Warning: Could not write metrics: {str(e)}")
        return False


# COMMAND ----------

# ================================================================
# Quarantine Management
# ================================================================

def create_quarantine_records(df_invalid, source_table, run_id, error_type_expr, error_message_expr, error_field_expr, record_columns):
    """
    Create quarantine records from invalid data

    Args:
        df_invalid: DataFrame with invalid records
        source_table: Source table name
        run_id: Pipeline run ID
        error_type_expr: PySpark expression for error type
        error_message_expr: PySpark expression for error message
        error_field_expr: PySpark expression for error field
        record_columns: List of columns to include in raw_record JSON
    """
    return df_invalid.select(
        F.expr("uuid()").alias("quarantine_id"),
        F.lit(source_table).alias("source_table"),
        F.lit(run_id).alias("pipeline_run_id"),
        error_type_expr.alias("error_type"),
        error_message_expr.alias("error_message"),
        error_field_expr.alias("error_field"),
        F.to_json(F.struct(*record_columns)).alias("raw_record"),
        F.lit("new").alias("status"),
        F.lit(None).cast("string").alias("resolution_notes"),
        F.lit(None).cast("string").alias("resolved_by"),
        F.lit(None).cast("timestamp").alias("resolved_at"),
        F.current_timestamp().alias("created_at")
    )


def write_quarantine(spark, df_quarantine, quarantine_table):
    """
    Write quarantine records to monitoring table
    """
    try:
        if df_quarantine.count() > 0:
            df_quarantine.write.mode("append").saveAsTable(quarantine_table)
            return True
        return True
    except Exception as e:
        print(f"⚠️  Warning: Could not write quarantine: {str(e)}")
        return False


# COMMAND ----------

# ================================================================
# Logging & Reporting
# ================================================================

def print_pipeline_summary(pipeline_name, run_id, environment,
                          records_read, records_written, records_failed,
                          duration_sec, additional_info=None):
    """
    Print standardized pipeline summary
    """
    error_rate = records_failed / records_read if records_read > 0 else 0.0

    print("\n" + "="*60)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Pipeline: {pipeline_name}")
    print(f"Run ID: {run_id}")
    print(f"Environment: {environment}")
    print(f"\n📥 INPUT")
    print(f"  Records read: {records_read:,}")
    print(f"\n📤 OUTPUT")
    print(f"  Records written: {records_written:,}")
    print(f"  Records failed: {records_failed:,}")
    print(f"\n⏱️  PERFORMANCE")
    print(f"  Duration: {duration_sec:.2f} seconds")
    if duration_sec > 0:
        print(f"  Records/sec: {records_read/duration_sec:.0f}")
    print(f"\n📊 QUALITY")
    print(f"  Error rate: {error_rate*100:.2f}%")
    print(f"  DQ score: {(1-error_rate)*100:.2f}%")

    if additional_info:
        print(f"\n📋 ADDITIONAL INFO")
        for key, value in additional_info.items():
            print(f"  {key}: {value}")

    print("="*60)


# COMMAND ----------

# ================================================================
# Testing Helpers
# ================================================================

def create_test_data(spark, schema, data):
    """
    Create test DataFrame for unit testing

    Example:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType())
        ])
        data = [(1, "Alice"), (2, "Bob")]
        df = create_test_data(spark, schema, data)
    """
    return spark.createDataFrame(data, schema)


def compare_dataframes(df1, df2, key_columns):
    """
    Compare two DataFrames and return differences
    Useful for testing
    """
    # Records in df1 but not in df2
    only_in_df1 = df1.join(df2, key_columns, "left_anti")

    # Records in df2 but not in df1
    only_in_df2 = df2.join(df1, key_columns, "left_anti")

    return {
        "only_in_df1": only_in_df1.count(),
        "only_in_df2": only_in_df2.count(),
        "df1_sample": only_in_df1,
        "df2_sample": only_in_df2
    }


# COMMAND ----------

print("✅ Common utilities loaded successfully")
