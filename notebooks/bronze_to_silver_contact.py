# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: Contact Table
# MAGIC
# MAGIC **Purpose**: Clean and validate contact data from Bronze to Silver layer
# MAGIC
# MAGIC **Process**:
# MAGIC 1. Read from Bronze (Dataverse linked table)
# MAGIC 2. Apply Data Quality checks (email, phone, required fields)
# MAGIC 3. Split: Valid → Silver, Invalid → Quarantine
# MAGIC 4. Apply transformations (trim, lowercase)
# MAGIC 5. Deduplicate based on contactid
# MAGIC 6. Write metrics to monitoring table
# MAGIC
# MAGIC **Data Quality Rules**:
# MAGIC - Email: Valid email format (regex)
# MAGIC - Phone: German phone format (optional)
# MAGIC - First/Last Name: Not null
# MAGIC
# MAGIC **Author**: Generated from deployment plan
# MAGIC **Last Updated**: 2025-10-26

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
import hashlib
import json
import re
import uuid

# COMMAND ----------

# Configuration - these will be parameterized later
# For now, hardcoded for MVP speed (as per deployment plan)

# Environment - can be passed as parameter
dbutils.widgets.text("environment", "dev", "Environment (dev/test/prod)")
environment = dbutils.widgets.get("environment")

# Lakehouse names
BRONZE_LAKEHOUSE = f"hs_bronze_{environment}"
SILVER_LAKEHOUSE = f"hs_silver_{environment}"
MONITORING_LAKEHOUSE = f"hs_monitoring_{environment}"

# Table names
SOURCE_TABLE = f"{BRONZE_LAKEHOUSE}.contact"
TARGET_TABLE = f"{SILVER_LAKEHOUSE}.contact"
QUARANTINE_TABLE = f"{MONITORING_LAKEHOUSE}.quarantine_records"
METRICS_TABLE = f"{MONITORING_LAKEHOUSE}.pipeline_metrics"

# Run metadata
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
PIPELINE_NAME = "bronze_to_silver_contact"

print(f"🚀 Starting {PIPELINE_NAME}")
print(f"   Environment: {environment}")
print(f"   Run ID: {RUN_ID}")
print(f"   Source: {SOURCE_TABLE}")
print(f"   Target: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Quality Validation Functions

# COMMAND ----------

def validate_email(email):
    """
    Validate email address format
    Returns: True if valid, False otherwise
    """
    if email is None or email == "":
        return False

    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, str(email)))


def validate_phone_de(phone):
    """
    Validate German phone number format
    Allows: +49 or 0 prefix, 9-15 digits
    Returns: True if valid or None (optional field), False if invalid
    """
    if phone is None or phone == "":
        return True  # Phone is optional

    # Remove common formatting characters
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

# Register UDFs for use in DataFrames
validate_email_udf = F.udf(validate_email, BooleanType())
validate_phone_udf = F.udf(validate_phone_de, BooleanType())
validate_not_null_udf = F.udf(validate_not_null, BooleanType())

print("✅ Validation functions registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Bronze Data

# COMMAND ----------

# Start timing
start_time = datetime.now()

try:
    df_bronze = spark.read.table(SOURCE_TABLE)
    records_read = df_bronze.count()

    print(f"✅ Bronze data loaded")
    print(f"   Records: {records_read:,}")
    print(f"   Columns: {len(df_bronze.columns)}")

    # Show sample
    print("\n📊 Sample data (first 3 rows):")
    df_bronze.select("contactid", "firstname", "surname", "emailaddress1", "mobilephone").show(3, truncate=False)

except Exception as e:
    print(f"❌ Error reading Bronze table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apply Data Quality Checks

# COMMAND ----------

# Apply all validations
df_validated = df_bronze.withColumn(
    "_email_valid",
    validate_email_udf(F.col("emailaddress1"))
).withColumn(
    "_phone_valid",
    validate_phone_udf(F.col("mobilephone"))
).withColumn(
    "_firstname_valid",
    validate_not_null_udf(F.col("firstname"))
).withColumn(
    "_surname_valid",
    validate_not_null_udf(F.col("surname"))
)

# Overall validity: ALL checks must pass
df_validated = df_validated.withColumn(
    "_is_valid",
    F.col("_email_valid") &
    F.col("_phone_valid") &
    F.col("_firstname_valid") &
    F.col("_surname_valid")
)

# Split into valid and invalid
df_valid = df_validated.filter(F.col("_is_valid") == True)
df_invalid = df_validated.filter(F.col("_is_valid") == False)

valid_count = df_valid.count()
invalid_count = df_invalid.count()

print(f"✅ Data Quality checks completed")
print(f"   Valid records: {valid_count:,} ({valid_count/records_read*100:.1f}%)")
print(f"   Invalid records: {invalid_count:,} ({invalid_count/records_read*100:.1f}%)")

# Show validation breakdown
if invalid_count > 0:
    print("\n⚠️  Validation failures breakdown:")
    df_validated.groupBy(
        F.col("_email_valid").alias("email_ok"),
        F.col("_phone_valid").alias("phone_ok"),
        F.col("_firstname_valid").alias("firstname_ok"),
        F.col("_surname_valid").alias("surname_ok")
    ).count().orderBy(F.desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transform Valid Records

# COMMAND ----------

# Apply transformations: trim, lowercase, etc.
df_silver = df_valid.select(
    # Business keys
    F.col("contactid"),

    # Personal info (trimmed)
    F.trim(F.col("firstname")).alias("firstname"),
    F.trim(F.col("surname")).alias("surname"),

    # Contact info (cleaned)
    F.lower(F.trim(F.col("emailaddress1"))).alias("emailaddress1"),
    F.trim(F.col("mobilephone")).alias("mobilephone"),

    # Additional fields (preserve as-is or add more transformations)
    F.col("address1_city"),
    F.col("address1_postalcode"),
    F.col("birthdate"),
    F.col("gendercode"),

    # Dataverse metadata
    F.col("createdon"),
    F.col("modifiedon"),
    F.col("statecode"),
    F.col("statuscode"),

    # Silver layer metadata
    F.lit(RUN_ID).alias("_silver_loaded_at"),
    F.current_timestamp().alias("_silver_timestamp")
)

print("✅ Transformations applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Deduplication

# COMMAND ----------

# Deduplication strategy: Keep latest record by modifiedon per contactid
window_spec = Window.partitionBy("contactid").orderBy(F.desc("modifiedon"))

df_silver_dedup = df_silver.withColumn(
    "_row_num",
    F.row_number().over(window_spec)
).filter(
    F.col("_row_num") == 1
).drop("_row_num")

records_after_dedup = df_silver_dedup.count()
duplicates_removed = valid_count - records_after_dedup

print(f"✅ Deduplication completed")
print(f"   Records before: {valid_count:,}")
print(f"   Records after: {records_after_dedup:,}")
print(f"   Duplicates removed: {duplicates_removed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Silver Layer

# COMMAND ----------

try:
    # Write to Silver (overwrite mode for MVP, later switch to merge)
    df_silver_dedup.write.mode("overwrite").saveAsTable(TARGET_TABLE)

    print(f"✅ Data written to Silver")
    print(f"   Table: {TARGET_TABLE}")
    print(f"   Records: {records_after_dedup:,}")

except Exception as e:
    print(f"❌ Error writing to Silver: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Invalid Records to Quarantine

# COMMAND ----------

if invalid_count > 0:
    try:
        # Create detailed quarantine records
        df_quarantine = df_invalid.select(
            F.expr("uuid()").alias("quarantine_id"),
            F.lit("contact").alias("source_table"),
            F.lit(RUN_ID).alias("pipeline_run_id"),

            # Determine error type (prioritize first failure)
            F.when(~F.col("_email_valid"), "email_invalid")
             .when(~F.col("_phone_valid"), "phone_invalid")
             .when(~F.col("_firstname_valid"), "firstname_null")
             .when(~F.col("_surname_valid"), "surname_null")
             .otherwise("unknown")
             .alias("error_type"),

            # Error message
            F.concat_ws("; ",
                F.when(~F.col("_email_valid"), F.lit("Invalid email format")),
                F.when(~F.col("_phone_valid"), F.lit("Invalid phone format")),
                F.when(~F.col("_firstname_valid"), F.lit("First name is null")),
                F.when(~F.col("_surname_valid"), F.lit("Surname is null"))
            ).alias("error_message"),

            # Error field
            F.when(~F.col("_email_valid"), "emailaddress1")
             .when(~F.col("_phone_valid"), "mobilephone")
             .when(~F.col("_firstname_valid"), "firstname")
             .when(~F.col("_surname_valid"), "surname")
             .otherwise("multiple")
             .alias("error_field"),

            # Raw record as JSON
            F.to_json(F.struct(
                "contactid", "firstname", "surname", "emailaddress1", "mobilephone"
            )).alias("raw_record"),

            # Status
            F.lit("new").alias("status"),
            F.lit(None).cast("string").alias("resolution_notes"),
            F.lit(None).cast("string").alias("resolved_by"),
            F.lit(None).cast("timestamp").alias("resolved_at"),

            # Timestamp
            F.current_timestamp().alias("created_at")
        )

        # Write to quarantine
        df_quarantine.write.mode("append").saveAsTable(QUARANTINE_TABLE)

        print(f"✅ Quarantine records written")
        print(f"   Records: {invalid_count:,}")

        # Show error type breakdown
        print("\n📊 Error types:")
        df_quarantine.groupBy("error_type").count().orderBy(F.desc("count")).show()

    except Exception as e:
        print(f"⚠️  Warning: Could not write to quarantine: {str(e)}")
        # Don't fail the whole pipeline if quarantine write fails

else:
    print("✅ No quarantine records (all data valid)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Pipeline Metrics

# COMMAND ----------

end_time = datetime.now()
duration_sec = (end_time - start_time).total_seconds()
error_rate = invalid_count / records_read if records_read > 0 else 0.0

try:
    metrics_data = [{
        "metric_id": str(uuid.uuid4()),
        "run_id": RUN_ID,
        "pipeline_name": PIPELINE_NAME,
        "stage": "bronze_to_silver",
        "source_table": "contact",
        "target_table": "silver.contact",
        "records_read": records_read,
        "records_written": records_after_dedup,
        "records_failed": invalid_count,
        "duration_sec": duration_sec,
        "error_rate": error_rate,
        "dq_score_avg": 1.0 - error_rate,
        "scd_inserts": None,
        "scd_updates": None,
        "scd_no_changes": None,
        "start_time": start_time,
        "end_time": end_time,
        "created_at": datetime.now()
    }]

    metrics_df = spark.createDataFrame(metrics_data)
    metrics_df.write.mode("append").saveAsTable(METRICS_TABLE)

    print(f"✅ Metrics written")

except Exception as e:
    print(f"⚠️  Warning: Could not write metrics: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary Report

# COMMAND ----------

print("\n" + "="*60)
print("📊 PIPELINE EXECUTION SUMMARY")
print("="*60)
print(f"Pipeline: {PIPELINE_NAME}")
print(f"Run ID: {RUN_ID}")
print(f"Environment: {environment}")
print(f"\n📥 INPUT")
print(f"  Bronze records: {records_read:,}")
print(f"\n✅ PROCESSING")
print(f"  Valid records: {valid_count:,} ({valid_count/records_read*100:.1f}%)")
print(f"  Invalid records: {invalid_count:,} ({invalid_count/records_read*100:.1f}%)")
print(f"  Duplicates removed: {duplicates_removed:,}")
print(f"\n📤 OUTPUT")
print(f"  Silver records: {records_after_dedup:,}")
print(f"  Quarantine records: {invalid_count:,}")
print(f"\n⏱️  PERFORMANCE")
print(f"  Duration: {duration_sec:.2f} seconds")
print(f"  Records/sec: {records_read/duration_sec:.0f}")
print(f"\n📊 QUALITY")
print(f"  Error rate: {error_rate*100:.2f}%")
print(f"  DQ score: {(1-error_rate)*100:.2f}%")
print("="*60)

# Return success
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "run_id": RUN_ID,
    "records_written": records_after_dedup,
    "records_failed": invalid_count,
    "error_rate": error_rate
}))
