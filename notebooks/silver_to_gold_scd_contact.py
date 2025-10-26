# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: Contact SCD Type 2
# MAGIC
# MAGIC **Purpose**: Create slowly changing dimension (Type 2) for contact history tracking
# MAGIC
# MAGIC **Process**:
# MAGIC 1. Read current snapshot from Silver
# MAGIC 2. Calculate hash of tracked attributes
# MAGIC 3. Compare with existing Gold records
# MAGIC 4. Use Delta MERGE to:
# MAGIC    - Close out changed records (set is_current=FALSE, update valid_to)
# MAGIC    - Insert new versions for changed records
# MAGIC    - Insert completely new records
# MAGIC 5. Track metrics (inserts, updates, no-changes)
# MAGIC
# MAGIC **SCD Type 2 Logic**:
# MAGIC - Business Key: contactid
# MAGIC - Tracked Attributes: emailaddress1, mobilephone, firstname, surname
# MAGIC - Hash-based change detection
# MAGIC - Valid_from/Valid_to temporal tracking
# MAGIC
# MAGIC **Author**: Generated from deployment plan
# MAGIC **Last Updated**: 2025-10-26

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import hashlib
import json
import uuid

# COMMAND ----------

# Configuration
dbutils.widgets.text("environment", "dev", "Environment (dev/test/prod)")
environment = dbutils.widgets.get("environment")

# Lakehouse names
SILVER_LAKEHOUSE = f"hs_silver_{environment}"
GOLD_LAKEHOUSE = f"hs_gold_{environment}"
MONITORING_LAKEHOUSE = f"hs_monitoring_{environment}"

# Table names
SOURCE_TABLE = f"{SILVER_LAKEHOUSE}.contact"
TARGET_TABLE = f"{GOLD_LAKEHOUSE}.dim_contact_scd"
METRICS_TABLE = f"{MONITORING_LAKEHOUSE}.pipeline_metrics"

# SCD Configuration
BUSINESS_KEY = "contactid"
TRACKED_COLUMNS = ["emailaddress1", "mobilephone", "firstname", "surname"]
SURROGATE_KEY = "contact_sk"

# Run metadata
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
PIPELINE_NAME = "silver_to_gold_scd_contact"

print(f"🚀 Starting {PIPELINE_NAME}")
print(f"   Environment: {environment}")
print(f"   Run ID: {RUN_ID}")
print(f"   Source: {SOURCE_TABLE}")
print(f"   Target: {TARGET_TABLE}")
print(f"   Business Key: {BUSINESS_KEY}")
print(f"   Tracked Columns: {', '.join(TRACKED_COLUMNS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helper Functions

# COMMAND ----------

def calculate_hash(df, columns):
    """
    Calculate MD5 hash of specified columns for change detection
    """
    # Concatenate all tracked columns with | separator
    concat_expr = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns])

    # Calculate MD5 hash
    return df.withColumn("hash_value", F.md5(concat_expr))


def get_scd_metrics(delta_history):
    """
    Extract SCD metrics from Delta table history
    """
    try:
        # Get the latest operation from Delta history
        latest_op = delta_history.orderBy(F.desc("version")).limit(1).collect()[0]

        operation_metrics = latest_op.operationMetrics if hasattr(latest_op, 'operationMetrics') else {}

        return {
            "inserts": int(operation_metrics.get("numTargetRowsInserted", 0)),
            "updates": int(operation_metrics.get("numTargetRowsUpdated", 0)),
            "deletes": int(operation_metrics.get("numTargetRowsDeleted", 0))
        }
    except:
        return {"inserts": 0, "updates": 0, "deletes": 0}

print("✅ Helper functions loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Silver Data (Current Snapshot)

# COMMAND ----------

start_time = datetime.now()

try:
    df_source = spark.read.table(SOURCE_TABLE)
    records_read = df_source.count()

    print(f"✅ Silver data loaded")
    print(f"   Records: {records_read:,}")

    # Show sample
    print("\n📊 Sample data:")
    df_source.select(BUSINESS_KEY, *TRACKED_COLUMNS).show(3, truncate=False)

except Exception as e:
    print(f"❌ Error reading Silver table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Source Data with Hash

# COMMAND ----------

# Select only needed columns
df_source_clean = df_source.select(
    BUSINESS_KEY,
    *TRACKED_COLUMNS,
    "createdon",
    "modifiedon",
    "_silver_loaded_at"
)

# Calculate hash for change detection
df_source_hashed = calculate_hash(df_source_clean, TRACKED_COLUMNS)

# Add SCD metadata columns
df_source_prepared = df_source_hashed.withColumn(
    "valid_from", F.current_timestamp()
).withColumn(
    "valid_to", F.to_timestamp(F.lit("9999-12-31 23:59:59"))
).withColumn(
    "is_current", F.lit(True)
)

print("✅ Source data prepared with hash values")
print(f"   Records prepared: {df_source_prepared.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Check if Target Table Exists (Initial Load vs. Incremental)

# COMMAND ----------

target_exists = spark.catalog.tableExists(TARGET_TABLE)

if not target_exists:
    print("ℹ️  Target table does not exist - performing INITIAL LOAD")
else:
    print("ℹ️  Target table exists - performing INCREMENTAL UPDATE (SCD Type 2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Initial Load (First Run Only)

# COMMAND ----------

if not target_exists:
    print("\n🔄 Starting initial load...")

    # Create initial version with auto-increment surrogate key
    # Note: We'll use Delta's IDENTITY column feature
    try:
        # Write initial data
        df_source_prepared.write.mode("overwrite").saveAsTable(TARGET_TABLE)

        # Add surrogate key column (Delta IDENTITY)
        spark.sql(f"""
            ALTER TABLE {TARGET_TABLE}
            ADD COLUMN {SURROGATE_KEY} BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
        """)

        records_inserted = df_source_prepared.count()

        print(f"✅ Initial load completed")
        print(f"   Records inserted: {records_inserted:,}")

        # Set metrics for initial load
        scd_inserts = records_inserted
        scd_updates = 0
        scd_no_changes = 0

    except Exception as e:
        print(f"❌ Error in initial load: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Incremental Update (SCD Type 2 MERGE)

# COMMAND ----------

if target_exists:
    print("\n🔄 Starting SCD Type 2 incremental update...")

    try:
        # Get Delta table reference
        delta_target = DeltaTable.forName(spark, TARGET_TABLE)

        # Get current state before MERGE
        target_before = spark.read.table(TARGET_TABLE)
        current_records_before = target_before.filter("is_current = TRUE").count()

        print(f"   Current records before merge: {current_records_before:,}")

        # ===================================================================
        # STEP 1: MERGE to close out changed records
        # ===================================================================
        print("\n   Step 1: Closing out changed records...")

        delta_target.alias("target").merge(
            df_source_prepared.alias("source"),
            f"target.{BUSINESS_KEY} = source.{BUSINESS_KEY} AND target.is_current = TRUE"
        ).whenMatchedUpdate(
            condition = "target.hash_value != source.hash_value",
            set = {
                "is_current": "FALSE",
                "valid_to": "source.valid_from"
            }
        ).execute()

        print("   ✓ Step 1 completed")

        # ===================================================================
        # STEP 2: Find records that were just closed (need new version)
        # ===================================================================
        print("\n   Step 2: Identifying changed records...")

        # Find contactids that were just updated (is_current set to FALSE in this run)
        changed_records = spark.sql(f"""
            SELECT DISTINCT {BUSINESS_KEY}
            FROM {TARGET_TABLE}
            WHERE is_current = FALSE
              AND valid_to >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
        """)

        changed_count = changed_records.count()
        print(f"   ✓ Found {changed_count:,} changed records")

        # ===================================================================
        # STEP 3: Insert new versions for changed records
        # ===================================================================
        if changed_count > 0:
            print("\n   Step 3: Inserting new versions...")

            df_new_versions = df_source_prepared.join(
                changed_records,
                on=BUSINESS_KEY,
                how="inner"
            )

            df_new_versions.write.mode("append").saveAsTable(TARGET_TABLE)
            print(f"   ✓ Inserted {changed_count:,} new versions")

        # ===================================================================
        # STEP 4: Find and insert completely new records
        # ===================================================================
        print("\n   Step 4: Inserting new records...")

        existing_keys = spark.sql(f"SELECT DISTINCT {BUSINESS_KEY} FROM {TARGET_TABLE}")

        df_new_records = df_source_prepared.join(
            existing_keys,
            on=BUSINESS_KEY,
            how="left_anti"
        )

        new_count = df_new_records.count()

        if new_count > 0:
            df_new_records.write.mode("append").saveAsTable(TARGET_TABLE)
            print(f"   ✓ Inserted {new_count:,} new records")
        else:
            print("   ✓ No new records to insert")

        # ===================================================================
        # STEP 5: Calculate metrics
        # ===================================================================
        target_after = spark.read.table(TARGET_TABLE)
        current_records_after = target_after.filter("is_current = TRUE").count()
        total_records_after = target_after.count()

        scd_inserts = new_count
        scd_updates = changed_count
        scd_no_changes = records_read - new_count - changed_count

        print(f"\n✅ SCD Type 2 merge completed")
        print(f"   New records: {scd_inserts:,}")
        print(f"   Changed records (new versions): {scd_updates:,}")
        print(f"   Unchanged records: {scd_no_changes:,}")
        print(f"   Current records: {current_records_after:,}")
        print(f"   Total history rows: {total_records_after:,}")

    except Exception as e:
        print(f"❌ Error in SCD merge: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Quality Checks on Gold Table

# COMMAND ----------

print("\n🔍 Running data quality checks on Gold table...")

# Check 1: Every business key should have exactly ONE current record
check_current = spark.sql(f"""
    SELECT {BUSINESS_KEY}, COUNT(*) as current_count
    FROM {TARGET_TABLE}
    WHERE is_current = TRUE
    GROUP BY {BUSINESS_KEY}
    HAVING COUNT(*) != 1
""")

violations = check_current.count()
if violations > 0:
    print(f"⚠️  WARNING: {violations} business keys have multiple current records!")
    check_current.show()
else:
    print("✓ All business keys have exactly 1 current record")

# Check 2: No overlapping valid_from/valid_to for same business key
check_overlap = spark.sql(f"""
    SELECT a.{BUSINESS_KEY}, a.valid_from as a_from, a.valid_to as a_to,
           b.valid_from as b_from, b.valid_to as b_to
    FROM {TARGET_TABLE} a
    JOIN {TARGET_TABLE} b
      ON a.{BUSINESS_KEY} = b.{BUSINESS_KEY}
      AND a.{SURROGATE_KEY} != b.{SURROGATE_KEY}
    WHERE a.valid_from < b.valid_to
      AND a.valid_to > b.valid_from
""")

overlaps = check_overlap.count()
if overlaps > 0:
    print(f"⚠️  WARNING: {overlaps} overlapping time periods found!")
    check_overlap.show()
else:
    print("✓ No overlapping time periods")

# Check 3: Valid_from should always be < valid_to
check_dates = spark.sql(f"""
    SELECT COUNT(*) as invalid_dates
    FROM {TARGET_TABLE}
    WHERE valid_from >= valid_to
""").collect()[0]['invalid_dates']

if check_dates > 0:
    print(f"⚠️  WARNING: {check_dates} records with invalid date ranges!")
else:
    print("✓ All date ranges are valid")

print("\n✅ Data quality checks completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Pipeline Metrics

# COMMAND ----------

end_time = datetime.now()
duration_sec = (end_time - start_time).total_seconds()

try:
    metrics_data = [{
        "metric_id": str(uuid.uuid4()),
        "run_id": RUN_ID,
        "pipeline_name": PIPELINE_NAME,
        "stage": "silver_to_gold",
        "source_table": "contact",
        "target_table": "gold.dim_contact_scd",
        "records_read": records_read,
        "records_written": scd_inserts + scd_updates,
        "records_failed": 0,
        "duration_sec": duration_sec,
        "error_rate": 0.0,
        "dq_score_avg": 1.0,
        "scd_inserts": scd_inserts,
        "scd_updates": scd_updates,
        "scd_no_changes": scd_no_changes,
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
print("📊 SCD TYPE 2 EXECUTION SUMMARY")
print("="*60)
print(f"Pipeline: {PIPELINE_NAME}")
print(f"Run ID: {RUN_ID}")
print(f"Environment: {environment}")
print(f"Load Type: {'Initial Load' if not target_exists else 'Incremental Update'}")
print(f"\n📥 INPUT")
print(f"  Silver records: {records_read:,}")
print(f"\n🔄 SCD PROCESSING")
print(f"  New records inserted: {scd_inserts:,}")
print(f"  Changed records (versions): {scd_updates:,}")
print(f"  Unchanged records: {scd_no_changes:,}")
print(f"\n📤 OUTPUT")
total_rows = spark.read.table(TARGET_TABLE).count()
current_rows = spark.read.table(TARGET_TABLE).filter("is_current = TRUE").count()
print(f"  Total history rows: {total_rows:,}")
print(f"  Current rows: {current_rows:,}")
print(f"  Historical rows: {total_rows - current_rows:,}")
print(f"\n⏱️  PERFORMANCE")
print(f"  Duration: {duration_sec:.2f} seconds")
if duration_sec > 0:
    print(f"  Records/sec: {records_read/duration_sec:.0f}")
print("="*60)

# Show sample of current and historical data
print("\n📊 Sample Current Records:")
spark.sql(f"""
    SELECT {BUSINESS_KEY}, firstname, surname, emailaddress1,
           valid_from, valid_to, is_current
    FROM {TARGET_TABLE}
    WHERE is_current = TRUE
    ORDER BY valid_from DESC
    LIMIT 5
""").show(truncate=False)

if total_rows > current_rows:
    print("\n📊 Sample Historical Records (changed records):")
    spark.sql(f"""
        SELECT {BUSINESS_KEY}, firstname, surname, emailaddress1,
               valid_from, valid_to, is_current
        FROM {TARGET_TABLE}
        WHERE is_current = FALSE
        ORDER BY valid_to DESC
        LIMIT 5
    """).show(truncate=False)

# Return success
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "run_id": RUN_ID,
    "scd_inserts": scd_inserts,
    "scd_updates": scd_updates,
    "scd_no_changes": scd_no_changes,
    "total_history_rows": total_rows
}))
