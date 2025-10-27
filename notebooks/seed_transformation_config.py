# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Transformation Configuration Data
# MAGIC
# MAGIC **Purpose**: Load initial transformation configurations for contact, account, and course tables
# MAGIC
# MAGIC **IMPORTANT**: Run 'setup_transformation_config' notebook first!
# MAGIC **Lakehouse**: hs_silver_dev (dev), hs_silver_test (test), hs_silver_prod (prod)
# MAGIC
# MAGIC **Author**: Generated for Fabric compatibility
# MAGIC **Last Updated**: 2025-10-27

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

environment = dbutils.widgets.get("environment") if "environment" in [w.name for w in dbutils.widgets.getAll()] else "dev"
print(f"Loading seed data for environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contact Table Configurations

# COMMAND ----------

# Config 1: Contact Bronze → Silver (DQ Checks)
contact_dq_config = {
    "config_id": "contact_bronze_silver_dq",
    "source_table": "bronze.contact",
    "target_table": "silver.contact",
    "target_layer": "silver",
    "rule_type": "dq_check",
    "rule_definition": """{
        "validations": [
            {
                "field": "emailaddress1",
                "type": "regex",
                "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$",
                "required": true,
                "error_action": "quarantine",
                "error_message": "Invalid email format"
            },
            {
                "field": "mobilephone",
                "type": "regex",
                "pattern": "^(\\\\+49|0)[1-9]\\\\d{8,14}$",
                "required": false,
                "error_action": "quarantine",
                "error_message": "Invalid German phone format"
            },
            {
                "field": "firstname",
                "type": "not_null",
                "required": true,
                "error_action": "quarantine",
                "error_message": "First name is required"
            },
            {
                "field": "surname",
                "type": "not_null",
                "required": true,
                "error_action": "quarantine",
                "error_message": "Surname is required"
            }
        ],
        "transformations": [
            {"field": "emailaddress1", "operations": ["trim", "lowercase"]},
            {"field": "firstname", "operations": ["trim"]},
            {"field": "surname", "operations": ["trim"]},
            {"field": "mobilephone", "operations": ["trim"]}
        ],
        "deduplication": {
            "enabled": true,
            "key_fields": ["contactid"],
            "order_by": "modifiedon",
            "order": "desc"
        }
    }""",
    "priority": 10,
    "active": True,
    "batch_size": 1000,
    "depends_on": None,
    "created_at": datetime.now(),
    "updated_at": None,
    "created_by": "system",
    "description": "Data quality checks for contact table: email validation, phone validation, required fields"
}

# Config 2: Contact Silver → Gold (SCD Type 2)
contact_scd_config = {
    "config_id": "contact_silver_gold_scd2",
    "source_table": "silver.contact",
    "target_table": "gold.dim_contact_scd",
    "target_layer": "gold",
    "rule_type": "scd2",
    "rule_definition": """{
        "business_key": "contactid",
        "tracked_columns": ["emailaddress1", "mobilephone", "firstname", "surname"],
        "additional_columns": ["address1_city", "address1_postalcode", "birthdate", "gendercode", "createdon", "modifiedon"],
        "metadata_columns": {
            "surrogate_key": "contact_sk",
            "valid_from": "valid_from",
            "valid_to": "valid_to",
            "is_current": "is_current",
            "hash_value": "hash_value"
        }
    }""",
    "priority": 20,
    "active": True,
    "batch_size": 5000,
    "depends_on": '["contact_bronze_silver_dq"]',
    "created_at": datetime.now(),
    "updated_at": None,
    "created_by": "system",
    "description": "SCD Type 2 for contact dimension - tracks changes in email, phone, and name"
}

# Insert contact configs
configs_df = spark.createDataFrame([contact_dq_config, contact_scd_config])
configs_df.write.mode("append").saveAsTable("transformation_config")

print("✅ Contact configurations loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Account Table Configurations

# COMMAND ----------

# Config 3: Account Bronze → Silver (DQ Checks)
account_dq_config = {
    "config_id": "account_bronze_silver_dq",
    "source_table": "bronze.account",
    "target_table": "silver.account",
    "target_layer": "silver",
    "rule_type": "dq_check",
    "rule_definition": """{
        "validations": [
            {
                "field": "name",
                "type": "not_null",
                "required": true,
                "error_action": "quarantine",
                "error_message": "Account name is required"
            },
            {
                "field": "hs_partner_level",
                "type": "enum",
                "allowed_values": ["Bronze", "Silver", "Gold", "Platinum"],
                "required": false,
                "error_action": "quarantine",
                "error_message": "Invalid partner level"
            },
            {
                "field": "emailaddress1",
                "type": "regex",
                "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$",
                "required": false,
                "error_action": "quarantine",
                "error_message": "Invalid email format"
            }
        ],
        "transformations": [
            {"field": "name", "operations": ["trim"]},
            {"field": "emailaddress1", "operations": ["trim", "lowercase"]}
        ],
        "deduplication": {
            "enabled": true,
            "key_fields": ["accountid"],
            "order_by": "modifiedon",
            "order": "desc"
        }
    }""",
    "priority": 10,
    "active": True,
    "batch_size": 1000,
    "depends_on": None,
    "created_at": datetime.now(),
    "updated_at": None,
    "created_by": "system",
    "description": "Data quality checks for account table: name validation, partner level enum"
}

# Config 4: Account Silver → Gold (SCD Type 2)
account_scd_config = {
    "config_id": "account_silver_gold_scd2",
    "source_table": "silver.account",
    "target_table": "gold.dim_account_scd",
    "target_layer": "gold",
    "rule_type": "scd2",
    "rule_definition": """{
        "business_key": "accountid",
        "tracked_columns": ["name", "hs_partner_level", "emailaddress1", "telephone1"],
        "additional_columns": ["address1_city", "address1_postalcode", "websiteurl", "createdon", "modifiedon"],
        "metadata_columns": {
            "surrogate_key": "account_sk",
            "valid_from": "valid_from",
            "valid_to": "valid_to",
            "is_current": "is_current",
            "hash_value": "hash_value"
        }
    }""",
    "priority": 20,
    "active": True,
    "batch_size": 5000,
    "depends_on": '["account_bronze_silver_dq"]',
    "created_at": datetime.now(),
    "updated_at": None,
    "created_by": "system",
    "description": "SCD Type 2 for account dimension - tracks changes in partner level and contact info"
}

# Insert account configs
configs_df = spark.createDataFrame([account_dq_config, account_scd_config])
configs_df.write.mode("append").saveAsTable("transformation_config")

print("✅ Account configurations loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Course Table Configurations

# COMMAND ----------

# Config 5: hs_course Bronze → Silver (DQ Checks)
course_dq_config = {
    "config_id": "hs_course_bronze_silver_dq",
    "source_table": "bronze.hs_course",
    "target_table": "silver.hs_course",
    "target_layer": "silver",
    "rule_type": "dq_check",
    "rule_definition": """{
        "validations": [
            {
                "field": "hs_name",
                "type": "not_null",
                "required": true,
                "error_action": "quarantine",
                "error_message": "Course name is required"
            },
            {
                "field": "hs_startdate",
                "type": "not_null",
                "required": true,
                "error_action": "quarantine",
                "error_message": "Start date is required"
            },
            {
                "field": "hs_enddate",
                "type": "not_null",
                "required": true,
                "error_action": "quarantine",
                "error_message": "End date is required"
            },
            {
                "type": "custom",
                "name": "date_logic_check",
                "expression": "hs_startdate < hs_enddate",
                "error_message": "Start date must be before end date",
                "error_action": "quarantine"
            },
            {
                "field": "hs_coursestatus",
                "type": "enum",
                "allowed_values": ["Planned", "Active", "Completed", "Cancelled"],
                "error_action": "quarantine",
                "error_message": "Invalid course status"
            }
        ],
        "transformations": [
            {"field": "hs_name", "operations": ["trim"]}
        ],
        "deduplication": {
            "enabled": true,
            "key_fields": ["hs_courseid"],
            "order_by": "modifiedon",
            "order": "desc"
        }
    }""",
    "priority": 10,
    "active": True,
    "batch_size": 1000,
    "depends_on": None,
    "created_at": datetime.now(),
    "updated_at": None,
    "created_by": "system",
    "description": "Data quality checks for hs_course table: date validation, status enum"
}

# Config 6: hs_course Silver → Gold (Simple Copy)
course_fact_config = {
    "config_id": "hs_course_silver_gold_fact",
    "source_table": "silver.hs_course",
    "target_table": "gold.fact_course",
    "target_layer": "gold",
    "rule_type": "simple_copy",
    "rule_definition": """{
        "business_key": "hs_courseid",
        "columns": [
            "hs_courseid", "hs_name", "hs_startdate", "hs_enddate",
            "hs_coursestatus", "hs_maxparticipants", "hs_currentparticipants",
            "createdon", "modifiedon"
        ],
        "write_mode": "overwrite"
    }""",
    "priority": 20,
    "active": True,
    "batch_size": 5000,
    "depends_on": '["hs_course_bronze_silver_dq"]',
    "created_at": datetime.now(),
    "updated_at": None,
    "created_by": "system",
    "description": "Simple copy to gold layer - course as fact table without SCD"
}

# Insert course configs
configs_df = spark.createDataFrame([course_dq_config, course_fact_config])
configs_df.write.mode("append").saveAsTable("transformation_config")

print("✅ Course configurations loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to verify all configs
# MAGIC SELECT
# MAGIC     config_id,
# MAGIC     source_table,
# MAGIC     target_table,
# MAGIC     target_layer,
# MAGIC     rule_type,
# MAGIC     priority,
# MAGIC     active,
# MAGIC     description
# MAGIC FROM transformation_config
# MAGIC ORDER BY priority, config_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary by layer and type
# MAGIC SELECT
# MAGIC     target_layer,
# MAGIC     rule_type,
# MAGIC     COUNT(*) as config_count,
# MAGIC     SUM(CASE WHEN active = TRUE THEN 1 ELSE 0 END) as active_count
# MAGIC FROM transformation_config
# MAGIC GROUP BY target_layer, rule_type
# MAGIC ORDER BY target_layer, rule_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_configs = spark.sql("SELECT COUNT(*) as cnt FROM transformation_config").collect()[0]['cnt']

print("\n" + "="*60)
print("✅ TRANSFORMATION CONFIG SEED DATA LOADED")
print("="*60)
print(f"\nTotal Configurations: {total_configs}")
print("\nConfigurations Loaded:")
print("  Contact:")
print("    - Bronze → Silver (DQ)")
print("    - Silver → Gold (SCD Type 2)")
print("  Account:")
print("    - Bronze → Silver (DQ)")
print("    - Silver → Gold (SCD Type 2)")
print("  Course:")
print("    - Bronze → Silver (DQ)")
print("    - Silver → Gold (Fact Table)")
print("="*60)

dbutils.notebook.exit("success")
