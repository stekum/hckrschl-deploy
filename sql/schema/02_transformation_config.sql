-- ================================================================
-- Transformation Configuration Schema
-- Purpose: Config-driven transformation rules for medallion architecture
-- Lakehouse: hs_silver_dev (dev), hs_silver_test (test), hs_silver_prod (prod)
-- ================================================================

-- ================================================================
-- Transformation Config Table
-- Defines how each table flows through the medallion architecture
-- ================================================================
CREATE TABLE IF NOT EXISTS transformation_config (
    -- Identity
    config_id STRING NOT NULL,

    -- Source & Target
    source_table STRING NOT NULL,      -- 'bronze.contact', 'silver.contact'
    target_table STRING NOT NULL,      -- 'silver.contact', 'gold.dim_contact_scd'
    target_layer STRING NOT NULL,      -- 'silver', 'gold'

    -- Transformation Type
    rule_type STRING NOT NULL,         -- 'dq_check', 'scd2', 'aggregate', 'simple_copy'
    rule_definition STRING NOT NULL,   -- JSON with transformation details

    -- Execution
    priority INT DEFAULT 100,          -- Lower = runs first
    active BOOLEAN DEFAULT true,
    batch_size INT DEFAULT 1000,

    -- Dependencies
    depends_on STRING,                 -- JSON array of config_ids that must run first

    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    created_by STRING,
    description STRING
)
USING DELTA
COMMENT 'Configuration for data transformations across medallion layers';

-- Add constraints and optimizations
ALTER TABLE transformation_config
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);


-- ================================================================
-- Data Quality Whitelist Table
-- Exceptions for DQ rules (false positives)
-- ================================================================
CREATE TABLE IF NOT EXISTS dq_whitelist (
    -- Identity
    whitelist_id STRING NOT NULL,

    -- Rule Info
    table_name STRING NOT NULL,
    field_name STRING,
    rule_type STRING NOT NULL,

    -- Whitelist Pattern
    pattern STRING NOT NULL,           -- Regex or exact value to whitelist
    reason STRING,

    -- Status
    active BOOLEAN DEFAULT true,
    expires_at TIMESTAMP,

    -- Metadata
    created_at TIMESTAMP NOT NULL,
    created_by STRING
)
USING DELTA
COMMENT 'Whitelist for data quality rule exceptions';


-- ================================================================
-- Sample Transformation Configs (Documentation)
-- ================================================================

/*
-- Example 1: DQ Check for Contact Email
{
  "config_id": "contact_email_dq",
  "source_table": "bronze.contact",
  "target_table": "silver.contact",
  "target_layer": "silver",
  "rule_type": "dq_check",
  "rule_definition": {
    "validations": [
      {
        "field": "emailaddress1",
        "type": "regex",
        "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
        "required": true,
        "error_action": "quarantine"
      },
      {
        "field": "mobilephone",
        "type": "regex",
        "pattern": "^(\\+49|0)[1-9]\\d{8,14}$",
        "required": false,
        "error_action": "quarantine"
      },
      {
        "field": "firstname",
        "type": "not_null",
        "error_action": "quarantine"
      },
      {
        "field": "surname",
        "type": "not_null",
        "error_action": "quarantine"
      }
    ],
    "transformations": [
      {
        "field": "emailaddress1",
        "operations": ["trim", "lowercase"]
      },
      {
        "field": "firstname",
        "operations": ["trim"]
      },
      {
        "field": "surname",
        "operations": ["trim"]
      }
    ],
    "deduplication": {
      "enabled": true,
      "key_fields": ["contactid"],
      "order_by": "modifiedon",
      "order": "desc"
    }
  },
  "priority": 10,
  "active": true
}

-- Example 2: SCD Type 2 for Contact
{
  "config_id": "contact_scd2",
  "source_table": "silver.contact",
  "target_table": "gold.dim_contact_scd",
  "target_layer": "gold",
  "rule_type": "scd2",
  "rule_definition": {
    "business_key": "contactid",
    "tracked_columns": [
      "emailaddress1",
      "mobilephone",
      "firstname",
      "surname"
    ],
    "metadata_columns": {
      "valid_from": "valid_from",
      "valid_to": "valid_to",
      "is_current": "is_current",
      "hash_value": "hash_value"
    },
    "surrogate_key": "contact_sk"
  },
  "priority": 20,
  "active": true,
  "depends_on": ["contact_email_dq"]
}

-- Example 3: DQ Check for Account
{
  "config_id": "account_dq",
  "source_table": "bronze.account",
  "target_table": "silver.account",
  "target_layer": "silver",
  "rule_type": "dq_check",
  "rule_definition": {
    "validations": [
      {
        "field": "name",
        "type": "not_null",
        "error_action": "quarantine"
      },
      {
        "field": "hs_partner_level",
        "type": "enum",
        "allowed_values": ["Bronze", "Silver", "Gold", "Platinum"],
        "required": false,
        "error_action": "quarantine"
      }
    ],
    "transformations": [
      {
        "field": "name",
        "operations": ["trim"]
      }
    ],
    "deduplication": {
      "enabled": true,
      "key_fields": ["accountid"],
      "order_by": "modifiedon",
      "order": "desc"
    }
  },
  "priority": 10,
  "active": true
}

-- Example 4: Session Date Logic Check
{
  "config_id": "session_date_dq",
  "source_table": "bronze.hs_course",
  "target_table": "silver.hs_course",
  "target_layer": "silver",
  "rule_type": "dq_check",
  "rule_definition": {
    "validations": [
      {
        "field": "hs_startdate",
        "type": "not_null",
        "error_action": "quarantine"
      },
      {
        "field": "hs_enddate",
        "type": "not_null",
        "error_action": "quarantine"
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
        "error_action": "quarantine"
      }
    ],
    "transformations": [],
    "deduplication": {
      "enabled": true,
      "key_fields": ["hs_courseid"],
      "order_by": "modifiedon",
      "order": "desc"
    }
  },
  "priority": 10,
  "active": true
}
*/
