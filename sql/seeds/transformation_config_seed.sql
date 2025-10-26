-- ================================================================
-- Transformation Config Seed Data
-- Purpose: Initialize transformation rules for Hackerschool tables
-- Run this AFTER creating the transformation_config table
-- ================================================================

-- Clean slate (for development only - comment out in production)
-- DELETE FROM transformation_config;

-- ================================================================
-- CONTACT TABLE CONFIGURATIONS
-- ================================================================

-- Config 1: Contact Bronze → Silver (DQ Checks)
INSERT INTO transformation_config VALUES (
    'contact_bronze_silver_dq',                    -- config_id
    'bronze.contact',                               -- source_table
    'silver.contact',                               -- target_table
    'silver',                                       -- target_layer
    'dq_check',                                     -- rule_type
    '{
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
            },
            {
                "field": "mobilephone",
                "operations": ["trim"]
            }
        ],
        "deduplication": {
            "enabled": true,
            "key_fields": ["contactid"],
            "order_by": "modifiedon",
            "order": "desc"
        }
    }',                                             -- rule_definition
    10,                                             -- priority
    true,                                           -- active
    1000,                                           -- batch_size
    NULL,                                           -- depends_on
    CURRENT_TIMESTAMP(),                            -- created_at
    NULL,                                           -- updated_at
    'system',                                       -- created_by
    'Data quality checks for contact table: email validation, phone validation, required fields'  -- description
);

-- Config 2: Contact Silver → Gold (SCD Type 2)
INSERT INTO transformation_config VALUES (
    'contact_silver_gold_scd2',                     -- config_id
    'silver.contact',                               -- source_table
    'gold.dim_contact_scd',                         -- target_table
    'gold',                                         -- target_layer
    'scd2',                                         -- rule_type
    '{
        "business_key": "contactid",
        "tracked_columns": [
            "emailaddress1",
            "mobilephone",
            "firstname",
            "surname"
        ],
        "additional_columns": [
            "address1_city",
            "address1_postalcode",
            "birthdate",
            "gendercode",
            "createdon",
            "modifiedon"
        ],
        "metadata_columns": {
            "surrogate_key": "contact_sk",
            "valid_from": "valid_from",
            "valid_to": "valid_to",
            "is_current": "is_current",
            "hash_value": "hash_value"
        }
    }',                                             -- rule_definition
    20,                                             -- priority
    true,                                           -- active
    5000,                                           -- batch_size
    '["contact_bronze_silver_dq"]',                 -- depends_on
    CURRENT_TIMESTAMP(),                            -- created_at
    NULL,                                           -- updated_at
    'system',                                       -- created_by
    'SCD Type 2 for contact dimension - tracks changes in email, phone, and name'  -- description
);


-- ================================================================
-- ACCOUNT TABLE CONFIGURATIONS
-- ================================================================

-- Config 3: Account Bronze → Silver (DQ Checks)
INSERT INTO transformation_config VALUES (
    'account_bronze_silver_dq',                     -- config_id
    'bronze.account',                               -- source_table
    'silver.account',                               -- target_table
    'silver',                                       -- target_layer
    'dq_check',                                     -- rule_type
    '{
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
            {
                "field": "name",
                "operations": ["trim"]
            },
            {
                "field": "emailaddress1",
                "operations": ["trim", "lowercase"]
            }
        ],
        "deduplication": {
            "enabled": true,
            "key_fields": ["accountid"],
            "order_by": "modifiedon",
            "order": "desc"
        }
    }',                                             -- rule_definition
    10,                                             -- priority
    true,                                           -- active
    1000,                                           -- batch_size
    NULL,                                           -- depends_on
    CURRENT_TIMESTAMP(),                            -- created_at
    NULL,                                           -- updated_at
    'system',                                       -- created_by
    'Data quality checks for account table: name validation, partner level enum'  -- description
);

-- Config 4: Account Silver → Gold (SCD Type 2)
INSERT INTO transformation_config VALUES (
    'account_silver_gold_scd2',                     -- config_id
    'silver.account',                               -- source_table
    'gold.dim_account_scd',                         -- target_table
    'gold',                                         -- target_layer
    'scd2',                                         -- rule_type
    '{
        "business_key": "accountid",
        "tracked_columns": [
            "name",
            "hs_partner_level",
            "emailaddress1",
            "telephone1"
        ],
        "additional_columns": [
            "address1_city",
            "address1_postalcode",
            "websiteurl",
            "createdon",
            "modifiedon"
        ],
        "metadata_columns": {
            "surrogate_key": "account_sk",
            "valid_from": "valid_from",
            "valid_to": "valid_to",
            "is_current": "is_current",
            "hash_value": "hash_value"
        }
    }',                                             -- rule_definition
    20,                                             -- priority
    true,                                           -- active
    5000,                                           -- batch_size
    '["account_bronze_silver_dq"]',                 -- depends_on
    CURRENT_TIMESTAMP(),                            -- created_at
    NULL,                                           -- updated_at
    'system',                                       -- created_by
    'SCD Type 2 for account dimension - tracks changes in partner level and contact info'  -- description
);


-- ================================================================
-- HS_COURSE TABLE CONFIGURATIONS
-- ================================================================

-- Config 5: hs_course Bronze → Silver (DQ Checks)
INSERT INTO transformation_config VALUES (
    'hs_course_bronze_silver_dq',                   -- config_id
    'bronze.hs_course',                             -- source_table
    'silver.hs_course',                             -- target_table
    'silver',                                       -- target_layer
    'dq_check',                                     -- rule_type
    '{
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
                "required": false,
                "error_action": "quarantine",
                "error_message": "Invalid course status"
            }
        ],
        "transformations": [
            {
                "field": "hs_name",
                "operations": ["trim"]
            }
        ],
        "deduplication": {
            "enabled": true,
            "key_fields": ["hs_courseid"],
            "order_by": "modifiedon",
            "order": "desc"
        }
    }',                                             -- rule_definition
    10,                                             -- priority
    true,                                           -- active
    1000,                                           -- batch_size
    NULL,                                           -- depends_on
    CURRENT_TIMESTAMP(),                            -- created_at
    NULL,                                           -- updated_at
    'system',                                       -- created_by
    'Data quality checks for hs_course table: date validation, status enum'  -- description
);

-- Config 6: hs_course Silver → Gold (No SCD, simple fact table)
INSERT INTO transformation_config VALUES (
    'hs_course_silver_gold_fact',                   -- config_id
    'silver.hs_course',                             -- source_table
    'gold.fact_course',                             -- target_table
    'gold',                                         -- target_layer
    'simple_copy',                                  -- rule_type
    '{
        "business_key": "hs_courseid",
        "columns": [
            "hs_courseid",
            "hs_name",
            "hs_startdate",
            "hs_enddate",
            "hs_coursestatus",
            "hs_maxparticipants",
            "hs_currentparticipants",
            "createdon",
            "modifiedon"
        ],
        "write_mode": "overwrite"
    }',                                             -- rule_definition
    20,                                             -- priority
    true,                                           -- active
    5000,                                           -- batch_size
    '["hs_course_bronze_silver_dq"]',               -- depends_on
    CURRENT_TIMESTAMP(),                            -- created_at
    NULL,                                           -- updated_at
    'system',                                       -- created_by
    'Simple copy to gold layer - course as fact table without SCD'  -- description
);


-- ================================================================
-- VERIFY SEED DATA
-- ================================================================

-- Query to verify all configs
SELECT
    config_id,
    source_table,
    target_table,
    target_layer,
    rule_type,
    priority,
    active,
    description
FROM transformation_config
ORDER BY priority, config_id;

-- Summary by layer and type
SELECT
    target_layer,
    rule_type,
    COUNT(*) as config_count,
    SUM(CASE WHEN active = TRUE THEN 1 ELSE 0 END) as active_count
FROM transformation_config
GROUP BY target_layer, rule_type
ORDER BY target_layer, rule_type;
