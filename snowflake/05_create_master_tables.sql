-- =============================================================================
-- 05_create_master_tables.sql
-- Creates all tables in MDM.MASTER:
--   GOLDEN_PAYORS       — one golden record per real-world payor entity
--   XREF                — source record → golden record cross-reference
--   PAYOR_HIERARCHY     — parent/child relationships between golden records
--   SURVIVORSHIP_CONFIG — field-level survivorship rules (driven by data, not code)
-- Idempotent: safe to re-run.
-- =============================================================================

USE DATABASE MDM;

-- ---------------------------------------------------------------------------
-- GOLDEN_PAYORS: One row per real-world payor entity.
-- master_payor_id = match_group_id from MATCH.MATCH_GROUPS.
-- field_sources (VARIANT) records which source record won each field.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.MASTER.GOLDEN_PAYORS (
    master_payor_id     VARCHAR(64)     NOT NULL,    -- = match_group_id

    -- Survived field values (best value per survivorship rules)
    payor_name          VARCHAR(256)    NOT NULL,
    payor_name_alt      VARCHAR(256),
    tax_id              VARCHAR(20),
    npi                 VARCHAR(20),
    cms_plan_id         VARCHAR(20),

    -- Survived address
    address_line_1      VARCHAR(256),
    address_line_2      VARCHAR(256),
    city                VARCHAR(128),
    state_code          VARCHAR(2),
    zip_code            VARCHAR(5),

    -- Survived contact
    phone               VARCHAR(10),
    website             VARCHAR(256),

    -- Classification
    payor_type          VARCHAR(64),
    line_of_business    VARCHAR(64),

    -- Status
    is_active           BOOLEAN,
    effective_date      DATE,
    termination_date    DATE,

    -- Provenance: which source record won each field
    -- JSON shape: {"payor_name": "crm:CRM-001", "tax_id": "claims:CLM-042", ...}
    field_sources       VARIANT,

    -- Quality metrics
    completeness_score  FLOAT,          -- % of key fields that are non-null (0.0 – 1.0)
    source_count        INTEGER,        -- number of contributing source records
    confidence_score    FLOAT,          -- average match confidence across group members

    -- Timestamps
    created_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- XREF: Cross-reference — every source record mapped to its golden record.
-- Enables reverse lookup: "which golden record owns this claims code?"
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.MASTER.XREF (
    source_record_id    VARCHAR(128)    NOT NULL,    -- FK to STAGING.STG_PAYORS_UNIONED
    master_payor_id     VARCHAR(64)     NOT NULL,    -- FK to GOLDEN_PAYORS
    source_system       VARCHAR(32)     NOT NULL,
    source_id           VARCHAR(64)     NOT NULL,
    match_confidence    FLOAT,                       -- confidence of the assignment
    created_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- PAYOR_HIERARCHY: Parent → child relationships between golden records.
-- Always requires steward_confirmed = TRUE before being treated as final.
-- relationship_type: 'parent_company', 'plan', 'subsidiary', 'dba'
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.MASTER.PAYOR_HIERARCHY (
    hierarchy_id        VARCHAR(64)     NOT NULL,    -- UUID
    parent_master_id    VARCHAR(64)     NOT NULL,    -- FK to GOLDEN_PAYORS
    child_master_id     VARCHAR(64)     NOT NULL,    -- FK to GOLDEN_PAYORS
    relationship_type   VARCHAR(32)     NOT NULL,    -- 'parent_company', 'plan', 'subsidiary', 'dba'
    effective_date      DATE,
    end_date            DATE,
    source              VARCHAR(32),                 -- 'inferred', 'steward', 'cms_reference', etc.
    steward_confirmed   BOOLEAN         DEFAULT FALSE,   -- hierarchy is tentative until confirmed
    created_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- SURVIVORSHIP_CONFIG: Field-level survivorship rules, stored as data.
-- Seeded by 07_seed_survivorship_config.sql.
-- Stewards can adjust rules via the UI without code changes.
-- rule_type values: 'source_priority', 'most_recent', 'most_complete',
--                   'longest_value', 'earliest', 'latest', 'any_true',
--                   'most_frequent', 'any_non_null', 'manual'
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.MASTER.SURVIVORSHIP_CONFIG (
    field_name          VARCHAR(64)     NOT NULL,
    rule_type           VARCHAR(32)     NOT NULL,
    -- Ordered JSON array of source names, e.g. ["crm","credentialing","claims","cms_reference"]
    -- NULL for non-source-priority rules
    source_priority     VARIANT,
    fallback_rule       VARCHAR(32),                 -- applied if primary rule yields NULL
    description         TEXT,
    updated_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (field_name)
);
