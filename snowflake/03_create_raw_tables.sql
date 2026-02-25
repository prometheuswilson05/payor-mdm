-- =============================================================================
-- 03_create_raw_tables.sql
-- Creates source landing tables in MDM.RAW.
-- One table per feed; columns preserve source fidelity (no standardization here).
-- Idempotent: safe to re-run.
-- =============================================================================

USE DATABASE MDM;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------------
-- Source 1: Internal CRM system (most trusted — source_trust_rank = 1)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.RAW.SRC_CRM_PAYORS (
    src_payor_id        VARCHAR(64)     NOT NULL,
    payor_name          VARCHAR(256),
    payor_name_2        VARCHAR(256),       -- alternate / DBA name
    tax_id              VARCHAR(20),        -- EIN/TIN (may include dashes)
    npi                 VARCHAR(20),        -- National Provider Identifier
    cms_plan_id         VARCHAR(20),        -- CMS contract ID (H/R/S prefix)
    address_line_1      VARCHAR(256),
    address_line_2      VARCHAR(256),
    city                VARCHAR(128),
    state               VARCHAR(2),
    zip                 VARCHAR(10),
    phone               VARCHAR(20),
    website             VARCHAR(256),
    payor_type          VARCHAR(64),        -- 'commercial', 'medicare_advantage', 'medicaid', 'exchange'
    parent_payor_id     VARCHAR(64),        -- self-referencing parent (if known)
    status              VARCHAR(32),        -- 'active', 'inactive', 'terminated'
    effective_date      DATE,
    termination_date    DATE,
    loaded_at           TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    source_system       VARCHAR(32)     DEFAULT 'crm'
);

-- ---------------------------------------------------------------------------
-- Source 2: Claims system (high volume, often stale — source_trust_rank = 3)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.RAW.SRC_CLAIMS_PAYORS (
    claims_payor_code   VARCHAR(32)     NOT NULL,
    payor_name          VARCHAR(256),
    tax_id              VARCHAR(20),
    address             VARCHAR(512),       -- single-line address (needs parsing in staging)
    city                VARCHAR(128),
    state               VARCHAR(64),        -- sometimes full name, sometimes abbreviation
    zip                 VARCHAR(10),
    payor_type          VARCHAR(64),
    line_of_business    VARCHAR(64),        -- 'HMO', 'PPO', 'EPO', 'POS'
    is_active           BOOLEAN,
    loaded_at           TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    source_system       VARCHAR(32)     DEFAULT 'claims'
);

-- ---------------------------------------------------------------------------
-- Source 3: Provider network / credentialing system (source_trust_rank = 2)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.RAW.SRC_CREDENTIALING_PAYORS (
    cred_payor_id       VARCHAR(64)     NOT NULL,
    organization_name   VARCHAR(256),
    doing_business_as   VARCHAR(256),
    ein                 VARCHAR(20),
    street_address      VARCHAR(256),
    suite               VARCHAR(64),
    city                VARCHAR(128),
    state_code          VARCHAR(2),
    postal_code         VARCHAR(10),
    contact_phone       VARCHAR(20),
    contact_email       VARCHAR(256),
    plan_type           VARCHAR(64),
    network_status      VARCHAR(32),        -- 'in_network', 'out_of_network', 'pending'
    loaded_at           TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    source_system       VARCHAR(32)     DEFAULT 'credentialing'
);

-- ---------------------------------------------------------------------------
-- Source 4: External reference data — CMS public files / NPPES (source_trust_rank = 4)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.RAW.SRC_REFERENCE_PAYORS (
    ref_id              VARCHAR(64)     NOT NULL,
    official_name       VARCHAR(256),
    parent_org_name     VARCHAR(256),
    tax_id              VARCHAR(20),
    cms_contract_id     VARCHAR(20),
    plan_type           VARCHAR(64),
    state               VARCHAR(2),
    enrollment_count    INTEGER,
    star_rating         FLOAT,
    source_url          VARCHAR(512),
    loaded_at           TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    source_system       VARCHAR(32)     DEFAULT 'cms_reference'
);
