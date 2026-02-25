-- =============================================================================
-- 04_create_match_tables.sql
-- Creates the staging unioned table and all matching engine tables.
-- STAGING.STG_PAYORS_UNIONED is the canonical input to the match pipeline.
-- MATCH.MATCH_CANDIDATES and MATCH.MATCH_GROUPS store engine outputs.
-- Idempotent: safe to re-run.
-- =============================================================================

USE DATABASE MDM;

-- ---------------------------------------------------------------------------
-- STAGING: Unified source records (all 4 sources standardized + unioned)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.STAGING.STG_PAYORS_UNIONED (
    source_record_id    VARCHAR(128)    NOT NULL,    -- composite: '{source_system}:{source_id}'
    source_system       VARCHAR(32)     NOT NULL,
    source_id           VARCHAR(64)     NOT NULL,    -- original PK from source table

    -- Standardized identity fields
    payor_name          VARCHAR(256),
    payor_name_clean    VARCHAR(256),                -- normalized: upper, legal suffixes stripped
    payor_name_alt      VARCHAR(256),                -- DBA / alternate name

    -- Standardized identifiers
    tax_id              VARCHAR(20),                 -- digits only, no dashes
    npi                 VARCHAR(20),
    cms_plan_id         VARCHAR(20),

    -- Standardized address
    address_line_1      VARCHAR(256),
    address_line_2      VARCHAR(256),
    city                VARCHAR(128),
    state_code          VARCHAR(2),                  -- always 2-letter abbreviation
    zip_code            VARCHAR(5),                  -- 5-digit only
    zip_plus_4          VARCHAR(4),

    -- Standardized contact
    phone               VARCHAR(10),                 -- digits only
    website             VARCHAR(256),

    -- Classification (standardized enums)
    payor_type          VARCHAR(64),
    line_of_business    VARCHAR(64),
    parent_ref          VARCHAR(256),                -- raw parent reference (name or ID from source)

    -- Status
    is_active           BOOLEAN,
    effective_date      DATE,
    termination_date    DATE,

    -- Pre-computed blocking keys (used by int_blocking_pairs to reduce comparison space)
    block_name_key      VARCHAR(32),                 -- LEFT(payor_name_clean, 6) || state_code
    block_tax_id        VARCHAR(20),                 -- tax_id exact match key
    block_zip3_name4    VARCHAR(16),                 -- LEFT(zip_code, 3) || LEFT(payor_name_clean, 4)

    -- Source trust and provenance
    source_trust_rank   INTEGER         NOT NULL,    -- 1=CRM (most), 2=credentialing, 3=claims, 4=reference
    loaded_at           TIMESTAMP_TZ,
    staged_at           TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- MATCH: Candidate pairs — scored pairs identified via blocking + scoring
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.MATCH.MATCH_CANDIDATES (
    candidate_id        VARCHAR(64)     NOT NULL,    -- UUID

    -- The two records being compared (FKs to STG_PAYORS_UNIONED)
    source_record_id_a  VARCHAR(128)    NOT NULL,
    source_record_id_b  VARCHAR(128)    NOT NULL,

    -- Field-level similarity scores (0.0 – 1.0; NULL if field unavailable on either record)
    score_name          FLOAT,                       -- Jaro-Winkler on payor_name_clean
    score_tax_id        FLOAT,                       -- exact match: 1.0 or 0.0
    score_address       FLOAT,                       -- token Jaccard on address tokens
    score_phone         FLOAT,                       -- exact match on last 7 digits
    score_cms_plan_id   FLOAT,                       -- exact match: 1.0 or 0.0

    -- Weighted composite (re-normalized over available fields)
    overall_score       FLOAT           NOT NULL,

    -- Which rule generated this candidate
    match_rule          VARCHAR(64)     NOT NULL,    -- 'DET-1', 'DET-2', 'DET-3', 'FUZZY'

    -- System auto-decision based on thresholds
    auto_decision       VARCHAR(16)     NOT NULL,    -- 'auto_match', 'auto_no_match', 'review'

    -- Human stewardship (written by React UI via Snowflake SQL API)
    steward_decision    VARCHAR(16),                 -- 'confirmed_match', 'confirmed_no_match', NULL
    steward_user        VARCHAR(128),
    steward_timestamp   TIMESTAMP_TZ,
    steward_notes       TEXT,

    -- Effective decision (steward overrides auto when set)
    final_decision      VARCHAR(16)     NOT NULL,    -- 'match', 'no_match'

    created_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- MATCH: Match groups — connected components of confirmed matches
-- Each group gets one match_group_id which becomes the master_payor_id
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.MATCH.MATCH_GROUPS (
    match_group_id      VARCHAR(64)     NOT NULL,    -- UUID → becomes master_payor_id in GOLDEN_PAYORS
    source_record_id    VARCHAR(128)    NOT NULL,    -- FK to STG_PAYORS_UNIONED
    is_survivor         BOOLEAN         DEFAULT FALSE,   -- TRUE for the record chosen as golden base
    group_confidence    FLOAT,                       -- average match score within the group
    created_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);
