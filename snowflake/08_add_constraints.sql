-- =============================================================================
-- 08_add_constraints.sql
-- Adds informational UNIQUE constraints flagged in PR #1 code review.
-- These are unenforced in Snowflake (informational only) but are used by
-- the query optimizer and surfaced as dbt schema tests.
-- Note: Snowflake does not support IF NOT EXISTS on ADD CONSTRAINT.
--       This script is a one-shot migration — safe to run on empty tables.
--       Re-running will error if constraints already exist (by design).
-- =============================================================================

-- Each source record maps to exactly one match group
ALTER TABLE MDM.MATCH.MATCH_GROUPS
    ADD CONSTRAINT uq_match_groups_source_record
    UNIQUE (source_record_id);

-- Each source record maps to exactly one golden record
ALTER TABLE MDM.MASTER.XREF
    ADD CONSTRAINT uq_xref_source_record
    UNIQUE (source_record_id);

-- A given parent/child pair should appear only once in the hierarchy
ALTER TABLE MDM.MASTER.PAYOR_HIERARCHY
    ADD CONSTRAINT uq_payor_hierarchy_pair
    UNIQUE (parent_master_id, child_master_id);
