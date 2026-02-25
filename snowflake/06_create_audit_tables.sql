-- =============================================================================
-- 06_create_audit_tables.sql
-- Creates the audit trail table in MDM.AUDIT.
-- Captures every stewardship action and golden record change.
-- Idempotent: safe to re-run.
-- =============================================================================

USE DATABASE MDM;

-- ---------------------------------------------------------------------------
-- MDM_CHANGE_LOG: Complete audit trail.
-- Written by: the matching engine (system), the survivorship engine (system),
--             and the stewardship UI (human stewards via SQL API write-back).
-- action values: 'created', 'updated', 'merged', 'split', 'override',
--                'conflict_detected', 'hierarchy_added', 'hierarchy_removed'
-- entity_type: 'match_candidate', 'golden_payor', 'hierarchy', 'survivorship_config'
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MDM.AUDIT.MDM_CHANGE_LOG (
    change_id           VARCHAR(64)     NOT NULL,
    entity_type         VARCHAR(32)     NOT NULL,    -- 'match_candidate', 'golden_payor', 'hierarchy'
    entity_id           VARCHAR(128)    NOT NULL,    -- the PK of the affected row
    action              VARCHAR(32)     NOT NULL,    -- 'created', 'updated', 'override', etc.
    field_name          VARCHAR(64),                 -- NULL for entity-level actions (e.g. 'created')
    old_value           TEXT,
    new_value           TEXT,
    changed_by          VARCHAR(128),                -- 'system' or steward username
    reason              TEXT,                        -- steward notes or system rule name
    changed_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);
