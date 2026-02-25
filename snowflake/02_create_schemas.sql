-- =============================================================================
-- 02_create_schemas.sql
-- Creates all schemas within the MDM database.
-- Idempotent: safe to re-run.
-- =============================================================================

USE DATABASE MDM;

-- Source system landing tables (one table per feed, preserves source fidelity)
CREATE SCHEMA IF NOT EXISTS MDM.RAW
  COMMENT = 'Source system landing tables — raw, unmodified records per feed';

-- Cleaned, standardized, unioned source records + pre-computed blocking keys
CREATE SCHEMA IF NOT EXISTS MDM.STAGING
  COMMENT = 'Standardized and unioned source records; blocking keys for match candidates';

-- Matching engine: candidate pairs, scores, decisions, match groups
CREATE SCHEMA IF NOT EXISTS MDM.MATCH
  COMMENT = 'Matching engine — candidate pairs, field scores, group assignments';

-- Golden records, cross-reference, hierarchy, survivorship config
CREATE SCHEMA IF NOT EXISTS MDM.MASTER
  COMMENT = 'Golden records, source cross-reference, payor hierarchy, survivorship config';

-- Full audit trail of all stewardship actions and golden record changes
CREATE SCHEMA IF NOT EXISTS MDM.AUDIT
  COMMENT = 'Audit trail — all MDM changes, stewardship actions, conflict log';
