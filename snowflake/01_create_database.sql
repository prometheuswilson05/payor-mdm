-- =============================================================================
-- 01_create_database.sql
-- Creates the MDM database.
-- Idempotent: safe to re-run.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS MDM
  COMMENT = 'Payor Master Data Management — source landing, matching, golden records, audit';
