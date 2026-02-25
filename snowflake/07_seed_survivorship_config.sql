-- =============================================================================
-- 07_seed_survivorship_config.sql
-- Seeds field-level survivorship rules into MDM.MASTER.SURVIVORSHIP_CONFIG.
-- Idempotent: MERGE on field_name — safe to re-run without creating duplicates.
-- Source trust ranking: 1=CRM (highest), 2=credentialing, 3=claims, 4=cms_reference
-- Note: PARSE_JSON() must be used in SELECT context, not VALUES clause.
-- =============================================================================

USE DATABASE MDM;
USE SCHEMA MASTER;

MERGE INTO MDM.MASTER.SURVIVORSHIP_CONFIG AS target
USING (
    -- payor_name: CRM is canonical; fall back to longest available value
    SELECT 'payor_name'     AS field_name,
           'source_priority' AS rule_type,
           PARSE_JSON('["crm","credentialing","claims","cms_reference"]') AS source_priority,
           'most_complete'  AS fallback_rule,
           'CRM holds the canonical payor name. Fall back to longest non-null value across sources.' AS description
    UNION ALL
    -- payor_name_alt: DBA names most complete in credentialing
    SELECT 'payor_name_alt', 'most_complete', NULL, 'source_priority',
           'DBA/alternate names are most completely captured in credentialing. Prefer the most complete value.'
    UNION ALL
    -- tax_id: Should not vary; conflicts logged to audit
    SELECT 'tax_id', 'source_priority',
           PARSE_JSON('["crm","credentialing","claims","cms_reference"]'),
           'any_non_null',
           'Tax IDs (EINs) should not vary across sources. Conflicts are logged to AUDIT.MDM_CHANGE_LOG.'
    UNION ALL
    -- npi
    SELECT 'npi', 'source_priority',
           PARSE_JSON('["crm","credentialing","claims","cms_reference"]'),
           'any_non_null', NULL
    UNION ALL
    -- cms_plan_id: CMS reference is authoritative
    SELECT 'cms_plan_id', 'source_priority',
           PARSE_JSON('["cms_reference","crm","credentialing","claims"]'),
           'any_non_null',
           'CMS reference is the authoritative source for CMS contract/plan IDs (H/R/S prefix).'
    UNION ALL
    -- address fields: prefer most recent (addresses change frequently)
    SELECT 'address_line_1', 'most_recent', NULL, 'source_priority',
           'Addresses change over time. Most recently loaded value wins; fall back to source priority.'
    UNION ALL
    SELECT 'address_line_2', 'most_recent', NULL, 'source_priority', NULL
    UNION ALL
    SELECT 'city',           'most_recent', NULL, 'source_priority', NULL
    UNION ALL
    SELECT 'state_code',     'most_recent', NULL, 'source_priority', NULL
    UNION ALL
    SELECT 'zip_code',       'most_recent', NULL, 'source_priority', NULL
    UNION ALL
    -- phone: also changes; prefer most recent
    SELECT 'phone', 'most_recent', NULL, 'source_priority', NULL
    UNION ALL
    -- website
    SELECT 'website', 'source_priority',
           PARSE_JSON('["crm","credentialing","claims","cms_reference"]'),
           'most_complete', NULL
    UNION ALL
    -- payor_type
    SELECT 'payor_type', 'source_priority',
           PARSE_JSON('["crm","credentialing","claims","cms_reference"]'),
           'most_frequent', NULL
    UNION ALL
    -- line_of_business
    SELECT 'line_of_business', 'source_priority',
           PARSE_JSON('["crm","credentialing","claims","cms_reference"]'),
           'any_non_null', NULL
    UNION ALL
    -- is_active: conservative — active if ANY source says active
    SELECT 'is_active', 'any_true', NULL, NULL,
           'A payor is active if any source reports it as active. All sources must agree inactive to set FALSE.'
    UNION ALL
    -- effective_date: earliest known
    SELECT 'effective_date', 'earliest', NULL, NULL,
           'Earliest known effective date across all sources — captures the longest known relationship.'
    UNION ALL
    -- termination_date: most generous (latest)
    SELECT 'termination_date', 'latest', NULL, NULL,
           'Latest known termination date — most generous interpretation avoids premature termination.'
) AS source
ON target.field_name = source.field_name
WHEN MATCHED THEN UPDATE SET
    rule_type       = source.rule_type,
    source_priority = source.source_priority,
    fallback_rule   = source.fallback_rule,
    description     = source.description,
    updated_at      = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    field_name, rule_type, source_priority, fallback_rule, description, updated_at
) VALUES (
    source.field_name,
    source.rule_type,
    source.source_priority,
    source.fallback_rule,
    source.description,
    CURRENT_TIMESTAMP()
);
