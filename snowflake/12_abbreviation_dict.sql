-- ===========================================================================
-- Strategy Performance Table + Abbreviation Dictionary
-- ===========================================================================

USE DATABASE MDM;
USE SCHEMA MATCH;

-- Strategy performance tracking
CREATE TABLE IF NOT EXISTS MDM.MATCH.STRATEGY_PERFORMANCE (
    run_id              VARCHAR(64)     NOT NULL,
    run_timestamp       TIMESTAMP_TZ    NOT NULL,
    field_name          VARCHAR(32)     NOT NULL,
    strategy_id         VARCHAR(32)     NOT NULL,
    times_selected      INTEGER         NOT NULL,
    times_won           INTEGER         NOT NULL,
    avg_score_when_won  FLOAT,
    true_positives      INTEGER,
    false_positives     INTEGER,
    true_negatives      INTEGER,
    false_negatives     INTEGER,
    precision_score     FLOAT,
    recall_score        FLOAT,
    routing_reasons     VARIANT,
    inserted_at         TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

-- Seed with all strategies at zero
INSERT INTO MDM.MATCH.STRATEGY_PERFORMANCE
    (run_id, run_timestamp, field_name, strategy_id, times_selected, times_won)
SELECT '00000000-0000-0000-0000-000000000000', CURRENT_TIMESTAMP(), f.field_name, f.strategy_id, 0, 0
FROM (
    SELECT 'name' AS field_name, column1 AS strategy_id
    FROM VALUES ('NAME_JW'), ('NAME_TSR'), ('NAME_PHO'), ('NAME_ABR'), ('NAME_NGR')
    UNION ALL
    SELECT 'address', column1
    FROM VALUES ('ADDR_JAC'), ('ADDR_CMP'), ('ADDR_ZIP'), ('ADDR_NRM')
    UNION ALL
    SELECT 'phone', column1
    FROM VALUES ('PHONE_L7'), ('PHONE_E164'), ('PHONE_AC')
    UNION ALL
    SELECT 'tax_id', column1
    FROM VALUES ('TIN_EXACT'), ('TIN_TRANS'), ('TIN_PFX')
    UNION ALL
    SELECT 'cms_plan_id', column1
    FROM VALUES ('CMS_EXACT'), ('CMS_PFX')
) f;

-- Abbreviation lookup table (for reference/UI — UDF has its own copy)
CREATE TABLE IF NOT EXISTS MDM.MATCH.ABBREVIATION_DICT (
    abbreviation    VARCHAR(16)     NOT NULL,
    expansion       VARCHAR(256)    NOT NULL,
    domain          VARCHAR(32)     DEFAULT 'payor',
    inserted_at     TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (abbreviation)
);

INSERT INTO MDM.MATCH.ABBREVIATION_DICT (abbreviation, expansion) VALUES
    ('BCBS', 'Blue Cross Blue Shield'),
    ('BC', 'Blue Cross'),
    ('BS', 'Blue Shield'),
    ('UHC', 'UnitedHealthcare'),
    ('UHG', 'UnitedHealth Group'),
    ('HCSC', 'Health Care Service Corporation'),
    ('KP', 'Kaiser Permanente'),
    ('KFHP', 'Kaiser Foundation Health Plan'),
    ('HMO', 'Health Maintenance Organization'),
    ('PPO', 'Preferred Provider Organization'),
    ('EPO', 'Exclusive Provider Organization'),
    ('POS', 'Point of Service'),
    ('MVP', 'MVP Health Care'),
    ('HAP', 'Health Alliance Plan'),
    ('PHP', 'Physicians Health Plan'),
    ('QHP', 'Qualified Health Plan'),
    ('FEHB', 'Federal Employees Health Benefits'),
    ('CCHP', 'Chinese Community Health Plan');

-- Add ensemble_detail column to MATCH_CANDIDATES if not present
ALTER TABLE MDM.MATCH.MATCH_CANDIDATES ADD COLUMN IF NOT EXISTS
    ensemble_detail VARIANT;
