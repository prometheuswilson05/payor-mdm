-- ===========================================================================
-- Snowpark Python UDF: MDM.MATCH.FUZZY_SCORE
-- ---------------------------------------------------------------------------
-- Computes field-level similarity scores + weighted composite for two
-- payor records. Used by the matching engine to score candidate pairs.
--
-- Weights: name=0.35, tax_id=0.25, address=0.20, phone=0.10, cms=0.10
-- Re-normalizes over non-null available fields.
-- ===========================================================================

USE DATABASE MDM;
USE SCHEMA MATCH;

CREATE OR REPLACE FUNCTION MDM.MATCH.FUZZY_SCORE(
    name_a VARCHAR, name_b VARCHAR,
    tax_a VARCHAR, tax_b VARCHAR,
    addr_a VARCHAR, addr_b VARCHAR,
    phone_a VARCHAR, phone_b VARCHAR,
    cms_a VARCHAR, cms_b VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('jellyfish')
HANDLER = 'fuzzy_score'
AS
$$
import jellyfish
import json

def fuzzy_score(name_a, name_b, tax_a, tax_b, addr_a, addr_b, phone_a, phone_b, cms_a, cms_b):
    """Compute field-level similarity scores and weighted composite."""
    scores = {}
    weights = {}

    # Name: Jaro-Winkler similarity (weight 0.35)
    if name_a and name_b:
        scores['name'] = jellyfish.jaro_winkler_similarity(
            name_a.upper().strip(), name_b.upper().strip()
        )
        weights['name'] = 0.35

    # Tax ID: exact match (weight 0.25)
    if tax_a and tax_b:
        scores['tax_id'] = 1.0 if tax_a.strip() == tax_b.strip() else 0.0
        weights['tax_id'] = 0.25

    # Address: Jaccard token overlap (weight 0.20)
    if addr_a and addr_b:
        # Tokenize, lowercase, remove common stopwords
        stopwords = {'st', 'street', 'ave', 'avenue', 'blvd', 'boulevard',
                     'rd', 'road', 'dr', 'drive', 'ln', 'lane', 'ct', 'court',
                     'ste', 'suite', 'apt', 'unit', 'fl', 'floor', '#'}
        tokens_a = set(t.lower().rstrip('.,') for t in addr_a.split()) - stopwords
        tokens_b = set(t.lower().rstrip('.,') for t in addr_b.split()) - stopwords
        intersection = tokens_a & tokens_b
        union = tokens_a | tokens_b
        scores['address'] = len(intersection) / len(union) if union else 0.0
        weights['address'] = 0.20

    # Phone: exact match on last 7 digits (weight 0.10)
    if phone_a and phone_b:
        pa = ''.join(c for c in phone_a if c.isdigit())
        pb = ''.join(c for c in phone_b if c.isdigit())
        scores['phone'] = 1.0 if len(pa) >= 7 and len(pb) >= 7 and pa[-7:] == pb[-7:] else 0.0
        weights['phone'] = 0.10

    # CMS Plan ID: exact match (weight 0.10)
    if cms_a and cms_b:
        scores['cms_plan_id'] = 1.0 if cms_a.strip().upper() == cms_b.strip().upper() else 0.0
        weights['cms_plan_id'] = 0.10

    # Weighted composite (re-normalized over available fields)
    total_weight = sum(weights.values())
    if total_weight > 0:
        composite = sum(scores[k] * weights[k] for k in scores) / total_weight
    else:
        composite = 0.0

    result = dict(scores)
    result['composite'] = round(composite, 4)
    return result
$$;
