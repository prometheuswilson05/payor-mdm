-- Scoring model: deterministic rules first, then real ensemble Jaro-Winkler
-- via MDM.MATCH.ENSEMBLE_MATCH_SCORE Snowpark UDF for FUZZY pairs.
-- UDF called once per pair in the ensemble CTE; fields extracted below.
-- Returns per-field scores, winning strategy per field, and a composite.

with ensemble as (

    select
        bp.source_record_id_a,
        bp.source_record_id_b,

        -- Expose field pairs for downstream visibility
        a.payor_name_clean      as name_a,
        b.payor_name_clean      as name_b,
        a.tax_id                as tax_a,
        b.tax_id                as tax_b,
        a.phone                 as phone_a,
        b.phone                 as phone_b,
        a.cms_plan_id           as cms_a,
        b.cms_plan_id           as cms_b,
        a.state_code            as state_a,
        b.state_code            as state_b,

        -- Deterministic rule classification
        case
            when a.tax_id      is not null and a.tax_id      = b.tax_id      then 'DET-1'
            when a.cms_plan_id is not null and a.cms_plan_id = b.cms_plan_id then 'DET-2'
            when a.payor_name_clean = b.payor_name_clean
                 and a.state_code   = b.state_code                           then 'DET-3'
            else 'FUZZY'
        end as match_rule,

        -- Call ensemble UDF once per pair
        MDM.MATCH.ENSEMBLE_MATCH_SCORE(
            a.payor_name_clean,
            b.payor_name_clean,
            a.tax_id,
            b.tax_id,
            a.address_line_1 || ' ' || coalesce(a.city, '') || ' '
                || coalesce(a.state_code, '') || ' ' || coalesce(a.zip_code, ''),
            b.address_line_1 || ' ' || coalesce(b.city, '') || ' '
                || coalesce(b.state_code, '') || ' ' || coalesce(b.zip_code, ''),
            a.phone,
            b.phone,
            a.cms_plan_id,
            b.cms_plan_id
        ) as ensemble_result

    from {{ ref('int_blocking_pairs') }} bp
    join {{ ref('stg_payors_unioned') }} a on bp.source_record_id_a = a.source_record_id
    join {{ ref('stg_payors_unioned') }} b on bp.source_record_id_b = b.source_record_id

)

select
    source_record_id_a,
    source_record_id_b,
    match_rule,
    name_a,
    name_b,
    tax_a,
    tax_b,
    phone_a,
    phone_b,
    cms_a,
    cms_b,

    -- Per-field scores from ensemble result
    ensemble_result['name_score']::float        as score_name,
    ensemble_result['tax_score']::float         as score_tax_id,
    ensemble_result['addr_score']::float        as score_address,
    ensemble_result['phone_score']::float       as score_phone,
    ensemble_result['cms_score']::float         as score_cms_plan_id,

    -- Overall score: 1.0 for deterministic, UDF composite for fuzzy
    case
        when match_rule != 'FUZZY' then 1.0
        else ensemble_result['composite']::float
    end                                         as overall_score,

    -- Winning strategy metadata
    ensemble_result['name_strategy']::varchar   as winning_name_strategy,
    ensemble_result['addr_strategy']::varchar   as winning_addr_strategy,
    ensemble_result['strategies_used']          as strategies_used,
    ensemble_result                             as ensemble_detail

from ensemble
