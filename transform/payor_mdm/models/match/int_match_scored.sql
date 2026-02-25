-- Scoring model: deterministic rules first, then real Jaro-Winkler via
-- MDM.MATCH.FUZZY_SCORE Snowpark UDF for FUZZY pairs.
-- UDF called once per pair in the scored CTE; fields extracted below.

with bp as (

    select * from {{ ref('int_blocking_pairs') }}

),

a_records as (

    select * from {{ ref('stg_payors_unioned') }}

),

b_records as (

    select * from {{ ref('stg_payors_unioned') }}

),

scored as (

    select
        bp.source_record_id_a,
        bp.source_record_id_b,

        -- Deterministic rule classification
        case
            when a.tax_id     is not null and a.tax_id     = b.tax_id     then 'DET-1'
            when a.cms_plan_id is not null and a.cms_plan_id = b.cms_plan_id then 'DET-2'
            when a.payor_name_clean = b.payor_name_clean
                 and a.state_code  = b.state_code                          then 'DET-3'
            else 'FUZZY'
        end as match_rule,

        -- Call UDF once; result is OBJECT with per-field scores + composite
        MDM.MATCH.FUZZY_SCORE(
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
        ) as fuzzy_result

    from bp
    join a_records a on bp.source_record_id_a = a.source_record_id
    join b_records b on bp.source_record_id_b = b.source_record_id

)

select
    source_record_id_a,
    source_record_id_b,
    match_rule,

    -- Per-field scores from UDF result object
    fuzzy_result['name']::float         as score_name,
    fuzzy_result['tax_id']::float       as score_tax_id,
    fuzzy_result['address']::float      as score_address,
    fuzzy_result['phone']::float        as score_phone,
    fuzzy_result['cms_plan_id']::float  as score_cms_plan_id,

    -- Overall score: 1.0 for deterministic matches, UDF composite for fuzzy
    case
        when match_rule != 'FUZZY' then 1.0
        else fuzzy_result['composite']::float
    end                                 as overall_score

from scored
