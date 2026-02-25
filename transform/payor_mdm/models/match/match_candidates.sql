-- Apply thresholds to scored pairs, generate candidate_id UUIDs,
-- compute final_decision (steward overrides auto).
-- Deterministic matches auto-match at 1.0.
-- No existing steward decisions yet — final_decision = auto_decision.

with scored as (

    select * from {{ ref('int_match_scored') }}

),

thresholded as (

    select
        uuid_string()                           as candidate_id,
        source_record_id_a,
        source_record_id_b,

        score_name,
        score_tax_id,
        score_address,
        score_phone,
        score_cms_plan_id,
        overall_score,
        match_rule,

        -- Auto decision based on score + deterministic rules
        case
            when match_rule in ('DET-1','DET-2','DET-3') then 'auto_match'
            when overall_score >= 0.85                   then 'auto_match'
            when overall_score >= 0.60                   then 'review'
            else                                              'auto_no_match'
        end                                     as auto_decision,

        -- Steward fields (null on creation — populated by UI write-back)
        null::varchar                           as steward_decision,
        null::varchar                           as steward_user,
        null::timestamp_tz                      as steward_timestamp,
        null::varchar                           as steward_notes,

        current_timestamp()                     as created_at

    from scored

),

with_final as (

    select
        *,
        -- final_decision: steward overrides auto; default to auto
        case
            when steward_decision = 'confirmed_match'    then 'match'
            when steward_decision = 'confirmed_no_match' then 'no_match'
            when auto_decision    = 'auto_match'         then 'match'
            else                                              'no_match'
        end                                     as final_decision

    from thresholded

)

select * from with_final
