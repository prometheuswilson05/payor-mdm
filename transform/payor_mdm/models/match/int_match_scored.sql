-- Apply deterministic rules first; then SQL-based fuzzy scoring for pairs
-- that don't resolve deterministically. Jaro-Winkler is approximated using
-- EDITDISTANCE (Levenshtein) — the Snowpark UDF will replace this later.

with pairs as (

    select * from {{ ref('int_blocking_pairs') }}

),

stg as (

    select * from {{ ref('stg_payors_unioned') }}

),

joined as (

    select
        p.source_record_id_a,
        p.source_record_id_b,

        -- Record A fields
        a.payor_name_clean      as name_a,
        a.tax_id                as tax_a,
        a.cms_plan_id           as cms_a,
        a.state_code            as state_a,
        a.phone                 as phone_a,
        trim(
            coalesce(a.address_line_1, '') || ' ' ||
            coalesce(a.city, '') || ' ' ||
            coalesce(a.state_code, '') || ' ' ||
            coalesce(a.zip_code, '')
        )                       as addr_a,

        -- Record B fields
        b.payor_name_clean      as name_b,
        b.tax_id                as tax_b,
        b.cms_plan_id           as cms_b,
        b.state_code            as state_b,
        b.phone                 as phone_b,
        trim(
            coalesce(b.address_line_1, '') || ' ' ||
            coalesce(b.city, '') || ' ' ||
            coalesce(b.state_code, '') || ' ' ||
            coalesce(b.zip_code, '')
        )                       as addr_b

    from pairs p
    join stg a on p.source_record_id_a = a.source_record_id
    join stg b on p.source_record_id_b = b.source_record_id

),

det_rules as (

    select
        *,
        case
            when tax_a is not null and tax_a != '' and tax_a = tax_b               then 'DET-1'
            when cms_a is not null and cms_a != '' and cms_a = cms_b               then 'DET-2'
            when name_a is not null and name_a = name_b
                 and state_a is not null and state_a = state_b                     then 'DET-3'
            else 'FUZZY'
        end as match_rule

    from joined

),

scored as (

    select
        source_record_id_a,
        source_record_id_b,
        match_rule,

        -- Individual field scores (0.0–1.0)
        case
            when tax_a is not null and tax_a != '' and tax_b is not null and tax_b != ''
            then iff(tax_a = tax_b, 1.0, 0.0)
            else null
        end                                                          as score_tax_id,

        case
            when cms_a is not null and cms_a != '' and cms_b is not null and cms_b != ''
            then iff(cms_a = cms_b, 1.0, 0.0)
            else null
        end                                                          as score_cms_plan_id,

        case
            when phone_a is not null and phone_a != '' and phone_b is not null and phone_b != ''
            then iff(right(phone_a, 7) = right(phone_b, 7), 1.0, 0.0)
            else null
        end                                                          as score_phone,

        -- Name: editdistance-based approximation of Jaro-Winkler
        case
            when name_a is not null and name_a != '' and name_b is not null and name_b != ''
            then round(
                1.0 - (editdistance(name_a, name_b)::float
                       / greatest(length(name_a), length(name_b), 1)),
                4
            )
            else null
        end                                                          as score_name,

        -- Address: token overlap approximation (Jaccard via edit distance on concat)
        case
            when length(addr_a) > 3 and length(addr_b) > 3
            then round(
                1.0 - (editdistance(addr_a, addr_b)::float
                       / greatest(length(addr_a), length(addr_b), 1)),
                4
            )
            else null
        end                                                          as score_address,

        -- Composite overall_score
        case
            when match_rule != 'FUZZY' then 1.0
            else (
                -- Weighted composite over available fields
                -- weights: name=0.35, tax=0.25, addr=0.20, phone=0.10, cms=0.10
                -- re-normalized over non-null fields
                (
                    coalesce(
                        case when name_a is not null and name_a != '' and name_b is not null and name_b != ''
                             then round(1.0 - editdistance(name_a,name_b)::float/greatest(length(name_a),length(name_b),1),4)*0.35
                        end, 0)
                    + coalesce(
                        case when tax_a is not null and tax_a != '' and tax_b is not null and tax_b != ''
                             then iff(tax_a=tax_b,1.0,0.0)*0.25
                        end, 0)
                    + coalesce(
                        case when length(addr_a)>3 and length(addr_b)>3
                             then round(1.0-editdistance(addr_a,addr_b)::float/greatest(length(addr_a),length(addr_b),1),4)*0.20
                        end, 0)
                    + coalesce(
                        case when phone_a is not null and phone_a != '' and phone_b is not null and phone_b != ''
                             then iff(right(phone_a,7)=right(phone_b,7),1.0,0.0)*0.10
                        end, 0)
                    + coalesce(
                        case when cms_a is not null and cms_a != '' and cms_b is not null and cms_b != ''
                             then iff(cms_a=cms_b,1.0,0.0)*0.10
                        end, 0)
                ) / greatest(
                    coalesce(iff(name_a is not null and name_a != '' and name_b is not null and name_b != '', 0.35, null), 0)
                    + coalesce(iff(tax_a is not null and tax_a != '' and tax_b is not null and tax_b != '', 0.25, null), 0)
                    + coalesce(iff(length(addr_a)>3 and length(addr_b)>3, 0.20, null), 0)
                    + coalesce(iff(phone_a is not null and phone_a != '' and phone_b is not null and phone_b != '', 0.10, null), 0)
                    + coalesce(iff(cms_a is not null and cms_a != '' and cms_b is not null and cms_b != '', 0.10, null), 0)
                    , 0.01)  -- avoid divide-by-zero
            )
        end                                                          as overall_score

    from det_rules

)

select * from scored
