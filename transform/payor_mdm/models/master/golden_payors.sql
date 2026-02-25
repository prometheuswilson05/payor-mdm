-- One golden record per match_group_id.
-- Field values from int_survivorship; quality metrics computed here.

with surv as (

    select * from {{ ref('int_survivorship') }}

),

group_members as (

    select
        mg.match_group_id,
        s.source_record_id,
        s.source_system

    from {{ ref('match_groups') }} mg
    join {{ ref('stg_payors_unioned') }} s on mg.source_record_id = s.source_record_id

),

source_counts as (

    select
        match_group_id,
        count(distinct source_system)  as source_count

    from group_members
    group by match_group_id

),

group_confidence as (

    select
        match_group_id,
        avg(coalesce(group_confidence, 1.0)) as confidence_score

    from {{ ref('match_groups') }}
    group by match_group_id

),

completeness as (

    select
        match_group_id,
        round(
            (
                iff(payor_name       is not null, 1, 0) +
                iff(payor_name_alt   is not null, 1, 0) +
                iff(tax_id           is not null, 1, 0) +
                iff(npi              is not null, 1, 0) +
                iff(cms_plan_id      is not null, 1, 0) +
                iff(address_line_1   is not null, 1, 0) +
                iff(city             is not null, 1, 0) +
                iff(state_code       is not null, 1, 0) +
                iff(zip_code         is not null, 1, 0) +
                iff(phone            is not null, 1, 0) +
                iff(website          is not null, 1, 0) +
                iff(payor_type       is not null, 1, 0) +
                iff(line_of_business is not null, 1, 0) +
                iff(is_active        is not null, 1, 0) +
                iff(effective_date   is not null, 1, 0)
            )::float / 15.0,
            4
        ) as completeness_score

    from surv

)

select
    s.match_group_id             as master_payor_id,
    s.payor_name,
    s.payor_name_alt,
    s.tax_id,
    s.npi,
    s.cms_plan_id,
    s.address_line_1,
    s.address_line_2,
    s.city,
    s.state_code,
    s.zip_code,
    s.phone,
    s.website,
    s.payor_type,
    s.line_of_business,
    s.is_active,
    s.effective_date,
    s.termination_date,
    c.completeness_score,
    sc.source_count,
    gc.confidence_score,
    current_timestamp()          as created_at,
    current_timestamp()          as updated_at

from surv s
left join completeness   c  on s.match_group_id = c.match_group_id
left join source_counts  sc on s.match_group_id = sc.match_group_id
left join group_confidence gc on s.match_group_id = gc.match_group_id
