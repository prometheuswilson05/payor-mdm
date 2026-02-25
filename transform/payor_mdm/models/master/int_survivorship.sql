-- Apply field-level survivorship rules per match group.
-- Rules from SURVIVORSHIP_CONFIG (applied inline — config table read at golden_payors):
--   source_priority fields: FIRST_VALUE IGNORE NULLS ordered by source_trust_rank ASC
--   most_recent fields (address, phone): FIRST_VALUE IGNORE NULLS ordered by loaded_at DESC
--   most_complete (payor_name_alt): MAX (longest string)
--   is_active: TRUE if any source says active (BOOL_OR)
--   effective_date: MIN, termination_date: MAX

with group_members as (

    select
        mg.match_group_id,
        s.*

    from {{ ref('match_groups') }} mg
    join {{ ref('stg_payors_unioned') }} s
        on mg.source_record_id = s.source_record_id

),

-- One row per group: source-priority survivorship (lowest trust_rank wins)
source_priority_raw as (

    select
        match_group_id,
        first_value(payor_name ignore nulls) over (
            partition by match_group_id order by source_trust_rank asc
            rows between unbounded preceding and unbounded following
        ) as payor_name,
        first_value(tax_id ignore nulls) over (
            partition by match_group_id order by source_trust_rank asc
            rows between unbounded preceding and unbounded following
        ) as tax_id,
        first_value(npi ignore nulls) over (
            partition by match_group_id order by source_trust_rank asc
            rows between unbounded preceding and unbounded following
        ) as npi,
        first_value(website ignore nulls) over (
            partition by match_group_id order by source_trust_rank asc
            rows between unbounded preceding and unbounded following
        ) as website,
        first_value(payor_type ignore nulls) over (
            partition by match_group_id order by source_trust_rank asc
            rows between unbounded preceding and unbounded following
        ) as payor_type,
        first_value(line_of_business ignore nulls) over (
            partition by match_group_id order by source_trust_rank asc
            rows between unbounded preceding and unbounded following
        ) as line_of_business

    from group_members
    qualify row_number() over (partition by match_group_id order by source_trust_rank asc) = 1

),

-- CMS plan ID: prefer cms_reference first, then other sources
cms_priority_raw as (

    select
        match_group_id,
        first_value(cms_plan_id ignore nulls) over (
            partition by match_group_id
            order by case when source_system = 'cms_reference' then 1 else source_trust_rank + 10 end asc
            rows between unbounded preceding and unbounded following
        ) as cms_plan_id

    from group_members
    qualify row_number() over (
        partition by match_group_id
        order by case when source_system = 'cms_reference' then 1 else source_trust_rank + 10 end asc
    ) = 1

),

-- Most recent survivorship (address, phone)
most_recent_raw as (

    select
        match_group_id,
        first_value(address_line_1 ignore nulls) over (
            partition by match_group_id order by loaded_at desc nulls last
            rows between unbounded preceding and unbounded following
        ) as address_line_1,
        first_value(address_line_2 ignore nulls) over (
            partition by match_group_id order by loaded_at desc nulls last
            rows between unbounded preceding and unbounded following
        ) as address_line_2,
        first_value(city ignore nulls) over (
            partition by match_group_id order by loaded_at desc nulls last
            rows between unbounded preceding and unbounded following
        ) as city,
        first_value(state_code ignore nulls) over (
            partition by match_group_id order by loaded_at desc nulls last
            rows between unbounded preceding and unbounded following
        ) as state_code,
        first_value(zip_code ignore nulls) over (
            partition by match_group_id order by loaded_at desc nulls last
            rows between unbounded preceding and unbounded following
        ) as zip_code,
        first_value(phone ignore nulls) over (
            partition by match_group_id order by loaded_at desc nulls last
            rows between unbounded preceding and unbounded following
        ) as phone

    from group_members
    qualify row_number() over (partition by match_group_id order by loaded_at desc nulls last) = 1

),

-- Most complete = longest non-null string (payor_name_alt)
most_complete_raw as (

    select
        match_group_id,
        max_by(payor_name_alt, length(payor_name_alt)) as payor_name_alt

    from group_members
    where payor_name_alt is not null and trim(payor_name_alt) != ''
    group by match_group_id

),

-- Boolean: active if any source says active
any_active_raw as (

    select
        match_group_id,
        boolor_agg(is_active) as is_active

    from group_members
    group by match_group_id

),

-- Date aggregates
date_agg_raw as (

    select
        match_group_id,
        min(effective_date)   as effective_date,
        max(termination_date) as termination_date

    from group_members
    group by match_group_id

),

final as (

    select
        sp.match_group_id,
        sp.payor_name,
        mc.payor_name_alt,
        sp.tax_id,
        sp.npi,
        cp.cms_plan_id,
        mr.address_line_1,
        mr.address_line_2,
        mr.city,
        mr.state_code,
        mr.zip_code,
        mr.phone,
        sp.website,
        sp.payor_type,
        sp.line_of_business,
        aa.is_active,
        da.effective_date,
        da.termination_date

    from source_priority_raw sp
    left join cms_priority_raw  cp on sp.match_group_id = cp.match_group_id
    left join most_recent_raw   mr on sp.match_group_id = mr.match_group_id
    left join most_complete_raw mc on sp.match_group_id = mc.match_group_id
    left join any_active_raw    aa on sp.match_group_id = aa.match_group_id
    left join date_agg_raw      da on sp.match_group_id = da.match_group_id

)

select * from final
