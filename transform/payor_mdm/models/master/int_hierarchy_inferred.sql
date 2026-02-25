-- Infer parent-child relationships between golden records.
-- Two signals:
--   1. parent_ref in source record matches another golden record's payor_name (name match)
--   2. One golden record's payor_name is a substring of another (contains relationship)

with golden as (

    select * from {{ ref('golden_payors') }}

),

stg as (

    select
        source_record_id,
        parent_ref

    from {{ ref('stg_payors_unioned') }}
    where parent_ref is not null and parent_ref != ''

),

xref as (

    select * from {{ ref('xref') }}

),

-- Signal 1: source record has a parent_ref → look up the parent in golden records
parent_ref_matches as (

    select distinct
        g_parent.master_payor_id    as parent_master_id,
        g_child.master_payor_id     as child_master_id,
        'name_ref_match'            as inference_method

    from stg s
    join xref xc            on s.source_record_id = xc.source_record_id
    join golden g_child     on xc.master_payor_id = g_child.master_payor_id
    join golden g_parent
        on upper(trim(s.parent_ref)) = upper(trim(g_parent.payor_name))
        and g_parent.master_payor_id != g_child.master_payor_id

),

-- Signal 2: one golden name is a substring of another (A is parent of B if B contains A)
substring_matches as (

    select distinct
        g_parent.master_payor_id    as parent_master_id,
        g_child.master_payor_id     as child_master_id,
        'name_substring'            as inference_method

    from golden g_parent
    join golden g_child
        on g_parent.master_payor_id != g_child.master_payor_id
        -- parent name appears in child name (e.g. "UnitedHealth" in "UnitedHealthcare of CA")
        and contains(upper(g_child.payor_name), upper(g_parent.payor_name))
        and length(g_parent.payor_name) >= 6  -- avoid single-word false positives
        -- child must be longer than parent
        and length(g_child.payor_name) > length(g_parent.payor_name)

),

combined as (

    select * from parent_ref_matches
    union
    select * from substring_matches

)

select
    uuid_string()               as inferred_hierarchy_id,
    parent_master_id,
    child_master_id,
    inference_method,
    current_timestamp()         as inferred_at

from combined
-- Don't infer circular relationships
where parent_master_id != child_master_id
