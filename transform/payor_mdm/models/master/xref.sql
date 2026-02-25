-- Cross-reference: every source record mapped to its golden (master) record.
-- match_group_id from match_groups is already the stable UUID for the group.

with mg as (

    select * from {{ ref('match_groups') }}

),

stg as (

    select * from {{ ref('stg_payors_unioned') }}

)

select
    mg.source_record_id,
    mg.match_group_id           as master_payor_id,
    s.source_system,
    s.source_id,
    mg.group_confidence         as match_confidence,
    current_timestamp()         as created_at

from mg
join stg s on mg.source_record_id = s.source_record_id
