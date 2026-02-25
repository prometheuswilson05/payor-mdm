-- Payor hierarchy: inferred parent-child relationships between golden records.
-- steward_confirmed=FALSE until a steward approves via the UI.

with inferred as (

    select * from {{ ref('int_hierarchy_inferred') }}

)

select
    uuid_string()               as hierarchy_id,
    parent_master_id,
    child_master_id,
    'parent_company'            as relationship_type,   -- default; steward can refine
    null::date                  as effective_date,
    null::date                  as end_date,
    inference_method            as source,
    false                       as steward_confirmed,
    current_timestamp()         as created_at

from inferred
