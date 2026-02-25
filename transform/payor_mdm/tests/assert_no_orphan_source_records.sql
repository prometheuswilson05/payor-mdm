-- Every record in stg_payors_unioned must appear in xref.
-- Returns rows that are in staging but NOT in xref (orphans = test failures).
-- An empty result = test passes.

select
    s.source_record_id,
    s.source_system,
    s.payor_name

from {{ ref('stg_payors_unioned') }} s
left join {{ ref('xref') }} x
    on s.source_record_id = x.source_record_id

where x.source_record_id is null
