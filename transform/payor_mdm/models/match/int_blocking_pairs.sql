-- Generate candidate pairs by blocking: two records must share at least
-- one blocking key to be considered for matching. source_record_id_a < _b
-- prevents duplicate pairs and self-joins.

with stg as (

    select * from {{ ref('stg_payors_unioned') }}

),

pairs as (

    select distinct
        a.source_record_id     as source_record_id_a,
        b.source_record_id     as source_record_id_b

    from stg a
    join stg b
        on a.source_record_id < b.source_record_id   -- canonical order, no self-joins
        and (
            -- Block 1: same tax_id (both non-null)
            (
                a.block_tax_id is not null
                and a.block_tax_id != ''
                and a.block_tax_id = b.block_tax_id
            )
            -- Block 2: name prefix + state
            or a.block_name_key = b.block_name_key
            -- Block 3: zip prefix + name prefix
            or a.block_zip3_name4 = b.block_zip3_name4
        )

)

select * from pairs
