with unioned as (

    select * from {{ ref('stg_crm_payors') }}
    union all
    select * from {{ ref('stg_claims_payors') }}
    union all
    select * from {{ ref('stg_credentialing_payors') }}
    union all
    select * from {{ ref('stg_reference_payors') }}

),

with_blocking_keys as (

    select
        source_record_id,
        source_system,
        source_id,
        payor_name,
        payor_name_clean,
        payor_name_alt,
        tax_id,
        npi,
        cms_plan_id,
        address_line_1,
        address_line_2,
        city,
        state_code,
        zip_code,
        zip_plus_4,
        phone,
        website,
        payor_type,
        line_of_business,
        parent_ref,
        is_active,
        effective_date,
        termination_date,
        source_trust_rank,
        loaded_at,

        -- Blocking key 1: first 6 chars of clean name + state
        left(coalesce(payor_name_clean, ''), 6)
            || coalesce(state_code, 'XX')            as block_name_key,

        -- Blocking key 2: exact tax_id match
        tax_id                                       as block_tax_id,

        -- Blocking key 3: first 3 of zip + first 4 of clean name
        coalesce(left(zip_code, 3), 'XXX')
            || left(coalesce(payor_name_clean, ''), 4) as block_zip3_name4

    from unioned

)

select * from with_blocking_keys
