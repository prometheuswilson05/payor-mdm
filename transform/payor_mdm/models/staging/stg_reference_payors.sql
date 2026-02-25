with source as (

    select * from {{ source('mdm_raw', 'src_reference_payors') }}

),

cleaned as (

    select
        -- source identifiers
        'ref:' || ref_id                             as source_record_id,
        'cms_reference'                              as source_system,
        ref_id                                       as source_id,

        -- name standardization (official_name → payor_name)
        trim(official_name)                          as payor_name,
        upper(
            trim(
                regexp_replace(
                    official_name,
                    '\\s*(,\\s*)?(Inc\\.?|LLC\\.?|Corp\\.?|Co\\.?|Ltd\\.?)\\s*$',
                    '',
                    1, 1, 'i'
                )
            )
        )                                            as payor_name_clean,
        trim(parent_org_name)                        as payor_name_alt,

        -- identifiers (cms_contract_id → cms_plan_id)
        regexp_replace(tax_id, '[^0-9]', '')         as tax_id,
        null::varchar                                as npi,
        upper(trim(cms_contract_id))                 as cms_plan_id,

        -- address: reference data only has state
        null::varchar                                as address_line_1,
        null::varchar                                as address_line_2,
        null::varchar                                as city,
        upper(trim(state))                           as state_code,
        null::varchar                                as zip_code,
        null::varchar                                as zip_plus_4,

        -- contact
        null::varchar                                as phone,
        null::varchar                                as website,

        -- classification (plan_type maps directly)
        lower(trim(plan_type))                       as payor_type,
        null::varchar                                as line_of_business,
        trim(parent_org_name)                        as parent_ref,

        -- status: reference data is active by definition
        true                                         as is_active,
        null::date                                   as effective_date,
        null::date                                   as termination_date,

        -- metadata
        4                                            as source_trust_rank,
        loaded_at

    from source

)

select * from cleaned
