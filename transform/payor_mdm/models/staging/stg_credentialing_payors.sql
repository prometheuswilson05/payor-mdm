with source as (

    select * from {{ source('mdm_raw', 'src_credentialing_payors') }}

),

cleaned as (

    select
        -- source identifiers
        'cred:' || cred_payor_id                     as source_record_id,
        'credentialing'                               as source_system,
        cred_payor_id                                 as source_id,

        -- name standardization (organization_name → payor_name)
        trim(organization_name)                      as payor_name,
        upper(
            trim(
                regexp_replace(
                    organization_name,
                    '\\s*(,\\s*)?(Inc\\.?|LLC\\.?|Corp\\.?|Co\\.?|Ltd\\.?|L\\.?P\\.?|Health\\.?|Inc\\.?)\\s*$',
                    '',
                    1, 1, 'i'
                )
            )
        )                                            as payor_name_clean,
        trim(doing_business_as)                      as payor_name_alt,

        -- identifiers (ein → tax_id)
        regexp_replace(ein, '[^0-9]', '')            as tax_id,
        null::varchar                                as npi,
        null::varchar                                as cms_plan_id,

        -- address (street_address → address_line_1, suite → address_line_2, postal_code → zip_code)
        trim(street_address)                         as address_line_1,
        trim(suite)                                  as address_line_2,
        upper(trim(city))                            as city,
        upper(trim(state_code))                      as state_code,
        left(regexp_replace(postal_code, '[^0-9]', ''), 5) as zip_code,
        case
            when length(regexp_replace(postal_code, '[^0-9]', '')) > 5
            then substr(regexp_replace(postal_code, '[^0-9]', ''), 6, 4)
        end                                          as zip_plus_4,

        -- contact (contact_phone → phone)
        regexp_replace(contact_phone, '[^0-9]', '') as phone,
        null::varchar                                as website,

        -- classification (plan_type → payor_type)
        lower(trim(plan_type))                       as payor_type,
        null::varchar                                as line_of_business,
        null::varchar                                as parent_ref,

        -- status
        (network_status = 'in_network')              as is_active,
        null::date                                   as effective_date,
        null::date                                   as termination_date,

        -- metadata
        2                                            as source_trust_rank,
        loaded_at

    from source

)

select * from cleaned
