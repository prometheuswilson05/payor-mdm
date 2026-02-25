with source as (

    select * from {{ source('mdm_raw', 'src_crm_payors') }}

),

cleaned as (

    select
        -- source identifiers
        'crm:' || src_payor_id                      as source_record_id,
        'crm'                                        as source_system,
        src_payor_id                                 as source_id,

        -- name standardization
        trim(payor_name)                             as payor_name,
        upper(
            trim(
                regexp_replace(
                    payor_name,
                    '\\s*(,\\s*)?(Inc\\.?|LLC\\.?|Corp\\.?|Co\\.?|Ltd\\.?|L\\.?P\\.?|L\\.?L\\.?C\\.?|P\\.?C\\.?)\\s*$',
                    '',
                    1, 1, 'i'
                )
            )
        )                                            as payor_name_clean,
        trim(payor_name_2)                           as payor_name_alt,

        -- identifiers
        regexp_replace(tax_id, '[^0-9]', '')         as tax_id,
        trim(npi)                                    as npi,
        upper(trim(cms_plan_id))                     as cms_plan_id,

        -- address
        trim(address_line_1)                         as address_line_1,
        trim(address_line_2)                         as address_line_2,
        upper(trim(city))                            as city,
        upper(left(trim(state), 2))                  as state_code,
        left(regexp_replace(zip, '[^0-9]', ''), 5)   as zip_code,
        case
            when length(regexp_replace(zip, '[^0-9]', '')) > 5
            then substr(regexp_replace(zip, '[^0-9]', ''), 6, 4)
        end                                          as zip_plus_4,

        -- contact
        regexp_replace(phone, '[^0-9]', '')          as phone,
        trim(website)                                as website,

        -- classification
        lower(trim(payor_type))                      as payor_type,
        null::varchar                                as line_of_business,
        parent_payor_id                              as parent_ref,

        -- status
        (status = 'active')                          as is_active,
        try_to_date(effective_date::varchar)         as effective_date,
        try_to_date(termination_date::varchar)       as termination_date,

        -- metadata
        1                                            as source_trust_rank,
        loaded_at

    from source

)

select * from cleaned
