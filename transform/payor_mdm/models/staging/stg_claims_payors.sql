-- State name → abbreviation lookup
with state_map as (
    select * from (values
        ('ALABAMA','AL'),('ALASKA','AK'),('ARIZONA','AZ'),('ARKANSAS','AR'),
        ('CALIFORNIA','CA'),('COLORADO','CO'),('CONNECTICUT','CT'),('DELAWARE','DE'),
        ('FLORIDA','FL'),('GEORGIA','GA'),('HAWAII','HI'),('IDAHO','ID'),
        ('ILLINOIS','IL'),('INDIANA','IN'),('IOWA','IA'),('KANSAS','KS'),
        ('KENTUCKY','KY'),('LOUISIANA','LA'),('MAINE','ME'),('MARYLAND','MD'),
        ('MASSACHUSETTS','MA'),('MICHIGAN','MI'),('MINNESOTA','MN'),('MISSISSIPPI','MS'),
        ('MISSOURI','MO'),('MONTANA','MT'),('NEBRASKA','NE'),('NEVADA','NV'),
        ('NEW HAMPSHIRE','NH'),('NEW JERSEY','NJ'),('NEW MEXICO','NM'),('NEW YORK','NY'),
        ('NORTH CAROLINA','NC'),('NORTH DAKOTA','ND'),('OHIO','OH'),('OKLAHOMA','OK'),
        ('OREGON','OR'),('PENNSYLVANIA','PA'),('RHODE ISLAND','RI'),('SOUTH CAROLINA','SC'),
        ('SOUTH DAKOTA','SD'),('TENNESSEE','TN'),('TEXAS','TX'),('UTAH','UT'),
        ('VERMONT','VT'),('VIRGINIA','VA'),('WASHINGTON','WA'),('WEST VIRGINIA','WV'),
        ('WISCONSIN','WI'),('WYOMING','WY'),('DISTRICT OF COLUMBIA','DC')
    ) as t(state_name, abbrev)
),

source as (

    select * from {{ source('mdm_raw', 'src_claims_payors') }}

),

cleaned as (

    select
        -- source identifiers
        'claims:' || claims_payor_code               as source_record_id,
        'claims'                                     as source_system,
        claims_payor_code                            as source_id,

        -- name standardization
        trim(payor_name)                             as payor_name,
        upper(
            trim(
                regexp_replace(
                    payor_name,
                    '\\s*(,\\s*)?(Inc\\.?|LLC\\.?|Corp\\.?|Co\\.?|Ltd\\.?|L\\.?P\\.?|L\\.?L\\.?C\\.?)\\s*$',
                    '',
                    1, 1, 'i'
                )
            )
        )                                            as payor_name_clean,
        null::varchar                                as payor_name_alt,

        -- identifiers
        regexp_replace(tax_id, '[^0-9]', '')         as tax_id,
        null::varchar                                as npi,
        null::varchar                                as cms_plan_id,

        -- address: parse single-line "street, city, state, zip" format
        -- try to extract from the 'address' column; fall back to explicit columns
        case
            when address is not null and city is null
            then trim(split_part(address, ',', 1))
            else null::varchar
        end                                          as address_line_1,
        null::varchar                                as address_line_2,
        upper(trim(coalesce(
            nullif(city, ''),
            nullif(split_part(address, ',', 2), '')
        )))                                          as city,

        -- state: handle full name vs abbreviation
        coalesce(
            sm.abbrev,
            case when length(trim(state)) = 2
                 then upper(trim(state))
            end
        )                                            as state_code,

        left(regexp_replace(zip, '[^0-9]', ''), 5)  as zip_code,
        case
            when length(regexp_replace(zip, '[^0-9]', '')) > 5
            then substr(regexp_replace(zip, '[^0-9]', ''), 6, 4)
        end                                          as zip_plus_4,

        -- contact
        null::varchar                                as phone,
        null::varchar                                as website,

        -- classification
        lower(trim(payor_type))                      as payor_type,
        lower(trim(line_of_business))                as line_of_business,
        null::varchar                                as parent_ref,

        -- status
        is_active,
        null::date                                   as effective_date,
        null::date                                   as termination_date,

        -- metadata
        3                                            as source_trust_rank,
        loaded_at

    from source s
    left join state_map sm
        on upper(trim(s.state)) = sm.state_name

)

select * from cleaned
