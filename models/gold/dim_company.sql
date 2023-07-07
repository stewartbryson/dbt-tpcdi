select
    {{dbt_utils.generate_surrogate_key(['company_id','start_time'])}} company_key,
    company_id,
    status,
    name,
    industry,
    ceo,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    description,
    founding_date,
    sp_rating,
    case
        when
            sp_rating in (
                'BB',
                'B',
                'CCC',
                'CC',
                'C',
                'D',
                'BB+',
                'B+',
                'CCC+',
                'BB-',
                'B-',
                'CCC-'
            )
        then true
        else false
    end as is_lowgrade,
    start_time,
    end_time,
    is_current
from {{ ref("companies") }}