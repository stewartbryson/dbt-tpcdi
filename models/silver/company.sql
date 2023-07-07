{{ config(enabled=true) }}

select
    cik as company_id,
    st.st_name status,
    company_name name,
    ind.in_name industry,
    case
        when
            sp_rating in (
                'AAA',
                'AA',
                'AA+',
                'AA-',
                'A',
                'A+',
                'A-',
                'BBB',
                'BBB+',
                'BBB-',
                'BB',
                'BB+',
                'BB-',
                'B',
                'B+',
                'B-',
                'CCC',
                'CCC+',
                'CCC-',
                'CC',
                'C',
                'D'
            )
        then sp_rating
        else cast(null as string)
    end as sp_rating,
    case
        when
            sp_rating
            in ('AAA', 'AA', 'A', 'AA+', 'A+', 'AA-', 'A-', 'BBB', 'BBB+', 'BBB-')
        then false
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
        else cast(null as boolean)
    end as is_lowgrade,
    ceo_name ceo,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    description,
    founding_date,
    nvl2(lead(pts) over (partition by cik order by pts), true, false) is_current
from {{ ref("stg_cmp") }} cmp
join {{ ref("stg_status_type") }} st on cmp.status = st.st_id
join {{ ref("stg_industry") }} ind on cmp.industry_id = ind.in_id
