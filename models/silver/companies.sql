
select
    cik as company_id,
    st.st_name status,
    company_name name,
    ind.in_name industry,
    ceo_name ceo,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    description,
    founding_date,
    sp_rating,
    pts as effective_timestamp,
    ifnull(
        timestampadd(
            'millisecond',
            -1,
            lag(pts) over (
                partition by company_id
                order by
                pts desc
            )
        ),
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
        WHEN (
            row_number() over (
                partition by company_id
                order by
                pts desc
            ) = 1
        ) THEN TRUE
        ELSE FALSE
    END as IS_CURRENT
from {{ ref("finwire_company") }} cmp
join {{ ref("reference_status_type") }} st on cmp.status = st.st_id
join {{ ref("reference_industry") }} ind on cmp.industry_id = ind.in_id
