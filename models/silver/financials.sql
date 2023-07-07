with s1 as (
    select
        YEAR,
        QUARTER,
        QUARTER_START_DATE,
        POSTING_DATE,
        REVENUE,
        EARNINGS,
        EPS,
        DILUTED_EPS,
        MARGIN,
        INVENTORY,
        ASSETS,
        LIABILITIES,
        SH_OUT,
        DILUTED_SH_OUT,
        coalesce(c1.name,c2.name) company_name,
        coalesce(c1.company_id, c2.company_id) company_id,
        pts as start_time
    from {{ ref('finwire_financials') }} s 
    left join {{ ref('companies') }} c1
    on s.cik = c1.company_id
    and pts between c1.start_time and c1.end_time
    left join {{ ref('companies') }} c2
    on s.company_name = c2.name
    and pts between c2.start_time and c2.end_time
)
select
    *,
    ifnull(
        timestampadd(
        'millisecond',
        -1,
        lag(start_time) over (
            partition by company_id
            order by
            start_time desc
        )
        ),
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_time,
    CASE
        WHEN (
            row_number() over (
                partition by company_id
                order by
                start_time desc
            ) = 1
        ) THEN TRUE
        ELSE FALSE
    END as IS_CURRENT
from s1
