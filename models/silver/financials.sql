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
        pts as effective_timestamp
    from {{ ref('finwire_financial') }} s 
    left join {{ ref('companies') }} c1
    on s.cik = c1.company_id
    and pts between c1.effective_timestamp and c1.end_timestamp
    left join {{ ref('companies') }} c2
    on s.company_name = c2.name
    and pts between c2.effective_timestamp and c2.end_timestamp
)
select
    *,
    ifnull(
        timestampadd(
        'millisecond',
        -1,
        lag(effective_timestamp) over (
            partition by company_id
            order by
            effective_timestamp desc
        )
        ),
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
        WHEN (
            row_number() over (
                partition by company_id
                order by
                effective_timestamp desc
            ) = 1
        ) THEN TRUE
        ELSE FALSE
    END as IS_CURRENT
from s1
