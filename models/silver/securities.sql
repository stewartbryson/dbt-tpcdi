select
    symbol,
    issue_type,
    s.status,
    s.name,
    ex_id,
    sh_out,
    first_trade_date,
    first_exchange_date,
    dividend,
    coalesce(c1.name,c2.name) company_name,
    coalesce(c1.company_id, c2.company_id) company_id,
    pts as start_time,
    ifnull(
        timestampadd(
        'millisecond',
        -1,
        lag(pts) over (
            partition by symbol
            order by
            pts desc
        )
        ),
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_time,
    CASE
        WHEN (
            row_number() over (
                partition by symbol
                order by
                pts desc
            ) = 1
        ) THEN TRUE
        ELSE FALSE
    END as IS_CURRENT
from {{ ref('finwire_securities') }} s 
left join {{ ref('companies') }} c1
on s.cik = c1.company_id
and pts between c1.start_time and c1.end_time
left join {{ ref('companies') }} c2
on s.company_name = c2.name
and pts between c2.start_time and c2.end_time
order by symbol, pts