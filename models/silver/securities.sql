select
    symbol,
    issue_type,
    case s.status
        when 'ACTV' then 'Active'
        when 'INAC' then 'Inactive'
        else null
    end status,
    s.name,
    ex_id exchange_id,
    sh_out shares_outstanding,
    first_trade_date,
    first_exchange_date,
    dividend,
    coalesce(c1.name,c2.name) company_name,
    coalesce(c1.company_id, c2.company_id) company_id,
    pts as effective_timestamp,
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
    ) as end_timestamp,
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
from {{ ref('finwire_security') }} s 
left join {{ ref('companies') }} c1
on s.cik = c1.company_id
and pts between c1.effective_timestamp and c1.end_timestamp
left join {{ ref('companies') }} c2
on s.company_name = c2.name
and pts between c2.effective_timestamp and c2.end_timestamp
