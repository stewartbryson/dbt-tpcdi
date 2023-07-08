with s1 as (
    select
        symbol,
        issue_type issue,
        s.status,
        s.name,
        exchange_id,
        sk_company_id,
        shares_outstanding,
        first_trade_date,
        first_exchange_date,
        dividend,
        s.effective_timestamp,
        s.end_timestamp,
        s.IS_CURRENT
    from
        {{ ref("securities") }} s
    join
        {{ ref("dim_company") }} c
    on 
        s.company_id = c.company_id
    and
        s.effective_timestamp between c.effective_timestamp and c.end_timestamp
)
select
    {{dbt_utils.generate_surrogate_key(['symbol','effective_timestamp'])}} sk_security_id,
    *
from
    s1
