with s1 as (
    select *,
    try_to_number(co_name_or_cik) as try_cik
    from {{ source("finwire", "sec") }}
)
select  
    pts,
    symbol,
    issue_type,
    status,
    name,
    ex_id,
    to_number(sh_out) as sh_out,
    to_date(first_trade_date,'yyyymmdd') as first_trade_date,
    to_date(first_exchange_date,'yyyymmdd') as first_exchange_date,
    cast(dividend as float) as dividend,
    try_cik cik,
    case when try_cik is null then co_name_or_cik else null end company_name
from s1