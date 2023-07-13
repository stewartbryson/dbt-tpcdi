with s1 as (
    select 
        *
    from {{ ref('holdings_history') }}
)
select
    ct.sk_trade_id sk_current_trade_id,
    pt.sk_trade_id,
    sk_customer_id,
    sk_account_id,
    sk_security_id,
    to_date(create_timestamp) sk_trade_date,
    create_timestamp trade_timestamp,
    trade_price current_price,
    quantity current_holding,
    bid_price current_bid_price,
    fee current_fee,
    commission current_commission
from s1
join {{ ref('dim_trade') }} ct
using (trade_id)
join {{ ref('dim_trade') }} pt 
on s1.previous_trade_id = pt.trade_id
join {{ ref('dim_account') }} a 
on s1.account_id = a.account_id 
and s1.create_timestamp between a.effective_timestamp and a.end_timestamp
join {{ ref('dim_security') }} s 
on s1.symbol = s.symbol