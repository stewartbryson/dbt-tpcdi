select
    sk_trade_id,
    sk_broker_id,
    sk_customer_id,
    sk_account_id,
    sk_security_id,
    to_date(create_timestamp) sk_create_date,
    create_timestamp,
    to_date(close_timestamp) sk_close_date,
    close_timestamp,
    executed_by,
    quantity,
    bid_price,
    trade_price,
    fee,
    commission,
    tax
from {{ ref('trades') }} t
join {{ ref('dim_trade') }} dt
on t.trade_id = dt.trade_id
and t.create_timestamp between dt.effective_timestamp and dt.end_timestamp
join
    {{ ref('dim_account') }} a
on 
    t.account_id = a.account_id
and
    t.create_timestamp between a.effective_timestamp and a.end_timestamp
join
    {{ ref('dim_security') }} s
on
    t.symbol = s.symbol
and
    t.create_timestamp between s.effective_timestamp and s.end_timestamp