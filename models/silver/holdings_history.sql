with s1 as (
    select
        HH_T_ID trade_id,
        HH_H_T_ID previous_trade_id,
        hh_before_qty previous_quantity,
        hh_after_qty quantity
    from {{ ref('brokerage_holding_history') }}
)
select s1.*,
       ct.account_id account_id,
       ct.symbol symbol,
       ct.create_timestamp,
       ct.close_timestamp,
       ct.trade_price,
       ct.bid_price,
       ct.fee,
       ct.commission
from s1
join {{ ref('trades') }} ct
using (trade_id)

