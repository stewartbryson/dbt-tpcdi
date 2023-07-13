with s1 as (
    select distinct
        trade_id,
        account_id,
        trade_status,
        trade_type,
        transaction_type,
        symbol,
        executor_name,
        quantity,
        bid_price,
        trade_price,
        fee,
        commission,
        tax,
        min(effective_timestamp) over (partition by trade_id) create_timestamp,
        max(effective_timestamp) over (partition by trade_id) close_timestamp
    from
        {{ ref('trades_history') }}
    order by trade_id, create_timestamp
)
select *
from s1