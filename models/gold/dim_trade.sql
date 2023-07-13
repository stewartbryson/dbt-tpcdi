select 
    {{dbt_utils.generate_surrogate_key(['trade_id','t.effective_timestamp'])}} sk_trade_id,
    trade_id,
    trade_status status,
    transaction_type,
    trade_type type,
    executor_name executed_by,
    t.effective_timestamp,
    t.end_timestamp,
    t.IS_CURRENT
from
    {{ ref('trades_history') }} t
    