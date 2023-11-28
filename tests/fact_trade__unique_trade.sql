select 
    sk_trade_id, 
    count(*) cnt
from {{ ref('fact_trade') }} 
group by all
having cnt > 1