select
    sk_customer_id,
    sk_security_id,
    to_date(placed_timestamp) sk_date_placed,
    to_date(removed_timestamp) sk_date_removed,
    1 as watch_cnt
from 
    {{ ref('watches') }} w
join
    {{ ref('dim_customer') }} c
ON
    w.customer_id = c.customer_id
and
    placed_timestamp between c.effective_timestamp and c.end_timestamp
join
    {{ ref('dim_security') }} s
ON
    w.symbol = s.symbol
and
    placed_timestamp between s.effective_timestamp and s.end_timestamp
