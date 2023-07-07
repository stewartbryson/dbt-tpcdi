
select
    *
from {{ source("brokerage", "daily_market") }}
