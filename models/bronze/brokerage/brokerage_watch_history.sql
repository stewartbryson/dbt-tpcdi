select
    *
from {{ source("brokerage", "watch_history") }}