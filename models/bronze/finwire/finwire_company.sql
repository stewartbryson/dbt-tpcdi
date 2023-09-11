
select
    *
from {{ source("finwire", "cmp") }}
