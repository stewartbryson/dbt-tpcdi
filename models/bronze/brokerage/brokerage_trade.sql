select *
from {{ source('brokerage', 'trade') }}
