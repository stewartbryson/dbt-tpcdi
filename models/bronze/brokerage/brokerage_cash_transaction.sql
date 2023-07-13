select *
from {{ source('brokerage', 'cash_transaction') }}
