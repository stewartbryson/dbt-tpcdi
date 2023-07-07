select *
from {{ source('digen','status_type') }}