select *
from {{ source('crm', 'customer_mgmt') }}