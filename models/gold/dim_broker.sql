select
    {{dbt_utils.generate_surrogate_key(['employee_id'])}} sk_broker_id,
    employee_id broker_id,
    manager_id,
    first_name,
    last_name,
    middle_initial,
    job_code,
    branch,
    office,
    phone
from
    {{ ref('employees') }}