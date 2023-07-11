select 
    employee_id,
    manager_id,
    employee_first_name first_name,
    employee_last_name last_name,
    employee_mi middle_initial,
    employee_job_code job_code,
    employee_branch branch,
    employee_office office,
    employee_phone phone
from {{ ref('hr_employee') }}