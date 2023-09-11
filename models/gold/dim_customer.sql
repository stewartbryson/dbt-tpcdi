with s1 as (
    select c.*,
           p.agency_id,
           p.credit_rating,
           p.net_worth
    FROM {{ ref('customers') }} c
    left join {{ ref('syndicated_prospect') }} p
    using (first_name, last_name, postal_code, address_line1, address_line2)
),
s2 as (
    SELECT
        {{dbt_utils.generate_surrogate_key(['customer_id','effective_timestamp'])}} sk_customer_id,
        customer_id,
        coalesce(tax_id, last_value(tax_id) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) tax_id,
        status,
        coalesce(last_name, last_value(last_name) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) last_name,
        coalesce(first_name, last_value(first_name) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) first_name,
        coalesce(middle_name, last_value(middle_name) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) middleinitial,
        coalesce(gender, last_value(gender) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) gender,
        coalesce(tier, last_value(tier) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) tier,
        coalesce(dob, last_value(dob) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) dob,
        coalesce(address_line1, last_value(address_line1) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) address_line1,
        coalesce(address_line2, last_value(address_line2) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) address_line2,
        coalesce(postal_code, last_value(postal_code) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) postal_code,
        coalesce(CITY, last_value(CITY) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) CITY,
        coalesce(state_province, last_value(state_province) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) state_province,
        coalesce(country, last_value(country) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) country,
        coalesce(phone1, last_value(phone1) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) phone1,
        coalesce(phone2, last_value(phone2) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) phone2,
        coalesce(phone3, last_value(phone3) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) phone3,
        coalesce(primary_email, last_value(primary_email) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) primary_email,
        coalesce(alternate_email, last_value(alternate_email) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) alternate_email,
        coalesce(local_tax_rate_name, last_value(local_tax_rate_name) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) local_tax_rate_name,
        coalesce(local_tax_rate, last_value(local_tax_rate) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) local_tax_rate,
        coalesce(national_tax_rate_name, last_value(national_tax_rate_name) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) national_tax_rate_name,
        coalesce(national_tax_rate, last_value(national_tax_rate) IGNORE NULLS OVER (
            PARTITION BY customer_id
            ORDER BY effective_timestamp)) national_tax_rate,
        agency_id,
        credit_rating,
        net_worth,
        effective_timestamp,
        end_timestamp,
        is_current
    FROM s1
)
select *
from s2
