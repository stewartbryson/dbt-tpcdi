select
    action_type,
    decode(action_type,
      'NEW','Active',
      'ADDACCT','Active',
      'UPDACCT','Active',
      'UPDCUST','Active',
      'INACT','Inactive') status,
    c_id customer_id,
    ca_id account_id,
    c_tax_id tax_id,
    c_gndr gender,
    c_tier tier,
    c_dob dob,
    c_l_name last_name,
    c_f_name first_name,
    c_m_name middle_name,
    c_adline1 address_line1,
    c_adline2 address_line2,
    c_zipcode postal_code,
    c_city city,
    c_state_prov state_province,
    c_ctry country,
    c_prim_email primary_email,
    c_alt_email alternate_email,
    c_phone_1 phone1,
    c_phone_2 phone2,
    c_phone_3 phone3,
    c_lcl_tx_id local_tax_rate_name,
    ltx.tx_rate local_tax_rate,
    c_nat_tx_id national_tax_rate_name,
    ntx.tx_rate national_tax_rate,
    ca_tax_st account_tax_status,
    ca_b_id broker_id,
    action_ts as effective_timestamp,
    ifnull(
        timestampadd(
            'millisecond',
            -1,
            lag(action_ts) over (
                partition by c_id
                order by
                action_ts desc
            )
        ),
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
        WHEN (
            row_number() over (
                partition by c_id
                order by
                action_ts desc
            ) = 1
        ) THEN TRUE
        ELSE FALSE
    END as IS_CURRENT
from
    {{ ref('crm_customer_mgmt') }} c
left join
    {{ ref('reference_tax_rate') }} ntx
on
    c.c_nat_tx_id = ntx.tx_id
left join
    {{ ref('reference_tax_rate') }} ltx
on
    c.c_lcl_tx_id = ltx.tx_id
where action_type in ('NEW', 'INACT', 'UPDCUST')