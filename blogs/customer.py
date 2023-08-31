import sys, json
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
from pathlib import Path

# Read the credentials.json file
with open("credentials.json") as jsonfile:
    credentials_dict = json.load(jsonfile)

# build the session
session = (
    Session
    .builder
    .configs(credentials_dict)
    .create()
)

# glob the files
pathlist = (
    Path('/Users/stewartbryson/dev/tpcdi-output/Batch1')
    .glob("CustomerMgmt.xml")
)
stage_path = "@tpcdi/Batch1/"

for file in pathlist:
    # put the file(s) in the stage
    put_result = (
        session 
        .file
        .put(
            str(file), 
            stage_path,
            parallel=4, 
            auto_compress=True,
            overwrite=False
        )
    )
    for result in put_result:
        print(f"File {result.source}: {result.status}")

# Simplifies retrieving XML elements
def get_xml_element(
        column:str,
        element:str,
        datatype:str,
        with_alias:bool = True
):
    new_element = (
        get(
            xmlget(
                col(column),
                lit(element),
            ),
            lit('$')
        )
        .cast(datatype)
    )

    # alias needs to be optional
    return (
        new_element.alias(element) if with_alias else new_element
    )
    
# Simplifies retrieving XML attributes
def get_xml_attribute(
        column:str,
        attribute:str,
        datatype:str,
        with_alias:bool = True
):
    new_attribute = (
        get(
            col(column),
            lit(f"@{attribute}")
        )
        .cast(datatype)
    )

    # alias needs to be optional
    return (
        new_attribute.alias(attribute) if with_alias else new_attribute
    )
    
# Simplifies the logic for constructing a phone number from multiple nested fields
def get_phone_number(
        phone_id:str,
        separator:str = '-'
):
    return (
        concat (
            get_xml_element(f"phone{phone_id}", 'C_CTRY_CODE', 'STRING', False),
            when(get_xml_element(f"phone{phone_id}", 'C_CTRY_CODE', 'STRING', False) == '', '')
            .otherwise(separator),
            get_xml_element(f"phone{phone_id}", 'C_AREA_CODE', 'STRING', False),
            when(get_xml_element(f"phone{phone_id}", 'C_AREA_CODE', 'STRING', False) == '', '')
            .otherwise(separator),
            get_xml_element(f"phone{phone_id}", 'C_LOCAL', 'STRING', False),
            when(get_xml_element(f"phone{phone_id}", 'C_EXT', 'STRING', False) == '', '')
            .otherwise(" ext: "),
            get_xml_element(f"phone{phone_id}", 'C_EXT', 'STRING', False)
        )
        .alias(f"c_phone_{phone_id}")
    )


# Read the XML file into a DataFrame and show it
table_name = 'customer_mgmt'
df = (
    session
    .read
    .option('STRIP_OUTER_ELEMENT', True) # Strips the TPCDI:Actions node
    .xml(f"{stage_path}CustomerMgmt.xml")
    .select(
        # flatten out all of the nested elements
        xmlget(col('$1'), lit('Customer'), 0).alias('customer'),
        xmlget(col('customer'), lit('Name'), 0).alias('name'),
        xmlget(col('customer'), lit('Address'), 0).alias('address'),
        xmlget(col('customer'), lit('ContactInfo'), 0).alias('contact_info'),
        xmlget(col('contact_info'), lit('C_PHONE_1')).alias('phone1'),
        xmlget(col('contact_info'), lit('C_PHONE_2')).alias('phone2'),
        xmlget(col('contact_info'), lit('C_PHONE_3')).alias('phone3'),
        xmlget(col('customer'), lit('TaxInfo'), 0).alias('tax_info'),
        xmlget(col('customer'), lit('Account'), 0).alias('account'),
        # get the Action attributes
        get_xml_attribute('$1','ActionType','STRING'),
        get_xml_attribute('$1','ActionTS','STRING'),
    )
    .select(
        # Handling Action attributes
        to_timestamp(
            col('ActionTs'),
            lit('yyyy-mm-ddThh:mi:ss')
        ).alias('action_ts'),
        col('ActionType').alias('ACTION_TYPE'),
        # Get Customer Attributes
        get_xml_attribute('customer','C_ID','NUMBER'),
        get_xml_attribute('customer','C_TAX_ID','STRING'),
        get_xml_attribute('customer','C_GNDR','STRING'),
        try_cast(
            get_xml_attribute('customer','C_TIER','STRING', False),
            'NUMBER'
        ).alias('c_tier'),
        get_xml_attribute('customer','C_DOB','DATE'),
        # Get Name elements
        get_xml_element('name','C_L_NAME','STRING'),
        get_xml_element('name','C_F_NAME','STRING'),
        get_xml_element('name','C_M_NAME','STRING'),
        # Get Address elements
        get_xml_element('address','C_ADLINE1','STRING'),
        get_xml_element('address', 'C_ADLINE2', 'STRING'),
        get_xml_element('address','C_ZIPCODE','STRING'),
        get_xml_element('address','C_CITY','STRING'),
        get_xml_element('address','C_STATE_PROV','STRING'),
        get_xml_element('address','C_CTRY','STRING'),
        # Get Contact Info elements
        get_xml_element('contact_info','C_PRIM_EMAIL','STRING'),
        get_xml_element('contact_info','C_ALT_EMAIL','STRING'),
        # Contruct phone numbers from multi-nested elements
        get_phone_number('1'),
        get_phone_number('2'),
        get_phone_number('3'),
        # Get TaxInfo elements
        get_xml_element('tax_info','C_LCL_TX_ID','STRING'),
        get_xml_element('tax_info','C_NAT_TX_ID','STRING'),
        # Get Account Attributes
        get_xml_attribute('account','CA_ID','STRING'),
        get_xml_attribute('account','CA_TAX_ST','NUMBER'),
        # Get Account elements
        get_xml_element('account','CA_B_ID','NUMBER'),
        get_xml_element('account','CA_NAME','STRING'),
    )
    .write
    .mode("overwrite")
    .save_as_table(table_name)
)

print(f"{table_name.upper()} table created.")

df = (
    session
    .table('customer_mgmt')
    .select(
        col('action_ts'),
        col('c_id'),
        col('c_tier'),
        col('c_phone_1')
    )
    .show()
)
