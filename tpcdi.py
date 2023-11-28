import sys, typer, json, re, logging
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
from typing_extensions import Annotated
from pathlib import Path

app = typer.Typer(help="A utility for loading TPC-DI generated files into Snowflake.")

def get_session():
    # Read the credentials.json file
    with open("credentials.json") as jsonfile:
        credentials_dict = json.load(jsonfile)

    # build the session
    session = Session.builder.configs(credentials_dict).create()
    #print(f"Session: {session}")

    # and return it
    return session

@app.command(help="CREATE or REPLACE the stage. Mostly useful while developing this utility.")
def recreate_stage(
        stage: Annotated[str, typer.Option(help="Name of the stage to recreate.")] = 'tpcdi',
):
    session = get_session()
    session.sql(f"create or replace stage {stage} directory = (enable = true)").collect()
    print(f"Stage {stage} recreated.")

@app.command(help="DROP the stage. Useful when all the data has been successfully loaded.")
def drop_stage(
        stage: Annotated[str, typer.Option(help="Name of the stage to drop.")] = 'tpcdi',
):
    session = get_session()
    session.sql(f"drop stage {stage}").collect()
    print(f"Stage {stage} dropped.")

@app.command(help="DROP a schema. Useful for cleaning up after a demo.")
def drop_schema(
        schema: Annotated[str, typer.Option(help="Name of the schema to drop.")],
):
    session = get_session()
    session.sql(f"drop schema if exists {schema}").collect()
    print(f"Schema {schema} dropped.")

@app.command(help="Upload a file or files into the stage and build the dependent tables.")
def process_files(
    output_directory: Annotated[str, typer.Option(help='The output directory from the TPC-DI DIGen.jar execution.')],
    file_name: Annotated[str, typer.Option(help="The TPC-DI file name to upload and process. Pass value 'FINWIRE' to process all of the financial wire files.")] = 'all',
    stage: Annotated[str, typer.Option(help="The stage name to upload to, without specifying '@'.")] = 'tpcdi',
    batch: Annotated[int, typer.Option(help="The TPC-DI batch number to process. Currently only supports the default of '1'.")] = 1,
    overwrite: Annotated[bool, typer.Option(help="Overwrite the file even if it exists?")] = False,
    skip_upload: Annotated[bool, typer.Option(help="Skip uploading the files?")] = False,
    show: Annotated[bool, typer.Option(help="Show the DataFrame instead of saving it as a table? This was useful during development.")] = False,
):
    session = get_session()

    # create the stage if it doesn't exist
    (
        session
        .sql(f"create stage if not exists {stage} directory = (enable = true)")
        .collect()
    )

    def get_stage_path(
            stage:str,
            file_name:str,
    ):
        return f"@{stage}/Batch{batch}/{file_name}"

    # method to control printing the dataframe or saving it
    def save_df(
            df: DataFrame,
            table_name: str
    ):
        if show:
            df.show()
        else:
            (
                df.write
                .mode("overwrite")
                .save_as_table(table_name)
            )

            print(f"{table_name.upper()} table created.")

    # method for uploading files
    def upload_files(
            file_name: str,
            stage_path: str,
    ):

        delimiter="|"
            
        if file_name == 'FINWIRE':
            pathlist = Path(output_directory).glob(f"Batch{batch}/FINWIRE??????")
        else:
            pathlist = Path(output_directory).glob(f"Batch{batch}/{file_name}")
        
        for file in pathlist:
            # capture the delimiter
            if file_name != 'FINWIRE':
                logging.info(f"Suffix: {file.suffix}")
                if file.suffix == '.csv':
                    delimiter=','
                logging.info(f"Delimiter: {delimiter}")

            # put the file(s) in the stage
            if not skip_upload:
                put_result = (
                        session
                        .file
                        .put(
                            str(file), 
                            stage_path, 
                            overwrite=overwrite, 
                            parallel=4, 
                            auto_compress=True 
                        )
                )
                for result in put_result:
                    print(f"File {result.source}: {result.status}")
        
        # return the delimiter
        return delimiter
        
    # method for creating a table from a simple CSV
    # works for either comma or pipe delimited, detecting that automatically by suffix
    def load_csv(
            schema: StructType,
            file_name: str,
            table_name: str,
    ):
        stage_path = get_stage_path(stage, file_name)
        delimiter=upload_files(file_name, stage_path)
        
        df = (
            session
            .read
            .schema(schema)
            .option("field_delimiter", delimiter)
            .csv(stage_path)
        )
        
        save_df(df, table_name)

    # Simplifies the DataFrame transformations for retrieving XML elements
    def get_xml_element(
            column:str,
            element:str,
            datatype:str,
            with_alias:bool = True
    ):
        new_element = get(xmlget(col(column), lit(element)), lit('$')).cast(datatype)
        # alias needs to be optional
        return (
            new_element.alias(element) if with_alias else new_element
        )
    
    # Simplifies the DataFrame transformations for retrieving XML attributes
    def get_xml_attribute(
            column:str,
            attribute: str,
            datatype: str,
            with_alias:bool = True
    ):
        new_attribute = get(col(column), lit(f"@{attribute}")).cast(datatype)
        # alias needs to be optional
        return (
            new_attribute.alias(attribute) if with_alias else new_attribute
        )
    
    # Simplifies the logic for constructing a phone number from multiple nested fields
    def get_phone_number(
            phone_id:str,
            separator:str = '-'
    ):
        return concat(
            get_xml_element(f"phone{phone_id}", 'C_CTRY_CODE', 'STRING', False),
            when(get_xml_element(f"phone{phone_id}", 'C_CTRY_CODE', 'STRING', False) == '', '').otherwise(separator),
            get_xml_element(f"phone{phone_id}", 'C_AREA_CODE', 'STRING', False),
            when(get_xml_element(f"phone{phone_id}", 'C_AREA_CODE', 'STRING', False) == '', '').otherwise(separator),
            get_xml_element(f"phone{phone_id}", 'C_LOCAL', 'STRING', False),
            when(get_xml_element(f"phone{phone_id}", 'C_EXT', 'STRING', False) == '', '').otherwise(" ext: "),
            get_xml_element(f"phone{phone_id}", 'C_EXT', 'STRING', False)
        ).alias(f"c_phone_{phone_id}")
    
    
    ### Start defining and loading the actual tables
    ### Variable 'con_file_name' declaration acts as the comment.

    con_file_name = 'Date.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("SK_DATE_ID", IntegerType(), False),
                StructField("DATE_VALUE", DateType(), False),
                StructField("DATE_DESC", StringType(), False),
                StructField("CALENDAR_YEAR_ID", IntegerType(), False),
                StructField("CALENDAR_YEAR_DESC", StringType(), False),
                StructField("CALENDAR_QTR_ID", IntegerType(), False),
                StructField("CALENDAR_QTR_DESC", StringType(), False),
                StructField("CALENDAR_MONTH_ID", IntegerType(), False),
                StructField("CALENDAR_MONTH_DESC", StringType(), False),
                StructField("CALENDAR_WEEK_ID", IntegerType(), False),
                StructField("CALENDAR_WEEK_DESC", StringType(), False),
                StructField("DAY_OF_WEEK_NUM", IntegerType(), False),
                StructField("DAY_OF_WEEK_DESC", StringType(), False),
                StructField("FISCAL_YEAR_ID", IntegerType(), False),
                StructField("FISCAL_YEAR_DESC", StringType(), False),
                StructField("FISCAL_QTR_ID", IntegerType(), False),
                StructField("FISCAL_QTR_DESC", StringType(), False),
                StructField("HOLIDAY_FLAG", BooleanType(), False),
        ])
        load_csv(schema, con_file_name, 'date')

    con_file_name = 'DailyMarket.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("DM_DATE", DateType(), False),
                StructField("DM_S_SYMB", StringType(), False),
                StructField("DM_CLOSE", FloatType(), False),
                StructField("DM_HIGH", FloatType(), False),
                StructField("DM_LOW", FloatType(), False),
                StructField("DM_VOL", FloatType(), False),
        ])
        load_csv(schema, con_file_name, 'daily_market')

    con_file_name = 'Industry.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("IN_ID", StringType(), False),
                StructField("IN_NAME", StringType(), False),
                StructField("IN_SC_ID", StringType(), False),
        ])
        load_csv(schema, con_file_name, 'industry')

    con_file_name = 'Prospect.csv'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("AGENCY_ID", StringType(), False),
                StructField("LAST_NAME", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("MIDDLE_INITIAL", StringType(), True),
                StructField("GENDER", StringType(), True),
                StructField("ADDRESS_LINE1", StringType(), True),
                StructField("ADDRESS_LINE2", StringType(), True),
                StructField("POSTAL_CODE", StringType(), True),
                StructField("CITY", StringType(), True),
                StructField("STATE", StringType(), True),
                StructField("COUNTRY", StringType(), True),
                StructField("PHONE", StringType(), True),
                StructField("INCOME", IntegerType(), True),
                StructField("NUMBER_CARS", IntegerType(), True),
                StructField("NUMBER_CHILDREN", IntegerType(), True),
                StructField("MARITAL_STATUS", StringType(), True),
                StructField("AGE", IntegerType(), True),
                StructField("CREDIT_RATING", IntegerType(), True),
                StructField("OWN_OR_RENT_FLAG", StringType(), True),
                StructField("EMPLOYER", StringType(), True),
                StructField("NUMBER_CREDIT_CARDS", IntegerType(), True),
                StructField("NET_WORTH", IntegerType(), True),
        ])
        load_csv(schema, con_file_name, 'prospect')

    con_file_name = 'CustomerMgmt.xml'
    if file_name in ['all', con_file_name]:

        upload_files(con_file_name, get_stage_path(stage, con_file_name))

        # this might get hairy
        df = (
            session
            .read
            .option('STRIP_OUTER_ELEMENT', True) # Strips the TPCDI:Actions element
            .xml(get_stage_path(stage, con_file_name))
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
                to_timestamp(col('ActionTs'), lit('yyyy-mm-ddThh:mi:ss')).alias('action_ts'),
                col('ActionType').alias('ACTION_TYPE'),
                # Get Customer Attributes
                get_xml_attribute('customer','C_ID','NUMBER'),
                get_xml_attribute('customer','C_TAX_ID','STRING'),
                get_xml_attribute('customer','C_GNDR','STRING'),
                try_cast(get_xml_attribute('customer','C_TIER','STRING', False),'NUMBER').alias('c_tier'),
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
        )
        
        save_df(df, 'customer_mgmt')

    con_file_name = 'TaxRate.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("TX_ID", StringType(), False),
                StructField("TX_NAME", StringType(), True),
                StructField("TX_RATE", FloatType(), True),
        ])
        load_csv(schema, con_file_name, 'tax_rate')

    con_file_name = 'HR.csv'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("EMPLOYEE_ID", IntegerType(), False),
                StructField("MANAGER_ID", IntegerType(), False),
                StructField("EMPLOYEE_FIRST_NAME", StringType(), True),
                StructField("EMPLOYEE_LAST_NAME", StringType(), True),
                StructField("EMPLOYEE_MI", StringType(), True),
                StructField("EMPLOYEE_JOB_CODE", IntegerType(), True),
                StructField("EMPLOYEE_BRANCH", StringType(), True),
                StructField("EMPLOYEE_OFFICE", StringType(), True),
                StructField("EMPLOYEE_PHONE", StringType(), True)
        ])
        load_csv(schema, con_file_name, 'hr')

    con_file_name = 'WatchHistory.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("W_C_ID", IntegerType(), False),
                StructField("W_S_SYMB", StringType(), True),
                StructField("W_DTS", TimestampType(), True),
                StructField("W_ACTION", StringType(), True)
        ])
        load_csv(schema, con_file_name, 'watch_history')

    con_file_name = 'Trade.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("T_ID", IntegerType(), False),
                StructField("T_DTS", TimestampType(), False),
                StructField("T_ST_ID", StringType(), False),
                StructField("T_TT_ID", StringType(), False),
                StructField("T_IS_CASH", BooleanType(), False),
                StructField("T_S_SYMB", StringType(), False),
                StructField("T_QTY", FloatType(),False),
                StructField("T_BID_PRICE", FloatType(), False),
                StructField("T_CA_ID", IntegerType(), False),
                StructField("T_EXEC_NAME", StringType(), False),
                StructField("T_TRADE_PRICE", FloatType(), True),
                StructField("T_CHRG", FloatType(), True),
                StructField("T_COMM", FloatType(), True),
                StructField("T_TAX", FloatType(), True),
        ])
        load_csv(schema, con_file_name, 'trade')

    con_file_name = 'TradeHistory.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("TH_T_ID", IntegerType(), False),
                StructField("TH_DTS", TimestampType(), False),
                StructField("TH_ST_ID", StringType(), False),
        ])
        load_csv(schema, con_file_name, 'trade_history')

    con_file_name = 'StatusType.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("ST_ID", StringType(), False),
                StructField("ST_NAME", StringType(), False),
        ])
        load_csv(schema, con_file_name, 'status_type')

    con_file_name = 'TradeType.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("TT_ID", StringType(), False),
                StructField("TT_NAME", StringType(), False),
                StructField("TT_IS_SELL", BooleanType(), False),
                StructField("TT_IS_MARKET", BooleanType(), False),
        ])
        load_csv(schema, con_file_name, 'trade_type')

    con_file_name = 'HoldingHistory.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("HH_H_T_ID", IntegerType(), False),
                StructField("HH_T_ID", IntegerType(), False),
                StructField("HH_BEFORE_QTY", FloatType(), False),
                StructField("HH_AFTER_QTY", FloatType(), False),
        ])
        load_csv(schema, con_file_name, 'holding_history')

    con_file_name = 'CashTransaction.txt'
    if file_name in ['all', con_file_name]:
        schema = StructType([
                StructField("CT_CA_ID", IntegerType(), False),
                StructField("CT_DTS", TimestampType(), False),
                StructField("CT_AMT", FloatType(), False),
                StructField("CT_NAME", StringType(), False),
        ])
        load_csv(schema, con_file_name, 'cash_transaction')

    con_file_name = 'FINWIRE'
    if file_name in ['all', con_file_name]:
        # These are fixed-width fields, so read the entire line in as "line"
        schema = StructType([
                StructField("line", StringType(), False),
        ])

        stage_path = get_stage_path(stage, con_file_name)
        upload_files(con_file_name, stage_path)

        # generic dataframe for all record types
        # create a temporary table
        df = (
            session
            .read
            .schema(schema)
            .option('field_delimiter', '|')
            .csv(stage_path)
            .with_column('rec_type', substring(col("line"), lit(16), lit(3)))
            .with_column('pts', to_timestamp(substring(col("line"), lit(0), lit(15)), lit("yyyymmdd-hhmiss")))
            .write.mode("overwrite").save_as_table("finwire", table_type="temporary")
        )

        # CMP record types
        df = (
            session
            .table('finwire')
            .where(col('rec_type') == 'CMP')
            .with_column('company_name', substr(col('line'), lit(19), lit(60)))
            .withColumn('cik', substring(col("line"), lit(79), lit(10)))
            .withColumn('status', substring(col("line"), lit(89), lit(4)))
            .withColumn('industry_id', substring(col("line"), lit(93), lit(2)))
            .withColumn('sp_rating', substring(col("line"), lit(95), lit(4)))
            .withColumn(
                'founding_date',
                try_cast(
                    trim(
                        substring(
                            col("line"), lit(99), lit(8)
                        )
                    ),
                    'DATE'
                )
            )
            .withColumn('address_line1', substring(col("line"), lit(107), lit(80)))
            .withColumn('address_line2', substring(col("line"), lit(187), lit(80)))
            .withColumn('postal_code', substring(col("line"), lit(267), lit(12)))
            .withColumn('city', substring(col("line"), lit(279), lit(25)))
            .withColumn('state_province', substring(col("line"), lit(304), lit(20)))
            .withColumn('country', substring(col("line"), lit(324), lit(24)))
            .withColumn('ceo_name', substring(col("line"), lit(348), lit(46)))
            .withColumn('description', substring(col("line"), lit(394), lit(150)))
            .drop(col("line"), col("rec_type"))
        )

        save_df(df, 'cmp')

        # SEC record types
        df = (
            session
            .table('finwire')
            .where(col('rec_type') == 'SEC')
            .withColumn('symbol', substring(col("line"), lit(19), lit(15)))
            .withColumn('issue_type', substring(col("line"), lit(34), lit(6)))
            .withColumn('status', substring(col("line"), lit(40), lit(4)))
            .withColumn('name', substring(col("line"), lit(44), lit(70)))
            .withColumn('ex_id', substring(col("line"), lit(114), lit(6)))
            .withColumn('sh_out', substring(col("line"), lit(120), lit(13)))
            .withColumn('first_trade_date', substring(col("line"), lit(133), lit(8)))
            .withColumn('first_exchange_date', substring(col("line"), lit(141), lit(8)))
            .withColumn('dividend', substring(col("line"), lit(149), lit(12)))
            .withColumn('co_name_or_cik', substring(col("line"), lit(161), lit(60)))
            .drop(col("line"), col("rec_type"))
        )

        save_df(df, 'sec')

        # FIN record types
        df = (
            session
            .table('finwire')
            .where(col('rec_type') == 'FIN')
            .withColumn('year', substring(col("line"), lit(19), lit(4)))
            .withColumn('quarter', substring(col("line"), lit(23), lit(1)))
            .withColumn('quarter_start_date', substring(col("line"), lit(24), lit(8)))
            .withColumn('posting_date', substring(col("line"), lit(32), lit(8)))
            .withColumn('revenue', substring(col("line"), lit(40), lit(17)))
            .withColumn('earnings', substring(col("line"), lit(57), lit(17)))
            .withColumn('eps', substring(col("line"), lit(74), lit(12)))
            .withColumn('diluted_eps', substring(col("line"), lit(86), lit(12)))
            .withColumn('margin', substring(col("line"), lit(98), lit(12)))
            .withColumn('inventory', substring(col("line"), lit(110), lit(17)))
            .withColumn('assets', substring(col("line"), lit(127), lit(17)))
            .withColumn('liabilities', substring(col("line"), lit(144), lit(17)))
            .withColumn('sh_out', substring(col("line"), lit(161), lit(13)))
            .withColumn('diluted_sh_out', substring(col("line"), lit(174), lit(13)))
            .withColumn('co_name_or_cik', substring(col("line"), lit(187), lit(60)))
            .drop(col("line"), col("rec_type"))
        )
            

        save_df(df, 'fin')

if __name__ == "__main__":
    app()
