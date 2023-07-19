import sys, typer, json, re, logging
from snowflake.snowpark import Session, DataFrame
from typing_extensions import Annotated
from pathlib import Path
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *

# Read the credentials.json file
with open("credentials.json") as jsonfile:
    credentials_dict = json.load(jsonfile)

session = Session.builder.configs(credentials_dict).create()
print(f"Session: {session}")

app = typer.Typer(help="A utility for loading TPC-DI generated files to Snowflake.")

@app.command()
def recreate_stage(
        name: Annotated[str, typer.Option(help="Name of the stage to recreate.")],
):
    session.sql(f"create or replace stage {name} directory = (enable = true)").collect()
    print(f"Stage {name} recreated.")

@app.command()
def drop_stage(
        name: Annotated[str, typer.Option(help="Name of the stage to recreate.")],
):
    session.sql(f"drop stage {name}").collect()
    print(f"Stage {name} dropped.")

@app.command()
def process_files(
    output_directory: Annotated[str, typer.Option(help='The output directory from the TPC-DI DIGen.jar execution.')],
    file_name: Annotated[str, typer.Option(help="The TPC-DI file name to upload and process. Pass value 'FINWIRE' to process all of the financial wire files.")] = 'all',
    stage: Annotated[str, typer.Option(help="The stage name to upload to, without specifying '@'.")] = 'upload',
    batch: Annotated[int, typer.Option(help="The TPC-DI batch number to process.")] = 1,
    overwrite: Annotated[bool, typer.Option(help="Overwrite the file even if it exists?")] = False,
    skip_upload: Annotated[bool, typer.Option(help="Skip uploading the files?")] = False,
    show: Annotated[bool, typer.Option(help="Show the DataFrame instead of saving it as a table?")] = False,
):
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
            df.write.mode("overwrite").save_as_table(table_name)
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
                put_result = session.file.put(str(file), stage_path, overwrite=overwrite, parallel=4, auto_compress=True )
                for result in put_result:
                    print(f"File {result.source}: {result.status}")
        
        # return the delimiter
        return delimiter
        
        

    # method for creating a table from a CSV
    def load_csv(
            schema: StructType,
            file_name: str,
            table_name: str,
    ):
        stage_path = get_stage_path(stage, file_name)
        delimiter=upload_files(file_name, stage_path)
        
        df = session.read.schema(schema).option("field_delimiter", delimiter).csv(stage_path)
        save_df(df, table_name)
    
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
        schema = StructType([
                StructField("ACTION_TYPE", StringType(), False),
                StructField("ACTION_TS", StringType(), True),
                StructField("C_ID", IntegerType(), True),
                StructField("C_TAX_ID", StringType(), True),
                StructField("C_GNDR", StringType(), True),
                StructField("C_TIER", IntegerType(), True),
                StructField("C_DOB", DateType(), True),
                StructField("C_L_NAME", StringType(), True),
                StructField("C_F_NAME", StringType(), True),
                StructField("C_M_NAME", StringType(), True),
                StructField("C_ADLINE1", StringType(), True),
                StructField("C_ADLINE2", StringType(), True),
                StructField("C_ZIPCODE", StringType(), True),
                StructField("C_CITY", StringType(), True),
                StructField("C_STATE_PROV", StringType(), True),
                StructField("C_CTRY", StringType(), True),
                StructField("C_PRIM_EMAIL", StringType(), True),
                StructField("C_ALT_EMAIL", StringType(), True),
                StructField("C_PHONE_1", StringType(), True),
                StructField("C_PHONE_2", StringType(), True),
                StructField("C_PHONE_3", StringType(), True),
                StructField("C_LCL_TX_ID", StringType(), True),
                StructField("C_NAT_TX_ID", StringType(), True),
                StructField("CA_ID", IntegerType(), True),
                StructField("CA_TAX_ST", StringType(), True),
                StructField("CA_B_ID", IntegerType(), True),
                StructField("CA_C_ID", IntegerType(), True),
                StructField("CA_NAME", StringType(), True)
        ])

        # custom DataFrame logic for dealing with XML
        # df = session.read.schema(schema).option("field_delimiter", '|') \
        #     .option("skip_header",1) \
        #     .csv(get_stage_path(stage, con_file_name)) \
        #     .with_column('action_ts', to_timestamp(col('action_ts'), lit('yyyy-mm-ddThh:mi:ss')))
        df = session.read \
            .option('STRIP_OUTER_ELEMENT', True) \
            .xml(get_stage_path(stage, con_file_name)) \
            .with_column('action_type', get(col('$1'), lit('@ActionType')).cast('STRING')) \
            .with_column('action_ts', get(col('$1'), lit('@ActionTS')).cast('TIMESTAMP')) \
            .with_column('customer', xmlget(col('$1'), lit('Customer'), 0)) \
            .with_column('name', xmlget(col('customer'), lit('Name'), 0)) \
            .with_column('c_dob', get(col('customer'), lit('@C_DOB')).cast('DATE')) \
            
        
        save_df(df, 'customer_mgmt_test')

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

        # CMP record types
        df = session.read.schema(schema) \
            .option('field_delimiter', '|') \
            .csv(stage_path) \
            .withColumn('pts', substring(col("line"), lit(0), lit(15))) \
            .with_column('rec_type', substring(col("line"), lit(16), lit(3))) \
            .where(col('rec_type') == 'CMP') \
            .with_column('company_name', substr(col('line'), lit(19), lit(60))) \
            .withColumn('cik', substring(col("line"), lit(79), lit(10))) \
            .withColumn('status', substring(col("line"), lit(89), lit(4))) \
            .withColumn('industry_id', substring(col("line"), lit(93), lit(2))) \
            .withColumn('sp_rating', substring(col("line"), lit(95), lit(4))) \
            .withColumn('founding_date', substring(col("line"), lit(99), lit(8))) \
            .withColumn('address_line1', substring(col("line"), lit(107), lit(80))) \
            .withColumn('address_line2', substring(col("line"), lit(187), lit(80))) \
            .withColumn('postal_code', substring(col("line"), lit(267), lit(12))) \
            .withColumn('city', substring(col("line"), lit(279), lit(25))) \
            .withColumn('state_province', substring(col("line"), lit(304), lit(20))) \
            .withColumn('country', substring(col("line"), lit(324), lit(24))) \
            .withColumn('ceo_name', substring(col("line"), lit(348), lit(46))) \
            .withColumn('description', substring(col("line"), lit(394), lit(150))) \
            .with_column('pts', to_timestamp(col('pts'), lit("yyyymmdd-hhmiss"))) \
            .drop(col('line'))

        save_df(df, 'cmp')

        # SEC record types
        df = session.read.schema(schema) \
            .option('field_delimiter', '|') \
            .csv(stage_path) \
            .withColumn('pts', substring(col("line"), lit(0), lit(15))) \
            .with_column('rec_type', substring(col("line"), lit(16), lit(3))) \
            .where(col('rec_type') == 'SEC') \
            .withColumn('symbol', substring(col("line"), lit(19), lit(15))) \
            .withColumn('issue_type', substring(col("line"), lit(34), lit(6))) \
            .withColumn('status', substring(col("line"), lit(40), lit(4))) \
            .withColumn('name', substring(col("line"), lit(44), lit(70))) \
            .withColumn('ex_id', substring(col("line"), lit(114), lit(6))) \
            .withColumn('sh_out', substring(col("line"), lit(120), lit(13))) \
            .withColumn('first_trade_date', substring(col("line"), lit(133), lit(8))) \
            .withColumn('first_exchange_date', substring(col("line"), lit(141), lit(8))) \
            .withColumn('dividend', substring(col("line"), lit(149), lit(12))) \
            .withColumn('co_name_or_cik', substring(col("line"), lit(161), lit(60))) \
            .withColumn("pts", to_timestamp(col("pts"), lit("yyyymmdd-hhmiss"))) \
            .drop(col('line'))

        save_df(df, 'sec')

        # FIN record types
        df = session.read.schema(schema) \
            .option('field_delimiter', '|') \
            .csv(stage_path) \
            .withColumn('pts', substring(col("line"), lit(0), lit(15))) \
            .with_column('rec_type', substring(col("line"), lit(16), lit(3))) \
            .where(col('rec_type') == 'FIN') \
            .withColumn('year', substring(col("line"), lit(19), lit(4))) \
            .withColumn('quarter', substring(col("line"), lit(23), lit(1))) \
            .withColumn('quarter_start_date', substring(col("line"), lit(24), lit(8))) \
            .withColumn('posting_date', substring(col("line"), lit(32), lit(8))) \
            .withColumn('revenue', substring(col("line"), lit(40), lit(17))) \
            .withColumn('earnings', substring(col("line"), lit(57), lit(17))) \
            .withColumn('eps', substring(col("line"), lit(74), lit(12))) \
            .withColumn('diluted_eps', substring(col("line"), lit(86), lit(12))) \
            .withColumn('margin', substring(col("line"), lit(98), lit(12))) \
            .withColumn('inventory', substring(col("line"), lit(110), lit(17))) \
            .withColumn('assets', substring(col("line"), lit(127), lit(17))) \
            .withColumn('liabilities', substring(col("line"), lit(144), lit(17))) \
            .withColumn('sh_out', substring(col("line"), lit(161), lit(13))) \
            .withColumn('diluted_sh_out', substring(col("line"), lit(174), lit(13))) \
            .withColumn('co_name_or_cik', substring(col("line"), lit(187), lit(60))) \
            .withColumn("pts", to_timestamp(col("pts"), lit("yyyymmdd-hhmiss"))) \
            .drop(col("line"))

        save_df(df, 'fin')

if __name__ == "__main__":
    app()
