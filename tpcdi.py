import sys, typer, json, re, logging
from snowflake.snowpark import Session, DataFrame
from typing_extensions import Annotated
from pathlib import Path
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import col, call_function, lit

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
    output_directory: Annotated[str, typer.Option(help='The output directory from the TPC-di DIGen.jar execution.')],
    file_name: Annotated[str, typer.Option(help="The TPC-DI file name to upload and process. Pass value 'FINWIRE' to process all of the financial wire files.")] = 'all',
    stage: Annotated[str, typer.Option(help="The stage name to upload to, without specifying '@'.")] = 'upload',
    batch: Annotated[int, typer.Option(help="The TPC-DI batch number to process.")] = 1,
    overwrite: Annotated[bool, typer.Option(help="Overwrite the file even if it exists?")] = False,
    skip_upload: Annotated[bool, typer.Option(help="Skip uploading the files?")] = False,
    show: Annotated[bool, typer.Option(help="Show the DataFrame instead of saving it as a table?")] = False,
):
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

    # method for creating a table from a CSV
    def load_csv(
            schema: StructType,
            file_name: str,
            table_name: str,
    ):
        delimiter="|"
        stage_path=f"@{stage}/Batch{batch}/{file_name}"
            
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
        
        stage_path=f"@{stage}/Batch{batch}/{file_name}"
        df = session.read.schema(schema).option("field_delimiter",delimiter).csv(stage_path)
        save_df(df, table_name)
    
    # process the Date.txt file
    if file_name in ['all','Date.txt']:
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
        load_csv(schema,'Date.txt','date')

    # Process the DailyMarket.txt file
    if file_name in ['all','DailyMarket.txt']:
        schema = StructType([
                StructField("DM_DATE", DateType(), False),
                StructField("DM_S_SYMB", StringType(), False),
                StructField("DM_CLOSE", FloatType(), False),
                StructField("DM_HIGH", FloatType(), False),
                StructField("DM_LOW", FloatType(), False),
                StructField("DM_VOL", FloatType(), False),
        ])
        load_csv(schema,'DailyMarket.txt','daily_market')

    # Process the Industry.txt file
    if file_name in ['all','Industry.txt']:
        schema = StructType([
                StructField("IN_ID", StringType(), False),
                StructField("IN_NAME", StringType(), False),
                StructField("IN_SC_ID", StringType(), False),
        ])
        load_csv(schema,'Industry.txt','industry')

    # Process the Prospect.csv file
    if file_name in ['all','Prospect.csv']:
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
        load_csv(schema,'Prospect.csv','prospect')

    # Process the CustomerMgmt.csv file
    if file_name in ['all','CustomerMgmt.csv']:
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
        # customer DataFrame logic
        file_name = 'CustomerMgmt.csv'
        stage_path = f"@{stage}/Batch{batch}/{file_name}"
        df = session.read.schema(schema).option("field_delimiter", '|').option("skip_header",1).csv(stage_path).with_column('action_ts', call_function('to_timestamp', col('action_ts'), lit('yyyy-mm-ddThh:mi:ss')))
        save_df(df, 'customer_mgmt')

    # Process the TaxRate.txt file
    if file_name in ['all','TaxRate.txt']:
        schema = StructType([
                StructField("TX_ID", StringType(), False),
                StructField("TX_NAME", StringType(), True),
                StructField("TX_RATE", FloatType(), True),
        ])
        load_csv(schema,'TaxRate.txt','tax_rate')

    # Process the HR.csv file
    if file_name in ['all','HR.csv']:
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
        load_csv(schema,'HR.csv','hr')

    # Process the WatchHistory.txt file
    if file_name in ['all','WatchHistory.txt']:
        schema = StructType([
                StructField("W_C_ID", IntegerType(), False),
                StructField("W_S_SYMB", StringType(), True),
                StructField("W_DTS", TimestampType(), True),
                StructField("W_ACTION", StringType(), True)
        ])
        load_csv(schema,'WatchHistory.txt','watch_history')

    # Process the Trade.txt file
    if file_name in ['all','Trade.txt']:
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
                StructField("T_TAX", FloatType(), True)
        ])
        load_csv(schema,'Trade.txt','trade')

    # Process the TradeHistory.txt file
    if file_name in ['all','TradeHistory.txt']:
        schema = StructType([
                StructField("TH_T_ID", IntegerType(), False),
                StructField("TH_DTS", TimestampType(), False),
                StructField("TH_ST_ID", StringType(), False)
        ])
        load_csv(schema,'TradeHistory.txt','trade_history')


if __name__ == "__main__":
    app()
