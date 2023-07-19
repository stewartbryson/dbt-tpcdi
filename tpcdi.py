import sys, typer, json, re, logging
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from typing_extensions import Annotated
from pathlib import Path
from snowflake.snowpark.types import *

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
                StructField("HOLIDAY_FLAG", BooleanType(), False)
        ])

        load_csv(schema,'Date.txt','date')

if __name__ == "__main__":
    app()
