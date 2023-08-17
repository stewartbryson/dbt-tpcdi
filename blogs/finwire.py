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
    .glob("FINWIRE??????")
)
stage_path = "@tpcdi/Batch1/FINWIRE"

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

# These are fixed-width fields, so read the entire line in as "line"
schema = StructType([
        StructField("line", StringType(), False),
])

# generic dataframe for all record types
# create a temporary table
# The delimiter '|' seems safer
df = (
    session
    .read
    .schema(schema)
    .option('field_delimiter', '|')
    .csv(stage_path)
    .with_column(
        'pts', 
        to_timestamp(
            substring(col("line"), lit(0), lit(15)), 
            lit("yyyymmdd-hhmiss")
        )
    )
    .with_column(
        'rec_type', 
        substring(col("line"), lit(16), lit(3))
    )
    .write
    .mode("overwrite")
    .save_as_table("finwire", table_type="temporary")
)

# let's see the table
df = (
    session 
    .table('finwire') 
    .show()
)

# SEC record types
table_name = 'sec'
df = (
    session
    .table('finwire')
    .where(col('rec_type') == 'SEC')
    .withColumn(
        'symbol', 
        substring(col("line"), lit(19), lit(15))
    )
    .withColumn(
        'issue_type', 
        substring(col("line"), lit(34), lit(6))
    )
    .withColumn(
        'status', 
        substring(col("line"), lit(40), lit(4))
    )
    .withColumn(
        'name', 
        substring(col("line"), lit(44), lit(70))
    )
    .withColumn(
        'ex_id', 
        substring(col("line"), lit(114), lit(6))
    )
    .withColumn(
        'sh_out', 
        substring(col("line"), lit(120), lit(13))
    )
    .withColumn(
        'first_trade_date', 
        substring(col("line"), lit(133), lit(8))
    )
    .withColumn(
        'first_exchange_date', 
        substring(col("line"), lit(141), lit(8))
    )
    .withColumn(
        'dividend', 
        substring(col("line"), lit(149), lit(12))
    )
    .withColumn(
        'co_name_or_cik', 
        substring(col("line"), lit(161), lit(60))
    )
    .drop(col("line"), col("rec_type"))
    .write
    .mode("overwrite")
    .save_as_table(table_name)
)

print(f"{table_name.upper()} table created.")
