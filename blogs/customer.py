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

