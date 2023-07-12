#!/opt/homebrew/bin/groovy

@Grab('com.snowflake:snowpark:1.8.0')
@Grab('commons-cli:commons-cli:1.5.0')

import com.snowflake.snowpark_java.types.*
import com.snowflake.snowpark_java.*
import groovy.cli.commons.CliBuilder
import groovy.io.FileType
import groovy.transform.Field

def fileType = 'all'
def stage = 'upload'
def batch = '1'
def directory = '.'
def cli = new CliBuilder(header: 'TPC-DI Data Loader', usage:'tpcdi-loader.groovy', width: -1)
cli.f(longOpt: 'filetype', "The filetype to load. [defaults to '${fileType}']", args: 1, defaultValue: fileType)
cli.s(longOpt: 'stage', "The stage to user. [defaults to '${stage}']", args: 1, defaultValue: stage)
cli.b(longOpt: 'batch', "The batch to load. [defaults to '${batch}']", args: 1, defaultValue: batch)
cli.r(longOpt: 'reset', "Delete tables and recreate the stage.")
cli.o(longOpt: 'overwrite', "Overwrite files when uploading to stage.")
cli.p(longOpt: 'print', "Print dataframes instead of saving them.")
cli.d(longOpt: 'directory', "Local output directory from the DIGen.jar file generation. Defaults to current directory.", args: 1, defaultValue: directory)
cli.n(longOpt: 'noupload', "Skip uploading of files.")

cliOptions = cli.parse(args)

dir = new File(cliOptions.directory)

session = Session.builder().configFile("credentials.properties").create()

options = [
        AUTO_COMPRESS: 'TRUE',
        PARALLEL     : '4',
        OVERWRITE    : (cliOptions.overwrite ? 'TRUE' : 'FALSE')
]

// upload files
def uploadFiles(String pattern) {
        if (!cliOptions.noupload) {
                // load all FINWIRE files
                new File(cliOptions.directory).eachFileRecurse (FileType.FILES) { file ->
                        if (file.parentFile.name.contains("Batch${cliOptions.batch}") && (!file.name.contains("audit")) && (file.name.contains(pattern)) && (file.name != 'BatchDate.txt')) {
                                PutResult[] pr = session.file().put(file.path, "@${cliOptions.stage}/Batch${cliOptions.batch}/${file.name}", options)
                                pr.each {
                                        println "File ${it.sourceFileName}: ${it.status}"
                                }
                        }
                }
        }
}

def saveDf(DataFrame df, String tableName) {
        if (cliOptions.print) {
                df.show()
        } else {
                df
                .write()
                .mode(SaveMode.Overwrite)
                .saveAsTable(tableName)

                println "${tableName.toUpperCase()} table created."
        }
}

def loadCsv(StructType structType, String fileName, String tableName) {

        def delimiter = ((fileName.tokenize('.')[1] == 'txt') ? '|' : ','  )
        
        stagePath = "@${cliOptions.stage}/Batch${cliOptions.batch}/${fileName}"

        uploadFiles(fileName)

        def dfCsv = session
                .read()
                .schema(structType)
                .option("field_delimiter", delimiter)
                .csv(stagePath)
        
        saveDf(dfCsv, tableName)
}

if (cliOptions.reset) {
        session.jdbcConnection().createStatement().execute("create or replace stage ${cliOptions.stage} directory = (enable = true)")
        session.jdbcConnection().createStatement().execute("drop table CMP")
        session.jdbcConnection().createStatement().execute("drop table SEC")
        session.jdbcConnection().createStatement().execute("drop table FIN")

        println "Tables deleted and stage '${cliOptions.stage}' reset."
}

session.jdbcConnection().createStatement().execute("create stage if not exists ${cliOptions.stage} directory = (enable = true)")

if (['all','finwire'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType fixed = StructType.create(
                new StructField("line", DataTypes.StringType, false)
        )

        // reusable DataFrameReader
        def dfr = session
                .read()
                .schema(fixed)
                .option("field_delimiter", "|")

        def stagePath
        def fileName

        // load all FINWIRE files
        uploadFiles('FINWIRE')

        stagePath = "@${cliOptions.stage}/Batch${batch}/FINWIRE"

        // create the CMP table
        def dfc = dfr.csv(stagePath)
                .withColumn('pts', Functions.substring(Functions.col("line"), Functions.lit(0), Functions.lit(15)))
                .withColumn('rec_type', Functions.substring(Functions.col("line"), Functions.lit(16), Functions.lit(3)))
                .where(Functions.col('rec_type').equal_to(Functions.lit('CMP')))
                .withColumn('company_name', Functions.substring(Functions.col("line"), Functions.lit(19), Functions.lit(60)))
                .withColumn('cik', Functions.substring(Functions.col("line"), Functions.lit(79), Functions.lit(10)))
                .withColumn('status', Functions.substring(Functions.col("line"), Functions.lit(89), Functions.lit(4)))
                .withColumn('industry_id', Functions.substring(Functions.col("line"), Functions.lit(93), Functions.lit(2)))
                .withColumn('sp_rating', Functions.substring(Functions.col("line"), Functions.lit(95), Functions.lit(4)))
                .withColumn('founding_date', Functions.substring(Functions.col("line"), Functions.lit(99), Functions.lit(8)))
                .withColumn('address_line1', Functions.substring(Functions.col("line"), Functions.lit(107), Functions.lit(80)))
                .withColumn('address_line2', Functions.substring(Functions.col("line"), Functions.lit(187), Functions.lit(80)))
                .withColumn('postal_code', Functions.substring(Functions.col("line"), Functions.lit(267), Functions.lit(12)))
                .withColumn('city', Functions.substring(Functions.col("line"), Functions.lit(279), Functions.lit(25)))
                .withColumn('state_province', Functions.substring(Functions.col("line"), Functions.lit(304), Functions.lit(20)))
                .withColumn('country', Functions.substring(Functions.col("line"), Functions.lit(324), Functions.lit(24)))
                .withColumn('ceo_name', Functions.substring(Functions.col("line"), Functions.lit(348), Functions.lit(46)))
                .withColumn('description', Functions.substring(Functions.col("line"), Functions.lit(394), Functions.lit(150)))
                .withColumn("pts", Functions.callUDF("to_timestamp", Functions.col("pts"), Functions.lit("yyyymmdd-hhmiss")))
                .drop(Functions.col("line"))

        saveDf(dfc,'cmp')

        // create the SEC table
        def dfs = dfr.csv(stagePath)
                .withColumn('pts', Functions.substring(Functions.col("line"), Functions.lit(0), Functions.lit(15)))
                .withColumn('rec_type', Functions.substring(Functions.col("line"), Functions.lit(16), Functions.lit(3)))
                .where(Functions.col('rec_type').equal_to(Functions.lit('SEC')))
                .withColumn('symbol', Functions.substring(Functions.col("line"), Functions.lit(19), Functions.lit(15)))
                .withColumn('issue_type', Functions.substring(Functions.col("line"), Functions.lit(34), Functions.lit(6)))
                .withColumn('status', Functions.substring(Functions.col("line"), Functions.lit(40), Functions.lit(4)))
                .withColumn('name', Functions.substring(Functions.col("line"), Functions.lit(44), Functions.lit(70)))
                .withColumn('ex_id', Functions.substring(Functions.col("line"), Functions.lit(114), Functions.lit(6)))
                .withColumn('sh_out', Functions.substring(Functions.col("line"), Functions.lit(120), Functions.lit(13)))
                .withColumn('first_trade_date', Functions.substring(Functions.col("line"), Functions.lit(133), Functions.lit(8)))
                .withColumn('first_exchange_date', Functions.substring(Functions.col("line"), Functions.lit(141), Functions.lit(8)))
                .withColumn('dividend', Functions.substring(Functions.col("line"), Functions.lit(149), Functions.lit(12)))
                .withColumn('co_name_or_cik', Functions.substring(Functions.col("line"), Functions.lit(161), Functions.lit(60)))
                .withColumn("pts", Functions.callUDF("to_timestamp", Functions.col("pts"), Functions.lit("yyyymmdd-hhmiss")))
                .drop(Functions.col("line"))

        saveDf(dfs,'sec')

        // create the FIN table
        def dff = dfr.csv(stagePath)
                .withColumn('pts', Functions.substring(Functions.col("line"), Functions.lit(0), Functions.lit(15)))
                .withColumn('rec_type', Functions.substring(Functions.col("line"), Functions.lit(16), Functions.lit(3)))
                .where(Functions.col('rec_type').equal_to(Functions.lit('FIN')))
                .withColumn('year', Functions.substring(Functions.col("line"), Functions.lit(19), Functions.lit(4)))
                .withColumn('quarter', Functions.substring(Functions.col("line"), Functions.lit(23), Functions.lit(1)))
                .withColumn('quarter_start_date', Functions.substring(Functions.col("line"), Functions.lit(24), Functions.lit(8)))
                .withColumn('posting_date', Functions.substring(Functions.col("line"), Functions.lit(32), Functions.lit(8)))
                .withColumn('revenue', Functions.substring(Functions.col("line"), Functions.lit(40), Functions.lit(17)))
                .withColumn('earnings', Functions.substring(Functions.col("line"), Functions.lit(57), Functions.lit(17)))
                .withColumn('eps', Functions.substring(Functions.col("line"), Functions.lit(74), Functions.lit(12)))
                .withColumn('diluted_eps', Functions.substring(Functions.col("line"), Functions.lit(86), Functions.lit(12)))
                .withColumn('margin', Functions.substring(Functions.col("line"), Functions.lit(98), Functions.lit(12)))
                .withColumn('inventory', Functions.substring(Functions.col("line"), Functions.lit(110), Functions.lit(17)))
                .withColumn('assets', Functions.substring(Functions.col("line"), Functions.lit(127), Functions.lit(17)))
                .withColumn('liabilities', Functions.substring(Functions.col("line"), Functions.lit(144), Functions.lit(17)))
                .withColumn('sh_out', Functions.substring(Functions.col("line"), Functions.lit(161), Functions.lit(13)))
                .withColumn('diluted_sh_out', Functions.substring(Functions.col("line"), Functions.lit(174), Functions.lit(13)))
                .withColumn('co_name_or_cik', Functions.substring(Functions.col("line"), Functions.lit(187), Functions.lit(60)))
                .withColumn("pts", Functions.callUDF("to_timestamp", Functions.col("pts"), Functions.lit("yyyymmdd-hhmiss")))
                .drop(Functions.col("line"))

        saveDf(dff,'fin')

}

if (['all','industry'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        StructType industry = StructType.create(
                new StructField("IN_ID", DataTypes.StringType, false),
                new StructField("IN_NAME", DataTypes.StringType, false),
                new StructField("IN_SC_ID", DataTypes.StringType, false)
        )

        loadCsv(industry, 'Industry.txt', 'industry')
}

if (['all','dailymarket'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        StructType dailyMarket = StructType.create(
                new StructField("DM_DATE", DataTypes.DateType, false),
                new StructField("DM_S_SYMB", DataTypes.StringType, false),
                new StructField("DM_CLOSE", DataTypes.FloatType, false),
                new StructField("DM_HIGH", DataTypes.FloatType, false),
                new StructField("DM_LOW", DataTypes.FloatType, false),
                new StructField("DM_VOL", DataTypes.FloatType, false)
        )

        loadCsv(dailyMarket, 'DailyMarket.txt', 'daily_market')
}

if (['all','date'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType date = StructType.create(
                new StructField("SK_DATE_ID", DataTypes.IntegerType, false),
                new StructField("DATE_VALUE", DataTypes.DateType, false),
                new StructField("DATE_DESC", DataTypes.StringType, false),
                new StructField("CALENDAR_YEAR_ID", DataTypes.IntegerType, false),
                new StructField("CALENDAR_YEAR_DESC", DataTypes.StringType, false),
                new StructField("CALENDAR_QTR_ID", DataTypes.IntegerType, false),
                new StructField("CALENDAR_QTR_DESC", DataTypes.StringType, false),
                new StructField("CALENDAR_MONTH_ID", DataTypes.IntegerType, false),
                new StructField("CALENDAR_MONTH_DESC", DataTypes.StringType, false),
                new StructField("CALENDAR_WEEK_ID", DataTypes.IntegerType, false),
                new StructField("CALENDAR_WEEK_DESC", DataTypes.StringType, false),
                new StructField("DAY_OF_WEEK_NUM", DataTypes.IntegerType, false),
                new StructField("DAY_OF_WEEK_DESC", DataTypes.StringType, false),
                new StructField("FISCAL_YEAR_ID", DataTypes.IntegerType, false),
                new StructField("FISCAL_YEAR_DESC", DataTypes.StringType, false),
                new StructField("FISCAL_QTR_ID", DataTypes.IntegerType, false),
                new StructField("FISCAL_QTR_DESC", DataTypes.StringType, false),
                new StructField("HOLIDAY_FLAG", DataTypes.BooleanType, false)
        )

        loadCsv(date,'Date.txt','date')
}

if (['all','prospect'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType prospect = StructType.create(
                new StructField("AGENCY_ID", DataTypes.StringType, false),
                new StructField("LAST_NAME", DataTypes.StringType, true),
                new StructField("FIRST_NAME", DataTypes.StringType, true),
                new StructField("MIDDLE_INITIAL", DataTypes.StringType, true),
                new StructField("GENDER", DataTypes.StringType, true),
                new StructField("ADDRESS_LINE1", DataTypes.StringType, true),
                new StructField("ADDRESS_LINE2", DataTypes.StringType, true),
                new StructField("POSTAL_CODE", DataTypes.StringType, true),
                new StructField("CITY", DataTypes.StringType, true),
                new StructField("STATE", DataTypes.StringType, true),
                new StructField("COUNTRY", DataTypes.StringType, true),
                new StructField("PHONE", DataTypes.StringType, true),
                new StructField("INCOME", DataTypes.IntegerType, true),
                new StructField("NUMBER_CARS", DataTypes.IntegerType, true),
                new StructField("NUMBER_CHILDREN", DataTypes.IntegerType, true),
                new StructField("MARITAL_STATUS", DataTypes.StringType, true),
                new StructField("AGE", DataTypes.IntegerType, true),
                new StructField("CREDIT_RATING", DataTypes.IntegerType, true),
                new StructField("OWN_OR_RENT_FLAG", DataTypes.StringType, true),
                new StructField("EMPLOYER", DataTypes.StringType, true),
                new StructField("NUMBER_CREDIT_CARDS", DataTypes.IntegerType, true),
                new StructField("NET_WORTH", DataTypes.IntegerType, true),
        )

        loadCsv(prospect, 'Prospect.csv', 'prospect')
}

if (['all','customer'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType customer = StructType.create(
                new StructField("ACTION_TYPE", DataTypes.StringType, false),
                new StructField("ACTION_TS", DataTypes.StringType, true),
                new StructField("C_ID", DataTypes.IntegerType, true),
                new StructField("C_TAX_ID", DataTypes.StringType, true),
                new StructField("C_GNDR", DataTypes.StringType, true),
                new StructField("C_TIER", DataTypes.IntegerType, true),
                new StructField("C_DOB", DataTypes.DateType, true),
                new StructField("C_L_NAME", DataTypes.StringType, true),
                new StructField("C_F_NAME", DataTypes.StringType, true),
                new StructField("C_M_NAME", DataTypes.StringType, true),
                new StructField("C_ADLINE1", DataTypes.StringType, true),
                new StructField("C_ADLINE2", DataTypes.StringType, true),
                new StructField("C_ZIPCODE", DataTypes.StringType, true),
                new StructField("C_CITY", DataTypes.StringType, true),
                new StructField("C_STATE_PROV", DataTypes.StringType, true),
                new StructField("C_CTRY", DataTypes.StringType, true),
                new StructField("C_PRIM_EMAIL", DataTypes.StringType, true),
                new StructField("C_ALT_EMAIL", DataTypes.StringType, true),
                new StructField("C_PHONE_1", DataTypes.StringType, true),
                new StructField("C_PHONE_2", DataTypes.StringType, true),
                new StructField("C_PHONE_3", DataTypes.StringType, true),
                new StructField("C_LCL_TX_ID", DataTypes.StringType, true),
                new StructField("C_NAT_TX_ID", DataTypes.StringType, true),
                new StructField("CA_ID", DataTypes.IntegerType, true),
                new StructField("CA_TAX_ST", DataTypes.StringType, true),
                new StructField("CA_B_ID", DataTypes.IntegerType, true),
                new StructField("CA_C_ID", DataTypes.IntegerType, true),
                new StructField("CA_NAME", DataTypes.StringType, true)
        )

        // need some custom transformations.
        fileName = "CustomerMgmt.csv"
        tableName = 'customer_mgmt'
        stagePath = "@${cliOptions.stage}/Batch${cliOptions.batch}/${fileName}"

        uploadFiles(fileName)

        def dfCustomer = session
                .read()
                .schema(customer)
                .option("field_delimiter", "|")
                .option("SKIP_HEADER",1)
                .csv(stagePath)
                .withColumn("action_ts", Functions.callUDF("to_timestamp", Functions.col("action_ts"), Functions.lit("yyyy-mm-ddThh:mi:ss")))

        saveDf(dfCustomer, tableName)

}

if (['all','taxrate'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType tax = StructType.create(
                new StructField("TX_ID", DataTypes.StringType, false),
                new StructField("TX_NAME", DataTypes.StringType, true),
                new StructField("TX_RATE", DataTypes.FloatType, true)
        )

        loadCsv(tax, 'TaxRate.txt', 'tax_rate')
}

if (['all','hr'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType hr = StructType.create(
                new StructField("EMPLOYEE_ID", DataTypes.IntegerType, false),
                new StructField("MANAGER_ID", DataTypes.IntegerType, false),
                new StructField("EMPLOYEE_FIRST_NAME", DataTypes.StringType, true),
                new StructField("EMPLOYEE_LAST_NAME", DataTypes.StringType, true),
                new StructField("EMPLOYEE_MI", DataTypes.StringType, true),
                new StructField("EMPLOYEE_JOB_CODE", DataTypes.IntegerType, true),
                new StructField("EMPLOYEE_BRANCH", DataTypes.StringType, true),
                new StructField("EMPLOYEE_OFFICE", DataTypes.StringType, true),
                new StructField("EMPLOYEE_PHONE", DataTypes.StringType, true)
        )

        loadCsv(hr, 'HR.csv', 'hr')
}

if (['all','watchhistory'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType watchHistory = StructType.create(
                new StructField("W_C_ID", DataTypes.IntegerType, false),
                new StructField("W_S_SYMB", DataTypes.StringType, true),
                new StructField("W_DTS", DataTypes.TimestampType, true),
                new StructField("W_ACTION", DataTypes.StringType, true)
        )

        loadCsv(watchHistory, "WatchHistory.txt", 'watch_history')

}

if (['all','trade'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType trade = StructType.create(
                new StructField("T_ID", DataTypes.IntegerType, false),
                new StructField("T_DTS", DataTypes.TimestampType, false),
                new StructField("T_ST_ID", DataTypes.StringType, false),
                new StructField("T_TT_ID", DataTypes.StringType, false),
                new StructField("T_IS_CASH", DataTypes.BooleanType, false),
                new StructField("T_S_SYMB", DataTypes.StringType, false),
                new StructField("T_QTY", DataTypes.FloatType,false),
                new StructField("T_BID_PRICE", DataTypes.FloatType, false),
                new StructField("T_CA_ID", DataTypes.IntegerType, false),
                new StructField("T_EXEC_NAME", DataTypes.StringType, false),
                new StructField("T_TRADE_PRICE", DataTypes.FloatType, true),
                new StructField("T_CHRG", DataTypes.FloatType, true),
                new StructField("T_COMM", DataTypes.FloatType, true),
                new StructField("T_TAX", DataTypes.FloatType, true)
        )

        loadCsv(trade, "Trade.txt", 'trade')
}

if (['all','tradehistory'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // a schema for realing a fixed width field as a single line
        StructType tradeHistory = StructType.create(
                new StructField("TH_T_ID", DataTypes.IntegerType, false),
                new StructField("TH_DTS", DataTypes.TimestampType, false),
                new StructField("TH_ST_ID", DataTypes.StringType, false)
        )

        loadCsv(tradeHistory, "TradeHistory.txt", 'trade_history')
}

if (['all','statustype'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        StructType statusType = StructType.create(
                new StructField("ST_ID", DataTypes.StringType, false),
                new StructField("ST_NAME", DataTypes.StringType, false)
        )

        loadCsv(statusType, "StatusType.txt", 'status_type')
}

session.close()