#!/opt/homebrew/bin/groovy

@Grab('com.snowflake:snowpark:1.8.0')
@Grab('commons-cli:commons-cli:1.5.0')

import com.snowflake.snowpark_java.types.*
import com.snowflake.snowpark_java.*
import groovy.cli.commons.CliBuilder
import groovy.io.FileType

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
cli.d(longOpt: 'directory', "Local output directory from the DIGen.jar file generation. Defaults to current directory.", args: 1, defaultValue: directory)

def cliOptions = cli.parse(args)
def dir = new File(cliOptions.directory)

// upload files
def uploadFiles(String pattern, String directory, String stage, String batch, Session session, Map options) {
        // load all FINWIRE files
        new File(directory).eachFileRecurse (FileType.FILES) { file ->
                if (file.parentFile.name.contains("Batch${batch}") && (!file.name.contains("audit")) && (file.name.contains(pattern)) && (file.name != 'BatchDate.txt')) {
                        PutResult[] pr = session.file().put(file.path, "@${stage}/Batch${batch}/${file.name}", options)
                        pr.each {
                                println "File ${it.sourceFileName}: ${it.status}"
                        }
                }
        }
}

def session = Session.builder().configFile("credentials.properties").create()

def options = [
        AUTO_COMPRESS: 'TRUE',
        PARALLEL     : '4',
        OVERWRITE    : (cliOptions.overwrite ? 'TRUE' : 'FALSE')
]

if (cliOptions.reset) {
        session.jdbcConnection().createStatement().execute("create or replace stage ${cliOptions.stage} directory = (enable = true)")
        session.jdbcConnection().createStatement().execute("drop table CMP")
        session.jdbcConnection().createStatement().execute("drop table SEC")
        session.jdbcConnection().createStatement().execute("drop table FIN")

        println "Tables deleted and stage '${cliOptions.stage}' reset."
}

session.jdbcConnection().createStatement().execute("create stage if not exists ${cliOptions.stage} directory = (enable = true)")

// a schema for realing a fixed width field as a single line
StructType fixed = StructType.create(
  new StructField("line", DataTypes.StringType, false)
)

// reusable DataFrameReader
def dfr = session
        .read()
        .schema(fixed)
        .option("field_delimiter", "|")

if (['all','finwire'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {

        // load all FINWIRE files
        uploadFiles('FINWIRE', cliOptions.directory, cliOptions.stage, cliOptions.batch, session, options)

        def stagePath = "@${cliOptions.stage}/Batch${batch}/FINWIRE"

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
                .drop(Functions.col("line"))
                .write().mode(SaveMode.Overwrite).saveAsTable("cmp")
                //.show()

        println "CMP table created."

        // create the SEC table
        def dfs = dfr.csv(stagePath)
                .withColumn('pts', Functions.substring(Functions.col("line"), Functions.lit(0), Functions.lit(15)))
                .withColumn('rec_type', Functions.substring(Functions.col("line"), Functions.lit(16), Functions.lit(3)))
                .where(Functions.col('rec_type').equal_to(Functions.lit('SEC')))
                .withColumn('symbol', Functions.substring(Functions.col("line"), Functions.lit(19), Functions.lit(15)))
                .withColumn('issue_type', Functions.substring(Functions.col("line"), Functions.lit(34), Functions.lit(6)))
                .withColumn('status', Functions.substring(Functions.col("line"), Functions.lit(154), Functions.lit(4)))
                .withColumn('name', Functions.substring(Functions.col("line"), Functions.lit(44), Functions.lit(70)))
                .withColumn('ex_id', Functions.substring(Functions.col("line"), Functions.lit(114), Functions.lit(6)))
                .withColumn('sh_out', Functions.substring(Functions.col("line"), Functions.lit(120), Functions.lit(13)))
                .withColumn('first_trade_date', Functions.substring(Functions.col("line"), Functions.lit(133), Functions.lit(8)))
                .withColumn('first_exchange_date', Functions.substring(Functions.col("line"), Functions.lit(141), Functions.lit(8)))
                .withColumn('dividend', Functions.substring(Functions.col("line"), Functions.lit(149), Functions.lit(12)))
                .withColumn('co_name_or_cik', Functions.substring(Functions.col("line"), Functions.lit(161), Functions.lit(60)))
                .drop(Functions.col("line"))
                .write().mode(SaveMode.Overwrite).saveAsTable("sec")
                //.show()

        println "SEC table created."

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
                .drop(Functions.col("line"))
                .write().mode(SaveMode.Overwrite).saveAsTable("fin")
                //.show()

        println "FIN table created."

}

if (['all','statustype'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {
        def fileName = "StatusType.txt"
        def stagePath = "@${cliOptions.stage}/Batch${cliOptions.batch}/${fileName}"

        uploadFiles(fileName, cliOptions.directory, cliOptions.stage, cliOptions.batch, session, options)

        // a schema for realing a fixed width field as a single line
        StructType statusType = StructType.create(
                new StructField("ST_ID", DataTypes.StringType, false),
                new StructField("ST_NAME", DataTypes.StringType, false)
        )

        def dfrst = session
                .read()
                .schema(statusType)
                .option("field_delimiter", "|")

        def dfst = dfrst.csv(stagePath)
                .write().mode(SaveMode.Overwrite).saveAsTable("status_type")
                //.show()

        println "STATUS_TYPE table created."

}

if (['all','industry'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {
        def fileName = "Industry.txt"
        def stagePath = "@${cliOptions.stage}/Batch${cliOptions.batch}/${fileName}"

        uploadFiles(fileName, cliOptions.directory, cliOptions.stage, cliOptions.batch, session, options)

        // a schema for realing a fixed width field as a single line
        StructType industry = StructType.create(
                new StructField("IN_ID", DataTypes.StringType, false),
                new StructField("IN_NAME", DataTypes.StringType, false),
                new StructField("IN_SC_ID", DataTypes.StringType, false)
        )

        def dfri = session
                .read()
                .schema(industry)
                .option("field_delimiter", "|")

        def dfi = dfri.csv(stagePath)
                .write().mode(SaveMode.Overwrite).saveAsTable("industry")
                //.show()

        println "INDUSTRY table created."

}

if (['all','dailymarket'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {
        def fileName = "DailyMarket.txt"
        def stagePath = "@${cliOptions.stage}/Batch${cliOptions.batch}/${fileName}"

        uploadFiles(fileName, cliOptions.directory, cliOptions.stage, cliOptions.batch, session, options)

        // a schema for realing a fixed width field as a single line
        StructType dailyMarket = StructType.create(
                new StructField("DM_DATE", DataTypes.DateType, false),
                new StructField("DM_S_SYMB", DataTypes.StringType, false),
                new StructField("DM_CLOSE", DataTypes.FloatType, false),
                new StructField("DM_HIGH", DataTypes.FloatType, false),
                new StructField("DM_LOW", DataTypes.FloatType, false),
                new StructField("DM_VOL", DataTypes.FloatType, false)
        )

        def dfrdm = session
                .read()
                .schema(dailyMarket)
                .option("field_delimiter", "|")

        def dfdm = dfrdm.csv(stagePath)
                .write().mode(SaveMode.Overwrite).saveAsTable("daily_market")
                //.show()

        println "DAILY_MARKET table created."

}

if (['all','date'].contains(cliOptions.filetype.toLowerCase()) && !cliOptions.reset) {
        def fileName = "Date.txt"
        def stagePath = "@${cliOptions.stage}/Batch${cliOptions.batch}/${fileName}"

        uploadFiles(fileName, cliOptions.directory, cliOptions.stage, cliOptions.batch, session, options)

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

        def dfrd = session
                .read()
                .schema(date)
                .option("field_delimiter", "|")

        def dfd = dfrd.csv(stagePath)
                .write().mode(SaveMode.Overwrite).saveAsTable("date")
                //.show()

        println "DATE table created."

}

session.close()