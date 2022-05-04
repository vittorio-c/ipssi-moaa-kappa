# %% pycharm={"name": "#%%\n"}
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
hadoop_uri = 'hdfs://localhost:9000/lambda-user/moaa_data/*'
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

# %% pycharm={"name": "#%%\n"}
from pyspark.sql.types import StructType, IntegerType, FloatType, StringType, TimestampType

schema = StructType() \
    .add("STATION", IntegerType(), False) \
    .add("DATE", TimestampType(), False) \
    .add("SOURCE", IntegerType(), True) \
    .add("LATITUDE", FloatType(), True) \
    .add("LONGITUDE", FloatType(), True) \
    .add("ELEVATION", StringType(), True) \
    .add("NAME", StringType(), True) \
    .add("REPORT_TYPE", StringType(), True) \
    .add("CALL_SIGN", StringType(), True) \
    .add("QUALITY_CONTROL", StringType(), True) \
    .add("WND", StringType(), True) \
    .add("CIG", StringType(), True) \
    .add("VIS", StringType(), True) \
    .add("TMP", StringType(), True) \
    .add("DEW", StringType(), True) \
    .add("SLP", StringType(), True) \
    .add("GA1", StringType(), True) \
    .add("GA2", StringType(), True) \
    .add("GA3", StringType(), True) \
    .add("GA4", StringType(), True) \
    .add("GF1", StringType(), True) \
    .add("MA1", StringType(), True) \
    .add("MW1", StringType(), True) \
    .add("MW2", StringType(), True) \
    .add("MW3", StringType(), True) \
    .add("OC1", StringType(), True) \
    .add("REM", StringType(), True) \
    .add("EQD", StringType(), True)


cols_of_interest = ("STATION","DATE","SOURCE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","CALL_SIGN","QUALITY_CONTROL","WND","CIG","VIS","TMP","DEW","SLP")

all_data = spark.read.load(
    hadoop_uri,
    format="csv",
    header=True,
    schema=schema,
    inferSchema=False
).select(*cols_of_interest)



# %% pycharm={"name": "#%%\n"}
all_data.limit(10).toPandas()
