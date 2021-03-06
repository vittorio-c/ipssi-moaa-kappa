import warnings

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, window, mean, to_json, struct, max, min
from pyspark.sql.types import FloatType, StructType, StructField, StringType, IntegerType, \
    TimestampType

warnings.filterwarnings("ignore")

conf = SparkConf()

conf.set("spark.jars.packages",
         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")

conf.set('loglevel', 'ERROR')

SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("MOAA-Stats-Live-Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

checkpointLocation = './app/speed_layer/stats_step/checkpoint/'

print('Spark loaded..........................................................................')

schema = StructType([
    StructField("STATION", StringType(), True),
    StructField("DATE", TimestampType(), False),
    StructField("SOURCE", IntegerType(), True),
    StructField("REPORT_TYPE", StringType(), True),
    StructField("CALL_SIGN", StringType(), True),
    StructField("QUALITY_CONTROL", StringType(), True),
    StructField("TMP", StringType(), True),
])

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_probes_results") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("parsed_value")) \
    .select(col("parsed_value.*"))


@udf(returnType=FloatType())
def extract_tmp(tmp_col: str):
    return int(tmp_col.split(',')[0].lstrip('+')) / 10


df = lines.na.drop(how="any", subset=["TMP"]) \
    .filter(~lines.TMP.contains("+9999"))

df = df.withColumn('temperature', extract_tmp(df['TMP']))

df = df \
    .withWatermark("DATE", "30 minutes") \
    .groupBy(window("DATE", "60 minutes").alias("datetime")) \
    .agg(
        mean("temperature").alias("mean_tmp"),
        max("temperature").alias("max_tmp"),
        min("temperature").alias("min_tmp"),
    )


# Start running the query that prints the running results to the console
query = df \
    .select(to_json(struct("datetime", "mean_tmp", "max_tmp", "min_tmp")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "agg_probes_results") \
    .option("checkpointLocation", checkpointLocation) \
    .trigger(processingTime='5 seconds') \
    .outputMode("complete") \
    .start()

query.awaitTermination()
