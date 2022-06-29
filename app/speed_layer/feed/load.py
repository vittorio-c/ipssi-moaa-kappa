from pyspark.sql import SparkSession

from app.speed_layer.schema import schema

spark = SparkSession \
    .builder \
    .getOrCreate()

mariadb_container_ip = 'localhost:3306'

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

cols_of_interest = (
    "STATION", "DATE", "SOURCE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "REPORT_TYPE", "CALL_SIGN",
    "QUALITY_CONTROL", "WND", "CIG", "VIS", "TMP", "DEW", "SLP")

csv_dir_path = 'app/data/2020/*'

all_data = spark.read.load(
    csv_dir_path,
    format="csv",
    header=True,
    schema=schema,
    inferSchema=False
).select(*cols_of_interest)
