
from pyspark.sql.types import FloatType, StructType, StructField, StringType, IntegerType, \
    TimestampType

schema = StructType([
    StructField("STATION", StringType(), True),
    StructField("DATE", TimestampType(), False),
    StructField("SOURCE", IntegerType(), True),
    StructField("LATITUDE", FloatType(), True),
    StructField("LONGITUDE", FloatType(), True),
    StructField("ELEVATION", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("REPORT_TYPE", StringType(), True),
    StructField("CALL_SIGN", StringType(), True),
    StructField("QUALITY_CONTROL", StringType(), True),
    StructField("WND", StringType(), True),
    StructField("CIG", StringType(), True),
    StructField("VIS", StringType(), True),
    StructField("TMP", StringType(), True),
    StructField("DEW", StringType(), True),
    StructField("SLP", StringType(), True),
    StructField("GA1", StringType(), True),
    StructField("GA2", StringType(), True),
    StructField("GA3", StringType(), True),
    StructField("GA4", StringType(), True),
    StructField("GF1", StringType(), True),
    StructField("MA1", StringType(), True),
    StructField("MW1", StringType(), True),
    StructField("MW2", StringType(), True),
    StructField("MW3", StringType(), True),
    StructField("OC1", StringType(), True),
    StructField("REM", StringType(), True),
    StructField("EQD", StringType(), True)
])