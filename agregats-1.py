# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown] pycharm={"name": "#%% md\n"}
# # Set up initial

# %% pycharm={"name": "#%%\n"}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
hadoop_uri = 'hdfs://localhost:9000/lambda-user/moaa_data/*'
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

# %% [markdown]
# ## Typage

# %% pycharm={"name": "#%%\n"}
from pyspark.sql.types import StructType, DateType, IntegerType, FloatType, StringType, TimestampType

schema = StructType() \
			.add("STATION",IntegerType(), False)\
			.add("DATE",TimestampType(), False)\
			.add("SOURCE",IntegerType(), True)\
			.add("LATITUDE",FloatType(), True)\
			.add("LONGITUDE",FloatType(), True)\
			.add("ELEVATION",StringType(), True)\
			.add("NAME",StringType(), True)\
			.add("REPORT_TYPE",StringType(), True)\
			.add("CALL_SIGN",StringType(), True)\
			.add("QUALITY_CONTROL",StringType(), True)\
			.add("WND", StringType(), True)\
			.add("CIG", StringType(), True)\
			.add("VIS", StringType(), True)\
			.add("TMP",StringType(), True)\
			.add("DEW",StringType(), True)\
			.add("SLP",StringType(),True)\
			.add("GA1",StringType(), True)\
			.add("GA2",StringType(), True)\
			.add("GA3",StringType(), True)\
			.add("GA4",StringType(), True)\
			.add("GF1",StringType(), True)\
			.add("MA1",StringType(), True)\
			.add("MW1",StringType(), True)\
			.add("MW2",StringType(), True)\
			.add("MW3",StringType(), True)\
			.add("OC1",StringType(), True)\
			.add("REM",StringType(), True)\
			.add("EQD",StringType(), True)

all_data = spark.read.load(
    "hdfs://localhost:9000/lambda-user/moaa_data/2000/01001499999.csv",
    format="csv",
    header=True,
    schema=schema
)

# %% pycharm={"name": "#%%\n"}
all_data.limit(10).toPandas()

# %% [markdown]
# # Moyenne des températures

# %% [markdown]
# ## Par mois

# %% pycharm={"name": "#%%\n"}
