#!/usr/bin/env python
# coding: utf-8
# %%

from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col

spark = SparkSession.builder.getOrCreate()


# %%


# Optionnel :
# from pyspark.sql.types import StructType
# schema = StructType() \
#                     	.add("STATION", "integer")\
#                     	.add("DATE", "datetime")\
#                     	.add("SOURCE", "integer")\
#                     	.add("technology", "string")
#
# all_data = spark.read.load(
#     "hdfs://localhost:9000/lambda-user/moaa_data/2000/01001499999.csv",
#     format="csv",
#     header=True,
#     schema=schema
# )

hadoop_uri = 'hdfs://localhost:9000/lambda-user/moaa_data/*'

all_data = spark.read.load(
    hadoop_uri,
    format="csv",
    header=True,
    inferSchema="True"
)

all_data.head()


# %%
@udf(returnType = TimestampType())
def to_datetime(colonne):
    return datetime.strptime(colonne, '%Y-%m-%dT%H:%M:%S')


# %%
all_data_with_date = all_data.withColumn("DATE_TIME", to_datetime("DATE"))
filtered_by_city = all_data_with_date.filter(col("NAME").contains("JAN MAYEN NOR NAVY, NO"))
filtered_by_city_and_date = filtered_by_city.filter(col("DATE_TIME").contains("2016-01-01 00:00:00"))
filtered_by_city_and_date.show()
