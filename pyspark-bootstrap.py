#!/usr/bin/env python
# coding: utf-8
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
# ---

# %%

# %%


from pyspark.sql import SparkSession

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
