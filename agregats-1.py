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
from pyspark.sql.types import StructType, IntegerType, FloatType, StringType, TimestampType
from base_notebook import all_data

# %% pycharm={"name": "#%%\n"}
all_data.limit(10).toPandas()

# %% pycharm={"name": "#%%\n"}
all_data.printSchema()

# %% [markdown]
# # Moyenne des températures

# %% [markdown]
# ## Par mois

# %% pycharm={"name": "#%%\n"}
from pyspark.sql.functions import hour, mean, month, udf


@udf(returnType = FloatType())
def extract_tmp(tmp_col: str):
    return int(tmp_col.split(',')[0].lstrip('+')) / 10


all_data_with_tmp = all_data.withColumn('temperature', extract_tmp(all_data['TMP']))

mean_tmp_by_month = all_data_with_tmp.groupBy(month("DATE").alias("month")).agg(
	mean("temperature").alias('Mean TMP')
)

# all_data_with_tmp.limit(10).toPandas()
mean_tmp_by_month.collect()
