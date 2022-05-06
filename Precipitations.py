# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf, hour, mean, month, year, to_date
from pyspark.sql.window import Window

# %%
spark = SparkSession.builder.getOrCreate()

# %%
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

# %%
spark

# %% [markdown]
# # Schema de données

# %%
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

# %% [markdown]
# # Chargement des données

# %%
# station_2018 = spark.read.load("./data/2018", format="csv", header=True, schema=schema, inferSchema=False)
# station_2018.show()

# %%
cols_of_interest = ("STATION","DATE","SOURCE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","CALL_SIGN","QUALITY_CONTROL","WND","CIG","VIS","TMP","DEW","SLP")

all_stations = spark.read.load("./data/*", format="csv", header=True, schema=schema, inferSchema=False).select(*cols_of_interest)

# %%
all_stations

# %%
# Nombre de lignes
all_stations.count()

# %% [markdown]
# ## Supprimer les lignes du champ TMP avec des valeurs vides OU des +9999

# %%
all_stations = all_stations.na.drop(how="any", subset=["TMP"]).filter(~all_stations.TMP.contains("+9999"))

# %% [markdown]
# ## Supprimer les lignes du champ ELEVATION avec des valeurs vides OU des +9999

# %%
all_stations = all_stations.na.drop(how="any", subset=["ELEVATION"]).filter(~all_stations.ELEVATION.contains("+9999"))

# %% [markdown]
# ## Supprimer les lignes du champ DEW avec des valeurs vides OU des +9999

# %%
all_stations = all_stations.na.drop(how="any", subset=["DEW"]).filter(~all_stations.DEW.contains("+9999"))

# %%
# Nombre de lignes après le drop
all_stations.count()

# %%
# Le schema de données
all_stations.printSchema()

# %% [markdown]
# ## Split de la colonne température

# %%
@udf(returnType=FloatType())
def extract_tmp(tmp_col: str):
    return int(tmp_col.split(',')[0].lstrip('+')) / 10

all_stations = all_stations.withColumn('temperature', extract_tmp(all_stations['TMP']))

# %% [markdown]
# ## Split de la colonne DEW

# %%
@udf(returnType=FloatType())
def extract_dew(dew_col: str):
    return int(dew_col.split(',')[0].lstrip('+')) / 10

all_stations = all_stations.withColumn('precipitation', extract_dew(all_stations['DEW']))

# %%
all_stations.printSchema()

# %% [markdown]
# ## Création du champ season

# %%
@udf(returnType=StringType())
def create_season(month: int):
    if month in [7, 8, 9]:
        season = 'Summer'
    elif month in [10, 11, 12]:
        season = 'Autumn'
    elif month in [1, 2, 3]:
        season = 'Winter'
    else:
        season = 'Spring'
    return season

all_stations = all_stations.withColumn('season', create_season(month("DATE")))

# %%
all_stations.printSchema()

# %%


# %% [markdown]
# ## Moyenne des températures par année/mois/journée/saison

# %%
# Par année
mean_tmp_by_year = all_stations.groupBy([year("DATE").alias("year")]).agg(mean("temperature").alias("mean_tmp"))
mean_tmp_by_year = mean_tmp_by_year.sort("year")
mean_tmp_by_year

# %%
# Par mois
mean_tmp_by_month = all_stations.groupBy([year("DATE").alias("year"), month("DATE").alias("month")]).agg(mean("temperature").alias("mean_tmp"))
mean_tmp_by_month = mean_tmp_by_month.sort("year", "month")
mean_tmp_by_month

# %%
# Par jour
mean_tmp_by_day = all_stations.groupBy(to_date("DATE").cast("date").alias("date")).agg(mean("temperature").alias("mean_tmp"))
mean_tmp_by_day = mean_tmp_by_day.sort("date")
mean_tmp_by_day

# %%
# Par saison
mean_tmp_by_season = all_stations.groupBy([year("DATE").alias("year"), "season"]).agg(mean("temperature").alias("mean_tmp"))
mean_tmp_by_season = mean_tmp_by_season.sort("year", "season")
mean_tmp_by_season

# %%
# Avec SQL
# all_stations.createOrReplaceTempView("stations_view")
# spark.sql('''SELECT STATION, year(DATE), month(DATE), month, AVG(temperature)
# FROM stations_view
# GROUP BY year(DATE), month(DATE)''').show()

# %%


# %% [markdown]
# ## Min/Max des températures par année/mois/journée/saison

# %%
# Par année
min_max_tmp_by_year = all_stations.groupBy([year("DATE").alias("year")]).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_year = min_max_tmp_by_year.sort("year")
min_max_tmp_by_year

# %%
# Par mois
min_max_tmp_by_month = all_stations.groupBy([year("DATE").alias("year"), month("DATE").alias("month")]).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_month = min_max_tmp_by_month.sort("year", "month")
min_max_tmp_by_month

# %%
# Par jour
min_max_tmp_by_day = all_stations.groupBy(to_date("DATE").cast("date").alias("date")).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_day = min_max_tmp_by_day.sort("date")
min_max_tmp_by_day

# %%
# Par saison
min_max_tmp_by_season = all_stations.groupBy([year("DATE").alias("year"), "season"]).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_season = min_max_tmp_by_season.sort("year", "season")
min_max_tmp_by_season

# %%


# %% [markdown]
# ## Moyenne des Températures et des Elevations (niveau de la mer) par année et par station

# %%
mean_tmp_elevation_by_year = all_stations.groupBy([year("DATE").alias("year"), "STATION"]).agg(mean("temperature").alias("mean_tmp"), mean("ELEVATION").alias("mean_elevation"))
mean_tmp_elevation_by_year = mean_tmp_elevation_by_year.sort("year")
mean_tmp_elevation_by_year

# %%
df_pandas = mean_tmp_elevation_by_year.toPandas()

# %%
df_pandas.head()

# %%
import seaborn as sns
%matplotlib inline

# %%
g = sns.relplot(x="mean_tmp", y="mean_elevation", hue="year", data=df_pandas, palette ='magma')


# %%


# %% [markdown]
# ## Moyenne des précipitations par année/mois/journée/saison

# %%
# Par année
mean_dew_by_year = all_stations.groupBy([year("DATE").alias("year")]).agg(mean("precipitation").alias("mean_dew"))
mean_dew_by_year = mean_dew_by_year.sort("year")
mean_dew_by_year

# %%
# Par mois
mean_dew_by_month = all_stations.groupBy([year("DATE").alias("year"), month("DATE").alias("month")]).agg(mean("precipitation").alias("mean_dew"))
mean_dew_by_month = mean_dew_by_month.sort("year", "month")
mean_dew_by_month

# %%
# Par jour
mean_dew_by_day = all_stations.groupBy(to_date("DATE").cast("date").alias("date")).agg(mean("precipitation").alias("mean_dew"))
mean_dew_by_day = mean_dew_by_day.sort("date")
mean_dew_by_day

# %%
# Par saison
mean_dew_by_season = all_stations.groupBy([year("DATE").alias("year"), "season"]).agg(mean("precipitation").alias("mean_dew"))
mean_dew_by_season = mean_dew_by_season.sort("year", "season")
mean_dew_by_season

# %%


# %% [markdown]
# ## Moyenne des Températures et des précipitations par année et par mois

# %%
mean_dew_elevation_by_year = all_stations.groupBy([year("DATE").alias("year"), month("DATE").alias("month")]).agg(mean("temperature").alias("mean_tmp"), mean("precipitation").alias("mean_dew"))
mean_dew_elevation_by_year = mean_dew_elevation_by_year.sort("year", "month")
mean_dew_elevation_by_year

# %% [markdown]
# 

# %%


# %%


# %%


# %%


# %%


# %%


# %%


# %%



