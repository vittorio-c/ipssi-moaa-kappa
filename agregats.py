# %%
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, mean, month, year, to_date, col
from pyspark.sql.types import *

from base_methods import all_data as all_stations


# %% [markdown]
#  ## Supprimer les lignes du champ TMP avec des valeurs vides OU des +9999

# %%
all_stations = all_stations.na.drop(how="any", subset=["TMP"]).filter(~all_stations.TMP.contains("+9999"))


# %% [markdown]
#  ## Split de la colonne température

# %%
@udf(returnType=FloatType())
def extract_tmp(tmp_col: str):
    return int(tmp_col.split(',')[0].lstrip('+')) / 10

all_stations = all_stations.withColumn('temperature', extract_tmp(all_stations['TMP']))


# %% [markdown]
#  ## Création du champ season

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


# %% [markdown]
#  ## Moyenne des températures par année/mois/journée/saison

# %%
# Par année
mean_tmp_by_year = all_stations.groupBy([year("DATE").alias("year")]).agg(mean("temperature").alias("mean_tmp"))
mean_tmp_by_year = mean_tmp_by_year.sort("year")
mean_tmp_by_year.head()


# %%
# Par mois
mean_tmp_by_month = all_stations.groupBy([year("DATE").alias("year"), month("DATE").alias("month")]).agg(mean("temperature").alias("mean_tmp"))
mean_tmp_by_month = mean_tmp_by_month.sort("year", "month")
# mean_tmp_by_month.head(5)


# %%
# Par jour
mean_tmp_by_day = all_stations.groupBy(to_date("DATE").cast("date").alias("date")).agg(mean("temperature").alias("mean_tmp"))
mean_tmp_by_day = mean_tmp_by_day.sort("date")
mean_tmp_by_day.head(10)


# %%
# Par saisons
mean_tmp_by_season = all_stations.groupBy([year("DATE").alias("year"), "season"]).agg(mean("temperature").alias("mean_tmp"))
mean_tmp_by_season = mean_tmp_by_season.sort("year", "season")
mean_tmp_by_season.head(10)



# %%
# Avec SQL
# all_stations.createOrReplaceTempView("stations_view")
# spark.sql('''SELECT STATION, year(DATE), month(DATE), month, AVG(temperature)
# FROM stations_view
# GROUP BY year(DATE), month(DATE)''').show()


# %% [markdown]
#  # Insertions SQL
#

# %%

from base_methods import mariadb_container_ip

def insert_data_in_mysql(dataframe, table_name):
    dataframe.write.format('jdbc').options(
      url=f'jdbc:mysql://{mariadb_container_ip}/moaa_db',
      dbtable=table_name,
      user='lambda-user',
      password='user_password')\
    .mode('overwrite')\
    .save()



# %%
table_name = 'mean_temperatures_by_year'

insert_data_in_mysql(mean_tmp_by_year, table_name)



# %%
table_name = 'mean_temperatures_by_month'

insert_data_in_mysql(mean_tmp_by_month, table_name)



# %%
table_name = 'mean_temperatures_by_day'

insert_data_in_mysql(mean_tmp_by_day, table_name)



# %%
table_name = 'mean_temperatures_by_season'

insert_data_in_mysql(mean_tmp_by_season, table_name)



# %% [markdown]
#  ## Min/Max des températures par année/mois/journée/saison

# %%
# Par année
min_max_tmp_by_year = all_stations.groupBy([year("DATE").alias("year")]).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_year = min_max_tmp_by_year.sort("year")
min_max_tmp_by_year.head(5)


# %%
# Par mois
min_max_tmp_by_month = all_stations.groupBy([year("DATE").alias("year"), month("DATE").alias("month")]).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_month = min_max_tmp_by_month.sort("year", "month")
min_max_tmp_by_month.head(10)


# %%
# Par jour
min_max_tmp_by_day = all_stations.groupBy(to_date("DATE").cast("date").alias("date")).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_day = min_max_tmp_by_day.sort("date")
min_max_tmp_by_day.head(100)


# %%
# Par saisons
min_max_tmp_by_season = all_stations.groupBy([year("DATE").alias("year"), "season"]).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_season = min_max_tmp_by_season.sort("year", "season")
min_max_tmp_by_season.head(10)


# %%
# Par Ville, Station et DATE
min_max_tmp_by_month_and_city = all_stations.groupBy([col("NAME").alias("ville"), col("STATION"), year("DATE").alias("year"), month("DATE").alias("month")]).agg(F.min("temperature").alias("min_tmp"), F.max("temperature").alias("max_tmp"))
min_max_tmp_by_month_and_city = min_max_tmp_by_month_and_city.sort("ville", "STATION", "year", "month")


# %%
table_name = 'min_max_temperatures_by_season'

insert_data_in_mysql(min_max_tmp_by_season, table_name)


# %%
table_name = 'min_max_temperatures_by_year'

insert_data_in_mysql(min_max_tmp_by_year, table_name)


# %%
table_name = 'min_max_temperatures_by_month'

insert_data_in_mysql(min_max_tmp_by_month, table_name)


# %%
table_name = 'min_max_temperatures_by_day'

insert_data_in_mysql(min_max_tmp_by_day, table_name)


# %%
table_name = "min_max_temperatures_by_month_and_city"

insert_data_in_mysql(min_max_tmp_by_month_and_city, table_name)


# %% [markdown]
#  # Moyenne des Températures et des Elevations (niveau de la mer) par année. @author: Steeve

# %%
all_stations_with_elev = all_stations.na.drop(how="any", subset=["ELEVATION"]).filter(~all_stations.ELEVATION.contains("+9999"))

mean_tmp_elevation_by_year = all_stations_with_elev.groupBy([year("DATE").alias("year"), "STATION"]).agg(mean("temperature").alias("mean_tmp"), mean("ELEVATION").alias("mean_elevation"))
mean_tmp_elevation_by_year = mean_tmp_elevation_by_year.sort("year")
mean_tmp_elevation_by_year.count()


# %% [markdown]
#  # Moyenne des Températures et précipitations par année et par mois.

# %%
all_stations_with_dew = all_stations.na.drop(how="any", subset=["DEW"]).filter(~all_stations.DEW.contains("+9999"))

@udf(returnType=FloatType())
def extract_dew(dew_col: str):
    return int(dew_col.split(',')[0].lstrip('+')) / 10

all_stations_with_dew = all_stations_with_dew.withColumn('precipitation', extract_dew(all_stations_with_dew['DEW']))

# %% pycharm={"name": "#%%\n"}

# Par année
mean_dew_by_year = all_stations_with_dew.groupBy([year("DATE").alias("year")]).agg(mean("precipitation").alias("mean_dew"))
mean_dew_by_year = mean_dew_by_year.sort("year")
mean_dew_by_year.head(10)

# %% pycharm={"name": "#%%\n"}

# Par mois
mean_dew_by_month = all_stations_with_dew.groupBy([year("DATE").alias("year"), month("DATE").alias("month")]).agg(mean("precipitation").alias("mean_dew"))
mean_dew_by_month = mean_dew_by_month.sort("year", "month")

# %% pycharm={"name": "#%%\n"}

# Par jour
mean_dew_by_day = all_stations_with_dew.groupBy(to_date("DATE").cast("date").alias("date")).agg(mean("precipitation").alias("mean_dew"))
mean_dew_by_day = mean_dew_by_day.sort("date")

# %% pycharm={"name": "#%%\n"}

# Par saison
mean_dew_by_season = all_stations_with_dew.groupBy([year("DATE").alias("year"), "season"]).agg(mean("precipitation").alias("mean_dew"))
mean_dew_by_season = mean_dew_by_season.sort("year", "season")

# %% pycharm={"name": "#%%\n"}

# Moyenne des Températures et des précipitations par année et par mois
mean_dew_tmp_by_month = all_stations_with_dew.groupBy([year("DATE").alias("year"), month("DATE").alias("month")]).agg(mean("temperature").alias("mean_tmp"), mean("precipitation").alias("mean_dew"))
mean_dew_tmp_by_month = mean_dew_tmp_by_month.sort("year", "month")
# mean_dew_tmp_by_month.head(100)

# %% pycharm={"name": "#%%\n"}
table_name = 'mean_dew_tmp_by_month'

insert_data_in_mysql(mean_dew_tmp_by_month, table_name)

