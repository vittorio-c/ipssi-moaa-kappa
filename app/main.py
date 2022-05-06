import datetime
from dateutil.relativedelta import relativedelta

from flask import Flask, jsonify, render_template, request

from database.connection import mariabdb_client

app = Flask(__name__)

from cors import setup_cors

setup_cors()


@app.route("/")
def hello_world():
    return "<p>Hello world!</p>"


@app.route("/api/geo/by-year/<int:year>")
def geo_by_year(year):
    # Ajout de l'année comme param, parm envoyé depuis le front
    # et donc ajout d'un WHERE
    mycursor = mariabdb_client.cursor()

    # Belle injection ! Fix this for production
    mycursor.execute(f"SELECT * FROM grouped_by_year_by_stations WHERE year = {year}")

    stations = mycursor.fetchall()

    mycursor.execute("SELECT DISTINCT year FROM grouped_by_year_by_stations")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (station_id, year, mean_tmp, latitude, longitude) in stations:
        record = {
            "station_id": station_id,
            "year": year,
            "mean_tmp": mean_tmp,
            "latitude": latitude,
            "longitude": longitude
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/tmp/by-year")
def mean_by_year():
    mycursor = mariabdb_client.cursor()
    mycursor.execute(f"SELECT * FROM mean_temperatures_by_year")

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT year FROM mean_temperatures_by_year")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (year, mean_tmp) in temperatures:
        record = {
            "date": year,
            "mean_tmp": mean_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/tmp/by-month/<year>")
@app.route("/api/tmp/by-month/", defaults={'year': None})
def mean_by_month(year):
    mycursor = mariabdb_client.cursor()
    sql = "SELECT * FROM mean_temperatures_by_month"

    if year:
        sql += f' WHERE year = \'{year}\''

    mycursor.execute(sql)

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT year FROM mean_temperatures_by_month")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (year, month, mean_tmp) in temperatures:
        record = {
            "date": f"{month}-{year}",
            "mean_tmp": mean_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/tmp/by-day/<year>")
@app.route("/api/tmp/by-day/", defaults={'year': None})
def mean_by_day(year):
    mycursor = mariabdb_client.cursor()
    sql = "SELECT * FROM mean_temperatures_by_day"

    if year:
        sql += f' WHERE YEAR(date) = \'{year}\''
    else:
        one_year_ago = datetime.date.today() - relativedelta(years=5)
        sql += f' WHERE date > \'{one_year_ago}\''

    mycursor.execute(sql)

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT YEAR(date) FROM mean_temperatures_by_day")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (date, mean_tmp) in temperatures:
        record = {
            "date": date.strftime("%m/%d/%Y"),
            "mean_tmp": mean_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/tmp/by-season")
def mean_by_season():
    mycursor = mariabdb_client.cursor()
    mycursor.execute(f"SELECT * FROM mean_temperatures_by_season")

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT year FROM mean_temperatures_by_season")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (year, season, mean_tmp) in temperatures:
        record = {
            "date": f"{season} {year}",
            "mean_tmp": mean_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/min-max/by-season")
def min_max_by_season():
    mycursor = mariabdb_client.cursor()
    mycursor.execute(f"SELECT * FROM min_max_temperatures_by_season")

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT year FROM min_max_temperatures_by_season")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (year, season, min_tmp, max_tmp) in temperatures:
        record = {
            "date": f"{season} {year}",
            "min_tmp": min_tmp,
            "max_tmp": max_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/min-max/by-day/<year>")
@app.route("/api/min-max/by-day/", defaults={'year': None})
def min_max_by_day(year):
    mycursor = mariabdb_client.cursor()
    sql = "SELECT * FROM min_max_temperatures_by_day"

    if year:
        sql += f' WHERE YEAR(date) = \'{year}\''
    else:
        one_year_ago = datetime.date.today() - relativedelta(years=5)
        sql += f' WHERE date > \'{one_year_ago}\''

    mycursor.execute(sql)

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT YEAR(date) FROM min_max_temperatures_by_day")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (date, min_tmp, max_tmp) in temperatures:
        record = {
            "date": date.strftime("%m/%d/%Y"),
            "min_tmp": min_tmp,
            "max_tmp": max_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/min-max/by-month/<year>")
@app.route("/api/min-max/by-month/", defaults={'year': None})
def min_max_by_month(year):
    mycursor = mariabdb_client.cursor()
    sql = "SELECT * FROM min_max_temperatures_by_month"

    if year:
        sql += f' WHERE year = \'{year}\''

    mycursor.execute(sql)

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT year FROM min_max_temperatures_by_month")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (year, month, min_tmp, max_tmp) in temperatures:
        record = {
            "date": f"{month}-{year}",
            "min_tmp": min_tmp,
            "max_tmp": max_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/min-max/by-year")
def min_max_by_year():
    mycursor = mariabdb_client.cursor()
    mycursor.execute(f"SELECT * FROM min_max_temperatures_by_year")

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT year FROM min_max_temperatures_by_year")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (year, min_tmp, max_tmp) in temperatures:
        record = {
            "date": year,
            "min_tmp": min_tmp,
            "max_tmp": max_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/min-max/by-city/<city>/<year>")
@app.route("/api/min-max/by-city/<city>/", defaults={'year': None})
def min_max_temperatures_by_month_and_city(city, year):
    mycursor = mariabdb_client.cursor()
    sql = f"SELECT * FROM min_max_temperatures_by_month_and_city WHERE ville = '{city}'"

    if year:
        sql += f" AND year = {year}"

    mycursor.execute(sql)
    temperatures = mycursor.fetchall()

    mycursor.execute("SELECT DISTINCT ville FROM min_max_temperatures_by_month_and_city")
    available_city = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_city": [city[0] for city in available_city],
    }}

    for (city, station, year, month, min_tmp, max_tmp) in temperatures:
        record = {
            "city": city,
            "station": station,
            "date": str(month) + '-' + str(year),
            "min_tmp": min_tmp,
            "max_tmp": max_tmp,
        }
        out["_data"].append(record)

    return jsonify(out)


@app.route("/api/tmp-elevation/by-year/<year>")
@app.route("/api/tmp-elevation/by-year/", defaults={'year': None})
def tmp_elevation(year):
    mycursor = mariabdb_client.cursor()
    sql = f"SELECT * FROM mean_tmp_elevation_by_year WHERE station IS NOT NULL"

    if year:
        sql += f' AND year = \'{year}\''

    mycursor.execute(sql)

    temperatures = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT year FROM mean_tmp_elevation_by_year")
    available_years = mycursor.fetchall()

    out = {"_data": [], "_meta": {
        "distinct_years": [year[0] for year in available_years]
    }}
    for (year, station, mean_tmp, mean_elevation) in temperatures:
        record = {
            "date": year,
            "station": station,
            "mean_tmp": mean_tmp,
            "mean_elevation": mean_elevation,
        }
        out["_data"].append(record)

    return jsonify(out)

