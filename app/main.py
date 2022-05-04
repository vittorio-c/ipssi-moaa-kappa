from flask import Flask, jsonify, render_template, request

from database.connection import mariabdb_client

app = Flask(__name__)

from cors import setup_cors

setup_cors()


@app.route("/")
def hello_world():
    return "<p>Hello, 222!</p>"


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
