import csv
import datetime
import os
import pickle
import socket
import time
import urllib.parse
from datetime import timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup

csv_path = 'app/speed_layer/feed/data/stations.csv'


def get_stations_links():
    url_dir = f"https://www.ncei.noaa.gov/data/global-hourly/access/2022/"
    r = requests.get(url_dir)
    data = r.text
    soup = BeautifulSoup(data, features="html5lib")

    return soup.find_all('a')


def write_headers():
    if os.path.isfile(csv_path):
        os.remove(csv_path)
    csv_headers = ["STATION", "DATE", "SOURCE", "REPORT_TYPE", "CALL_SIGN", "QUALITY_CONTROL", "TMP"]
    with open(csv_path, 'a', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)


def get_url_params(stations_names_as_string):
    today = datetime.date.today()
    startdate = today - timedelta(days=3)

    return {
        'stations': stations_names_as_string,
        'dataset': 'global-hourly',
        'dataTypes': 'TMP',
        'startDate': f'{startdate.isoformat()}T00:00:00Z',
        'endDate': f'{startdate.isoformat()}T23:59:59Z',
        'format': 'csv'
    }


def download_today_data():
    print('Starting downloading the data...')
    write_headers()
    step_size = 50  # max requested station quantity allowed by MOAA API
    number_of_statoins = 100  # how many stations you want to retrieve
    start_at = 10  # we start at 10 because first records are garbage

    for i in range(start_at, number_of_statoins, step_size):
        stations_names = []
        links = get_stations_links()
        for link in links[i:i + step_size]:
            station_name = link.contents[0]
            station_name = station_name.split('.')[0]
            stations_names.append(station_name)

        api_url = "https://www.ncei.noaa.gov/access/services/data/v1/?"
        params = get_url_params(','.join(stations_names))
        final_url = api_url + urllib.parse.urlencode(params)

        response = requests.get(final_url)
        response_as_dict = response.text.split('\n')

        # remove headers
        response_as_dict.pop(0)

        print(f'Writing results to file for step {i}...')
        with open(csv_path, 'a', encoding='utf-8') as f:
            writer = csv.writer(f)

            for index, row in enumerate(response_as_dict):
                response_as_dict[index] = [i.strip('"') for i in row.split('",')]

            # remove last line because it's empty...
            response_as_dict.pop(-1)

            writer.writerows(response_as_dict)

    print('Done !')


def send_to_server_socket(client, payload):
    print('>> Sending data to socket')
    data = pickle.dumps(payload[1].to_dict())
    client.send(data)


def publish_data():
    print("Starting publishing the data to socket...")
    if os.path.exists("/tmp/python_unix_sockets_example"):
        client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        client.connect("/tmp/python_unix_sockets_example")
        print("Ready.")
        print("Ctrl-C to quit.")

        dtypes = {'STATION': 'str', 'TMP': 'str'}
        parse_dates = ['DATE']
        df = pd.read_csv(csv_path, dtype=dtypes, parse_dates=parse_dates)
        df = df.sort_values(by=['DATE'])

        try:
            for row in df.iterrows():
                time.sleep(0.1)
                send_to_server_socket(client, row)
        except KeyboardInterrupt as k:
            client.close()
            print("Shutting down.")
        except Exception as e:
            print('An error occured !')
            print(e)

    else:
        print("Couldn't Connect!")
        print("Done")


if __name__ == "__main__":
    # download_today_data()
    publish_data()
