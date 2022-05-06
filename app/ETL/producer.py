# Récupérer un fichier précis depuis le serveur

# Serialiser le fichier en json, ou bien en binaire

# Publier un message dans le BUS
import urllib.request
import csv

from kafka_producer_factory import get_producer


def download_file(url):
    (file, n) = urllib.request.urlretrieve(url, filename=f"./data/tmp/file1")

    return file


url = 'https://www.ncei.noaa.gov/data/global-hourly/access/1980/01001099999.csv'

downloaded_file_path = download_file(url)

print(downloaded_file_path)


producer = get_producer('localhost:9092')

print(producer)

with open(downloaded_file_path, "r") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=",")

    message = []
    for row in reader:
        message.append(row)
    print('Message prepared :')


# producer.send('moaa', message)
# print('Messge sent !')
producer.send('moaa', 'raw_bytes')
