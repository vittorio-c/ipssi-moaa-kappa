import os
import requests
import urllib.request

from bs4 import BeautifulSoup


year_counter = 2022

while year_counter > 2007:
    url_dir = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year_counter}/"
    r  = requests.get(url_dir)
    data = r.text
    soup = BeautifulSoup(data, features="html5lib")
    for link in soup.find_all('a'):
        os.makedirs(f"/data/{year_counter}", exist_ok=True)
        print(f"DIRECTORY {year_counter} CREATED")
        if link.get('href').endswith(".csv"):
            urllib.request.urlretrieve(url_dir + link.string, filename=f"/data/{year_counter}/{link.string}")
            print(f"DOWNLOADED: {url_dir + link.string}")
    year_counter -= 1