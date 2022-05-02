from bs4 import BeautifulSoup
import requests
import urllib.request


year_counter = 1901

while year_counter < 2023:
    url_dir = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year_counter}/"
    r  = requests.get(url_dir)
    data = r.text
    soup = BeautifulSoup(data, features="html5lib")
    for link in soup.find_all('a'):
        if link.get('href').endswith(".csv"):
            urllib.request.urlretrieve(url_dir + link.string, f"data/{link.string}")
            print(f"DOWNLOADED: {url_dir + link.string}")
    year_counter += 1