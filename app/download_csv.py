import os
import requests
import urllib.request
from multiprocessing import Pool

from bs4 import BeautifulSoup


def import_by_years(year_range):
    year_range_as_string = ' '.join(map(str, year_range))
    print(f'Worker starting to work on year range : {year_range_as_string}')
    for year in year_range:
        url_dir = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/"
        r = requests.get(url_dir)
        data = r.text
        soup = BeautifulSoup(data, features="html5lib")
        for link in soup.find_all('a'):
            os.makedirs(f"./data/{year}", exist_ok=True)
            if link.get('href').endswith(".csv"):
                (r, n) = urllib.request.urlretrieve(url_dir + link.string, filename=f"./data/{year}/{link.string}")
                print(f"DOWNLOADED: {url_dir + link.string}")


rangings = [[i, f] for (i, f) in zip(range(2000, 2020, 2), range(2001, 2021, 2))]

if __name__ == '__main__':
    pool = Pool(processes=10)
    pool.map(import_by_years, rangings)

