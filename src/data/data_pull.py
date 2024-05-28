import os
from urllib.request import urlretrieve

def download_file(url, filename):
    if not os.path.exists(f"{os.getcwd}{filename}"):
        urlretrieve(url, filename)

if __name__ == '__main__':
    url = "https://www2.census.gov/ces/movs/movs_st_main2005.csv"
    filename = "data/raw/movs_st_main2005.csv"
    download_file(url, filename)