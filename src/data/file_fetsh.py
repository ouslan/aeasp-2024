import os
from urllib.request import urlretrieve

def download_file(url, filename):
    if not os.path.exists(filename):
        urlretrieve(url, filename)