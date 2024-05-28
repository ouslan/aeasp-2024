from src.data.data_pull import download_file
import polars as pl
import pandas as pd
import numpy as np
import os 
import json

class DataClean:

    def __init__(self, url, file_name):
        download_file(url, file_name)
        self.df = pl.read_csv("data/raw/movs_st_main2005.csv", ignore_errors=True)

    def clean_data(self):
        df = self.df.with_columns(
            pl.when(pl.col("sex") == -1).then(0).otherwise(pl.col("sex")).alias("sex"))
        
