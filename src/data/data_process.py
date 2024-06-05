from urllib.request import urlretrieve
import geopandas as gpd
import polars as pl
import pandas as pd
import numpy as np
import zipfile
import os 
import json

class DataClean:

    def __init__(self, url="https://www2.census.gov/ces/movs/movs_st_main2005.csv", file_name="data/raw/movs_st_main2005.csv"):
        # verify mov file exists & load into polars dataframe
        if not os.path.exists(file_name):
            self.download_file(url, file_name)
        self.df = pl.read_csv(file_name, ignore_errors=True)
        
        # verify shape file exists & load into geopandas dataframe
        if not os.path.exists("data/shape_files/cb_2018_us_state_500k.shp"):
            self.download_file("https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip", "data/shape_files/tmp.zip")
            with zipfile.ZipFile("data/shape_files/tmp.zip", 'r') as zip_ref:
                zip_ref.extractall('data/shape_files')
        self.shp = gpd.read_file("data/shape_files/cb_2018_us_state_500k.shp")
        self.shp.rename({"GEOID": "state"}, axis=1, inplace=True)
        self.shp.rename({"NAME": "state_name"}, axis=1, inplace=True)
        self.shp["state"] = self.shp["state"].astype(int)  
        self.shp = self.shp.to_crs("EPSG:3857")

        # verify state_code file exists & load into pandas dataframe
        if not os.path.exists("data/raw/state_code.parquet"):
            self.codes = self.df.select(pl.col("state_abbr").str.to_lowercase().unique())
            self.codes = self.codes.filter(pl.col("state_abbr") != "us")
            self.codes = self.codes.join(self.df.with_columns(pl.col("state_abbr").str.to_lowercase()), on="state_abbr", how="inner")
            self.codes = self.codes.select(pl.col("state_abbr", "fips", "state_name")).unique()
            self.codes.write_parquet("data/external/state_code.parquet")
        else:
            self.codes = pl.read_parquet("data/external/state_code.parquet")

        
    def download_file(self, url, filename):
        if not os.path.exists(f"{os.getcwd}{filename}"):
            urlretrieve(url, filename)

    def graph(self, year):
        mov = self.df.group_by("state", "year").agg(pl.col("meqinc").mean().alias("avg_meqinc"))
        mov = mov.to_pandas()
        gdf = pd.merge(mov, self.shp, on="state", how="inner")
        gdf = gdf[gdf["year"] == year].reset_index().drop("index", axis=1)
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry")
        return gdf

    def retreve_shps(self):
        for state in self.codes["fips"]:
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/tl_2023_{str(state).zfill(2)}_tabblock20.zip"
            file_name = f"data/shape_files/tl_2023_{str(state).zfill(2)}_tabblock20.zip"
            if not os.path.exists(file_name):
                self.download_file(url, file_name)
            with zipfile.ZipFile(file_name, 'r') as zip_ref:
                zip_ref.extractall('data/shape_files')      
    
    def make_us_shp(self, path):
        table = read_pyogrio(path)
        gdf = to_geopandas(table)
        