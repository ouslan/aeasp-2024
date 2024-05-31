from src.data.data_pull import download_file
import geopandas as gpd
import polars as pl
import pandas as pd
import numpy as np
import zipfile
import os 
import json

class DataClean:

    def __init__(self, url="https://www2.census.gov/ces/movs/movs_st_main2005.csv", file_name="data/raw/movs_st_main2005.csv"):
        if not os.path.exists(file_name):
            download_file(url, file_name)
        self.df = pl.read_csv(file_name, ignore_errors=True)
        
        if not os.path.exists("data/shape_files/cb_2018_us_state_500k.shp"):
            download_file("https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip", "data/shape_files/tmp.zip")
            with zipfile.ZipFile("data/shape_files/tmp.zip", 'r') as zip_ref:
                zip_ref.extractall('data/shape_files')
        self.shp = gpd.read_file("data/shape_files/cb_2018_us_state_500k.shp")
        self.shp.rename({"GEOID": "state"}, axis=1, inplace=True)
        self.shp.rename({"NAME": "state_name"}, axis=1, inplace=True)
        self.shp["state"] = self.shp["state"].astype(int)  
        #self.shp = self.shp.to_crs("EPSG:3857")
        

    def graph(self, year):
        mov = self.df.group_by("state", "year").agg(pl.col("meqinc").mean().alias("avg_meqinc"))
        mov = mov.to_pandas()
        gdf = pd.merge(mov, self.shp, on="state", how="inner")
        gdf = gdf[gdf["year"] == year].reset_index().drop("index", axis=1)
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry")
        return gdf
