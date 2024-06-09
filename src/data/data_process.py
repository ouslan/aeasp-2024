from geoarrow.rust.core import read_pyogrio, to_geopandas
from geoarrow.rust.core import from_shapely
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
        if not os.path.exists("data/shape_files/states.zip"):
            self.download_file("https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip", "data/shape_files/states.zip")
        self.shp = gpd.read_file("data/shape_files/states.zip", engine="pyogrio")
        self.shp.rename({"GEOID": "state"}, axis=1, inplace=True)
        self.shp.rename({"NAME": "state_name"}, axis=1, inplace=True)
        self.shp["state"] = self.shp["state"].astype(int)
        #self.shp = self.shp.set_crs('epsg:3857')

        # verify state_code file exists & load into pandas dataframe
        if not os.path.exists("data/raw/state_code.parquet"):
            self.codes = self.df.select(pl.col("state_abbr").str.to_lowercase().unique())
            self.codes = self.codes.filter(pl.col("state_abbr") != "us")
            self.codes = self.codes.join(self.df.with_columns(pl.col("state_abbr").str.to_lowercase()), on="state_abbr", how="inner")
            self.codes = self.codes.select(pl.col("state_abbr", "fips", "state_name")).unique()
            self.codes.write_parquet("data/external/state_code.parquet")
        else:
            self.codes = pl.read_parquet("data/external/state_code.parquet")

        #creating the census block Dataset
        empty_df = [
                    pl.Series("STATEFP20", [], dtype=pl.String),
                    pl.Series("GEOID20", [], dtype=pl.String),
                    pl.Series("lon", [], dtype=pl.Float64),
                    pl.Series("lat", [], dtype=pl.Float64),
                ]
        if not os.path.exists("data/processed/blocks.parquet"):
            self.blocks = pl.DataFrame(empty_df).clear()
            self.retreve_shps()
        else:
            self.blocks = pl.read_parquet("data/processed/blocks.parquet")
            self.blocks = self.blocks.unique()

        # create the lodes dataset
        if not os.path.exists("data/processed/lodes.parquet"):
            empty_df = [
                        pl.Series("state", [], dtype=pl.String),
                        pl.Series("fips", [], dtype=pl.String),
                        pl.Series("state_abbr", [], dtype=pl.String),
                        pl.Series("year", [], dtype=pl.Int64),
                        pl.Series("avg_distance", [], dtype=pl.Float64),
                    ]
            self.lodes = pl.DataFrame(empty_df).clear()
            #self.lodes = self.retreve_lodes()

    def retreve_shps(self):
        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            print(f"processing {name}, {state}")
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/tl_2023_{str(state).zfill(2)}_tabblock20.zip"
            file_name = f"data/shape_files/{name}_{str(state).zfill(2)}.zip"
            self.download_file(url, file_name)
            
            tmp = gpd.read_file(file_name, engine="pyogrio")
            tmp = tmp.set_crs(3857, allow_override=True)
            tmp_shp = tmp[["STATEFP20", "GEOID20", "geometry"]].copy()
            
            tmp_shp["centroid"] = tmp_shp.centroid
            tmp_shp['lon'] = tmp_shp.centroid.x
            tmp_shp['lat'] = tmp_shp.centroid.y
            tmp_block = pl.from_pandas(tmp_shp[["STATEFP20", "GEOID20", "lon", "lat"]])
            
            self.blocks = pl.concat([self.blocks, tmp_block], how="vertical")
            print(f"finished procecing {state}")
            #os.remove(file_name)
        self.blocks.write_parquet("data/processed/blocks.parquet")
    
    def retreve_lodes(self):
        if not os.path.exists("data/processed/lodes.parquet"):
            for state, name, fips in self.codes.select(pl.col("state_abbr", "state_name", "fips" )).rows():
                for year in range(2005, 2020):
                    url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/od/{state}_od_main_JT00_{year}.csv.gz"
                    file_name = f"data/raw/lodes_{state}_{year}.csv.gz"
                    try:
                        self.download_file(url, file_name)
                    except:
                        print(f"failed to download {name}, {state}, {year}")
                        continue
                    value = self.processe_lodes(file_name)
                    tmp_df = [
                    pl.Series("state", [state], dtype=pl.String),
                    pl.Series("fips", [fips], dtype=pl.String),
                    pl.Series("state_abbr", [name], dtype=pl.String),
                    pl.Series("year", [year], dtype=pl.Int64),
                    pl.Series("avg_distance", [value], dtype=pl.Float64),
                    ]
                    tmp_df = pl.DataFrame(tmp_df)
                    self.lodes = pl.concat([self.lodes, tmp_df], how="vertical")
                    print(f"finished processing {name}, {state}, {year}")
            self.lodes.write_parquet("data/processed/lodes.parquet")
 
    def download_file(self, url, filename):
        if not os.path.exists(f"{filename}"):
            urlretrieve(url, filename)

    def graph(self, year):
        mov = pl.read_parquet("data/processed/lodes.parquet")
        mov = mov.rename({"state": "STUSPS"})
        mov = mov.with_columns(pl.col("STUSPS").str.to_uppercase())
        mov = mov.to_pandas()
        gdf = pd.merge(mov, self.shp, on="STUSPS", how="inner")
        gdf = gdf[gdf["year"] == year].reset_index().drop("index", axis=1)
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry")
        return gdf
    
    def processe_lodes(self, path):
        df = pl.read_csv(path, ignore_errors=True)
        df = df.rename({"S000":"total_jobs"}).select(pl.col("w_geocode", "h_geocode", "total_jobs"))

        dest = self.blocks
        dest = dest.rename({"GEOID20":"w_geocode", "lon":"w_lon", "lat":"w_lat"})
        dest = dest.with_columns((pl.col("w_geocode").cast(pl.Int64)).alias("w_geocode"))
        
        origin = self.blocks
        origin = origin.rename({"GEOID20":"h_geocode", "lon":"h_lon", "lat":"h_lat"})
        origin = origin.with_columns((pl.col("h_geocode").cast(pl.Int64)).alias("h_geocode"))

        df = df.join(origin, on="h_geocode", how="left")
        df = df.join(dest, on="w_geocode", how="left")
        df = df.with_columns(
                        (np.sqrt((pl.col("h_lon")-pl.col("w_lon"))**2 + (pl.col("h_lat")-pl.col("w_lat"))**2)*pl.col("total_jobs")).alias("distance"))
        df = df.select(pl.col("distance").sum().alias("total_distance"),
                   pl.col("total_jobs").sum().alias("total_jobs"))
        df = df.select((pl.col("total_distance")/pl.col("total_jobs")).alias("avg_distance")).item()
        return df