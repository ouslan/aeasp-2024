import os
import numpy as np
import geopandas as gpd
import polars as pl
import pandas as pd
from src.data.data_pull import DataPull

class DataProcess(DataPull):

    def __init__(self):

        self.mov_file_url = "https://www2.census.gov/ces/movs/movs_st_main2005.csv"
        self.mov_file_path = "data/raw/movs_st_main2005.csv"
        self.shape_file_url = "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip"
        self.shape_file_path = "data/shape_files/states.zip"
        self.state_code_file_path = "data/external/state_code.parquet"
        self.blocks_file_path = "data/processed/blocks.parquet"
        self.lodes_file_path = "data/processed/lodes.parquet"
        
        self.mov = self.load_mov_data()
        self.shp = self.load_shape_data()
        self.codes = self.load_state_codes()
        self.blocks = self.load_blocks_data()
        self.lodes = self.load_lodes_data()
        self.df = self.create_graph_dataset()
    
    def retrieve_shps(self, blocks):

        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            print(f"Processing {name}, {state}")
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/tl_2023_{str(state).zfill(2)}_tabblock20.zip"
            file_name = f"data/shape_files/{name}_{str(state).zfill(2)}.zip"
            self.retrieve_file(url, file_name)
            tmp = gpd.read_file(file_name, engine="pyogrio")
            tmp = tmp.set_crs(3857, allow_override=True)
            tmp_shp = tmp[["STATEFP20", "GEOID20", "geometry"]].copy()
            tmp_shp["centroid"] = tmp_shp.centroid
            tmp_shp['lon'] = tmp_shp.centroid.x
            tmp_shp['lat'] = tmp_shp.centroid.y
            tmp_block = pl.from_pandas(tmp_shp[["STATEFP20", "GEOID20", "lon", "lat"]])
            blocks = pl.concat([blocks, tmp_block], how="vertical")
            print(f"Finished processing {state}")
        blocks.write_parquet(self.blocks_file_path)
    
    def process_lodes(self, lodes):

        for state, name, fips in self.codes.select(pl.col("state_abbr", "state_name", "fips")).rows():
            for year in range(2005, 2020):
                url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/od/{state}_od_main_JT00_{year}.csv.gz"
                file_name = f"data/raw/lodes_{state}_{year}.csv.gz"
                try:
                    self.retrieve_file(url, file_name)
                except:
                    print(f"Failed to download {name}, {state}, {year}")
                    continue
                value = self.process_lodes(file_name)
                tmp_df = pl.DataFrame([
                    pl.Series("state", [state], dtype=pl.String),
                    pl.Series("fips", [fips], dtype=pl.String),
                    pl.Series("state_abbr", [name], dtype=pl.String),
                    pl.Series("year", [year], dtype=pl.Int64),
                    pl.Series("avg_distance", [value], dtype=pl.Float64),
                ])
                lodes = pl.concat([lodes, tmp_df], how="vertical")
                print(f"Finished processing {name}, {state}, {year}")
        lodes.write_parquet(self.lodes_file_path)

    def process_lodes(self, path):

        df = pl.read_csv(path, ignore_errors=True)
        df = df.rename({"S000": "total_jobs"}).select(pl.col("w_geocode", "h_geocode", "total_jobs"))

        dest = self.blocks.rename({"GEOID20": "w_geocode", "lon": "w_lon", "lat": "w_lat"})
        dest = dest.with_columns((pl.col("w_geocode").cast(pl.Int64)).alias("w_geocode"))
        
        origin = self.blocks.rename({"GEOID20": "h_geocode", "lon": "h_lon", "lat": "h_lat"})
        origin = origin.with_columns((pl.col("h_geocode").cast(pl.Int64)).alias("h_geocode"))

        df = df.join(origin, on="h_geocode", how="left")
        df = df.join(dest, on="w_geocode", how="left")
        df = df.with_columns(
            (6371.01 * np.arccos(
                np.sin(pl.col("h_lat")) * np.sin(pl.col("w_lat")) + 
                np.cos(pl.col("h_lat")) * np.cos(pl.col("w_lat")) * 
                np.cos(pl.col("h_lon") - pl.col("w_lon"))
            )).alias("distance")
        )
        
        df = df.filter(pl.col("distance") != np.nan)
        df = df.select(pl.col("distance").sum().alias("total_distance"),
                       pl.col("total_jobs").sum().alias("total_jobs"))
        value = df.select((pl.col("total_distance") / pl.col("total_jobs")).alias("avg_distance")).item()
        return value