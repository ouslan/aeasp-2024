from src.data.data_pull import DataPull
import geopandas as gpd
import pandas as pd
import polars as pl
import numpy as np
import os


class DataProcess(DataPull):
    def __init__(self, debug=False):
        super().__init__()
        self.debug = debug
        self.process_states()
        self.process_county()
        self.process_pumas()
        self.process_acs()
        self.process_roads()

    def process_states(self):
        if not os.path.exists("data/interim/states.gpkg"):
            gdf = gpd.read_file("data/shape_files/states.zip", engine="pyogrio")
            gdf.to_file("data/interim/states.gpkg", driver="GPKG")
            if self.debug:
                print("\033[0;36mINFO: \033[0m" + "Finished processing states")

    def process_county(self):
        if not os.path.exists("data/interim/counties.gpkg"):
            gdf = gpd.read_file("data/shape_files/counties.zip", engine="pyogrio")
            gdf.to_file("data/interim/counties.gpkg", driver="GPKG")
            if self.debug:
                print("\033[0;36mINFO: \033[0m" + "Finished processing counties")

    def process_pumas(self):
        if not os.path.exists("data/interim/puma.gpkg"):
            puma_df = gpd.GeoDataFrame(columns=["geo_id", "name", "geometry"])
            for file in os.listdir("data/shape_files"):
                if file.startswith("puma"):
                    gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio").set_crs(3857, allow_override=True)
                    gdf = gdf[["GEOID10", "NAMELSAD10", "geometry"]].rename(columns={"GEOID10": "geo_id", "NAMELSAD10": "name"})
                    puma_df = pd.concat([puma_df, gdf], ignore_index=True, verify_integrity=True)
            puma_df.to_file("data/interim/puma.gpkg", driver="GPKG")
            if self.debug:
                print("\033[0;36mINFO: \033[0m" + "Finished processing pumas")

    def process_acs(self):
        empty_df = [
            pl.Series("year", [], dtype=pl.Int64),
            pl.Series("state", [], dtype=pl.Int64),
            pl.Series("PUMA", [], dtype=pl.Int64),
            pl.Series("PWGTP", [], dtype=pl.Int64),
            pl.Series("avg_time", [], dtype=pl.Float64),
            pl.Series("sex", [], dtype=pl.Int32),
            pl.Series("race", [], dtype=pl.String),
        ]
        acs = pl.DataFrame(empty_df).clear()

        for file in os.listdir("data/raw"):
            if file.startswith("acs"):
                original = pl.read_parquet(f"data/raw/{file}")
                for sex in [1, 2, 3]:
                    for race in ["RACAIAN","RACASN","RACBLK","RACNUM","RACWHT","RACSOR","HISP","ALL",]:
                        df = original
                        if not sex == 3:
                            df = df.filter(pl.col("SEX") == sex)
                        if not race == "ALL":
                            df = df.filter(pl.col(race) == 1)
                        df = df.filter(pl.col("JWMNP") > 0)
                        df = df.select("year", "state", "PUMA", "PWGTP", "JWMNP")
                        df = df.with_columns(total_time=(pl.col("PWGTP") * pl.col("JWMNP")))
                        df = df.group_by("year", "state", "PUMA").agg(
                                                                      pl.col("PWGTP", "total_time").sum())
                        df = df.select("year","state", "PUMA", "PWGTP",
                                       (pl.col("total_time") / pl.col("PWGTP")).alias("avg_time"),
                                      )
                        df = df.with_columns(
                                             sex=pl.lit(sex),
                                             race=pl.lit(race),
                        )
                        acs = pl.concat([acs, df], how="vertical")
        acs.write_parquet("data/interim/acs.parquet")
        if self.debug:
            print("\033[0;36mINFO: \033[0m" + "Finished processing acs")

    def process_roads(self):
        roads = gpd.GeoDataFrame(columns=['linear_id', 'year', 'geometry'])
        
        for year in range(2012, 2019):
            roads_df = gpd.GeoDataFrame(columns=['linear_id', 'year', 'geometry'])
            if os.path.exists(f"data/interim/roads_{year}.gpkg"):
                continue
            for file in os.listdir(f"data/shape_files/"):
                if file.startswith(f"roads_{year}"):
                    gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio")
                    gdf.rename(columns={"LINEARID": "linear_id"}, inplace=True)
                    gdf[["county_id", "year"]] = "01063", 2012
                    gdf = gdf[["year", "linear_id", "county_id", "geometry"]].set_crs(3857, allow_override=True)
                    
                    roads_df = pd.concat([roads_df, gdf], ignore_index=True) 
                    if self.debug:
                        print("\033[0;36mINFO: \033[0m" + f"Finished processing roads for {file}")
            
            roads_df.to_file(f"data/interim/roads_{year}.gpkg", driver="GPKG")

if __name__ == "__main__":
    DataProcess()
