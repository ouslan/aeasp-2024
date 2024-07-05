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
        # self.blocks = self.process_shps()
        # self.process_lodes()
        self.process_roads()

    def process_shps(self) -> pl.DataFrame:
        empty_df = [
            pl.Series("STATEFP20", [], dtype=pl.String),
            pl.Series("GEOID20", [], dtype=pl.String),
            pl.Series("lon", [], dtype=pl.Float64),
            pl.Series("lat", [], dtype=pl.Float64),
        ]
        blocks = pl.DataFrame(empty_df).clear()
        if not os.path.exists("data/processed/blocks.parquet"):
            for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
                file_name = f"data/shape_files/block_{name}_{str(state).zfill(2)}.zip"
                tmp = gpd.read_file(file_name, engine="pyogrio")
                tmp = tmp.set_crs(3857, allow_override=True)
                tmp_shp = tmp[["STATEFP20", "GEOID20", "geometry"]].copy()
                tmp_shp["centroid"] = tmp_shp.centroid
                tmp_shp["lon"] = tmp_shp.centroid.x
                tmp_shp["lat"] = tmp_shp.centroid.y
                tmp_block = pl.from_pandas(
                    tmp_shp[["STATEFP20", "GEOID20", "lon", "lat"]]
                )
                blocks = pl.concat([blocks, tmp_block], how="vertical")
                print(
                    "\033[0;36mPROCESS: \033[0m" + f"Finished processing {name} Shapes"
                )
            blocks.sort(by=["STATEFP20", "GEOID20"]).write_parquet(
                "data/processed/blocks.parquet"
            )
            return blocks
        else:
            return pl.read_parquet("data/processed/blocks.parquet")

    def process_acs(self):
        empty_df = [
            pl.Series("year", [], dtype=pl.Date),
            pl.Series("state", [], dtype=pl.Int64),
            pl.Series("PUMA", [], dtype=pl.Int64),
            pl.Series("avg_time", [], dtype=pl.Float64),
            pl.Series("sex", [], dtype=pl.Int32),
            pl.Series("race", [], dtype=pl.String),
        ]
        acs = pl.DataFrame(empty_df).clear()

        for file in os.listdir("data/raw"):
            if file.startswith("acs"):
                original = pl.read_parquet(f"data/raw/{file}")
                for sex in [0, 1, 3]:
                    for race in [
                        "RACAIAN",
                        "RACASN",
                        "RACBLK",
                        "RACNUM",
                        "RACWHT",
                        "RACSOR",
                        "HISP",
                        "ALL",
                    ]:
                        df = original
                        if not sex == 3:
                            df = df.filter(pl.col("SEX") == sex)
                        if not race == "ALL":
                            df = df.filter(pl.col(race) == 1)
                        df = df.select("year", "state", "PUMA", "PWGTP", "JWMNP")
                        df = df.with_columns(
                            total_time=(pl.col("PWGTP") * pl.col("JWMNP"))
                        )
                        df = df.group_by("year", "state", "PUMA").agg(
                            pl.col("PWGTP", "total_time").sum()
                        )
                        df = df.select(
                            "year",
                            "state",
                            "PUMA",
                            (pl.col("total_time") / pl.col("PWGTP")).alias("avg_time"),
                        )
                        df = df.with_columns(
                            pl.col("year").cast(pl.String).str.to_date("%Y"),
                            sex=sex,
                            race=pl.lit(race),
                        )
                        acs = pl.concat([acs, df], how="vertical")
        return acs

    def process_roads(self):
        columns = ['linear_id', 'year', 'geometry']
        empty_data = {col: [] for col in columns}

        for year in range(2010, 2011):
            first_file_processed = False
            if os.path.exists(f"data/processed/roads_{year}.parquet"):
                continue
            for file in os.listdir(f"data/shape_files/"):
                if file.startswith(f"roads_{year}"):
                    gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio")
                    gdf.rename(columns={"LINEARID": "linear_id"}, inplace=True)
                    gdf["year"] = pd.to_datetime(year, format="%Y")
                    gdf = gdf[["linear_id", "year", "geometry"]].set_crs(3857, allow_override=True)
                    
                    if not first_file_processed:
                        roads_df = gdf.copy()  # Initialize roads_df with the first valid gdf
                        first_file_processed = True
                    else:
                        roads_df = pd.concat([roads_df, gdf], ignore_index=True)
                        
                    print("\033[0;36mINFO: \033[0m" + f"Finished processing roads for {file}")
            
            roads_df.to_parquet(f"data/processed/roads_{year}.parquet")


if __name__ == "__main__":
    DataProcess()
