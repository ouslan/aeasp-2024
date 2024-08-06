from concurrent.futures import ThreadPoolExecutor, as_completed
from src.data.data_pull import DataPull
import geopandas as gpd
import pandas as pd
import polars as pl
import cpi
import os

class DataProcess(DataPull):
    def __init__(self, debug=False):
        super().__init__()
        self.debug = debug
        self.pumas = self.process_pumas()
        self.process_states()
        self.process_county()
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
        puma_df = gpd.GeoDataFrame(columns=["puma_id", "name", "geometry"])
        if not os.path.exists("data/interim/pumas.gpkg"):
            for file in os.listdir("data/shape_files"):
                if file.startswith("puma"):
                    gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio").set_crs(3857, allow_override=True)
                    gdf = gdf[["GEOID10", "NAMELSAD10", "geometry"]].copy()
                    gdf = gdf.rename(columns={"GEOID10": "puma_id", "NAMELSAD10": "name"})
                    puma_df = pd.concat([puma_df, gdf], ignore_index=True, verify_integrity=True)
            puma_df.to_file("data/interim/pumas.gpkg", driver="GPKG")
        if self.debug:
            print("\033[0;36mINFO: \033[0m" + "Finished processing pumas")

    def process_acs(self):
        empty_df = [
            pl.Series("year", [], dtype=pl.Int64),
            pl.Series("state", [], dtype=pl.Int64),
            pl.Series("PUMA", [], dtype=pl.Int64),
            pl.Series("PWGTP", [], dtype=pl.Int64),
            pl.Series("total_time", [], dtype=pl.Int64),
            pl.Series("car", [], dtype=pl.Int64),
            pl.Series("bus", [], dtype=pl.Int64),
            pl.Series("streetcar", [], dtype=pl.Int64),
            pl.Series("subway", [], dtype=pl.Int64),
            pl.Series("railroad", [], dtype=pl.Int64),
            pl.Series("ferry", [], dtype=pl.Int64),
            pl.Series("taxi", [], dtype=pl.Int64),
            pl.Series("motorcycle", [], dtype=pl.Int64),
            pl.Series("bicycle", [], dtype=pl.Int64),
            pl.Series("walking", [], dtype=pl.Int64),
            pl.Series("home", [], dtype=pl.Int64),
            pl.Series("other", [], dtype=pl.Int64),
            pl.Series("HINCP", [], dtype=pl.Float64),
            pl.Series("avg_time", [], dtype=pl.Float64),
            pl.Series("sex", [], dtype=pl.Int32),
            pl.Series("race", [], dtype=pl.String),
        ]
        acs = pl.DataFrame(empty_df).clear()

        if not os.path.exists("data/processed/acs.parquet"):
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
                            df = df.with_columns(pl.when(pl.col("JWTR") == 1).then(pl.col("PWGTP")).otherwise(0).alias("car"),
                                                 pl.when(pl.col("JWTR") == 2).then(pl.col("PWGTP")).otherwise(0).alias("bus"),
                                                 pl.when(pl.col("JWTR") == 3).then(pl.col("PWGTP")).otherwise(0).alias("streetcar"),
                                                 pl.when(pl.col("JWTR") == 4).then(pl.col("PWGTP")).otherwise(0).alias("subway"),
                                                 pl.when(pl.col("JWTR") == 5).then(pl.col("PWGTP")).otherwise(0).alias("railroad"),
                                                 pl.when(pl.col("JWTR") == 6).then(pl.col("PWGTP")).otherwise(0).alias("ferry"),
                                                 pl.when(pl.col("JWTR") == 7).then(pl.col("PWGTP")).otherwise(0).alias("taxi"),
                                                 pl.when(pl.col("JWTR") == 8).then(pl.col("PWGTP")).otherwise(0).alias("motorcycle"),
                                                 pl.when(pl.col("JWTR") == 9).then(pl.col("PWGTP")).otherwise(0).alias("bicycle"),
                                                 pl.when(pl.col("JWTR") == 10).then(pl.col("PWGTP")).otherwise(0).alias("walking"),
                                                 pl.when(pl.col("JWTR") == 11).then(pl.col("PWGTP")).otherwise(0).alias("home"),
                                                 pl.when(pl.col("JWTR") == 12).then(pl.col("PWGTP")).otherwise(0).alias("other"))
                            
                            df = df.with_columns(total_time=(pl.col("PWGTP") * pl.col("JWMNP")))
                            year = df.select(pl.col("year")).unique().item()
                            df = df.group_by("year", "state", "PUMA").agg(
                                                                        pl.col("PWGTP", "total_time", "car", "bus", "streetcar", "subway", "railroad", "ferry", "taxi", "motorcycle", "bicycle", "walking", "home", "other").sum(),
                                                                        (cpi.inflate(pl.col("HINCP"), year)).median())
                            df = df.with_columns("year","state", "PUMA", "PWGTP",
                                        (pl.col("total_time") / pl.col("PWGTP")).alias("avg_time"))
                            df = df.with_columns(
                                                sex=pl.lit(sex),
                                                race=pl.lit(race),
                            )
                            acs = pl.concat([acs, df], how="vertical")
            acs.write_parquet("data/processed/acs.parquet")
            if self.debug:
                print("\033[0;36mINFO: \033[0m" + "Finished processing acs")

    def process_roads(self):

        empty_df = [
                    pl.Series("year", [], dtype=pl.Int64),
                    pl.Series("state_id", [], dtype=pl.String),
                    pl.Series("puma_id", [], dtype=pl.String),
                    pl.Series("length", [], dtype=pl.Float64),
                    ]
        master_df = pl.DataFrame(empty_df).clear()

        if not os.path.exists("data/processed/roads.parquet"):
            for year in range(2012, 2020):

                for state in self.codes.select(pl.col("fips")).to_series().to_list():

                    roads_df = gpd.GeoDataFrame(columns=['linear_id', 'year', 'geometry'])
                    for file in os.listdir("data/shape_files/"):
                        if file.startswith(f"roads_{year}_{str(state).zfill(2)}"):
                            gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio")
                            gdf.rename(columns={"LINEARID": "linear_id"}, inplace=True)
                            gdf[["county_id", "year"]] = "01063", year
                            gdf = gdf[["year", "linear_id", "county_id", "geometry"]].set_crs(3857, allow_override=True)

                            roads_df = pd.concat([roads_df, gdf], ignore_index=True) 
                            if self.debug:
                                print("\033[0;36mREAD: \033[0m" + f"Finished processing roads for {file}")

                    tmp = self.process_length(roads_df, state, year, empty_df)
                    master_df = pl.concat([master_df, tmp], how="vertical")
                    print("\033[0;35mMERGE STATE: \033[0m" + f"Finished processing roads for {state}")

            master_df.write_parquet("data/processed/roads.parquet")

    def process_length(self, roads, state_id, year, empty_df):
        df = pl.DataFrame(empty_df)
        pumas = self.pumas[self.pumas["puma_id"].str.startswith(str(state_id).zfill(2))]
        
        def process_puma(puma_id):
            tmp = pumas.loc[pumas["puma_id"] == puma_id]
            clipped = roads.clip(tmp['geometry'])
            leng = pl.DataFrame({
                "year": year,
                "state_id": str(state_id).zfill(2),
                "puma_id": puma_id,
                "length": clipped.length.sum()
            })
            print("\033[0;35mROAD LENGTH: \033[0m" + f"Finished processing roads for {puma_id}")
            return leng
        
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_puma, puma_id) for puma_id in pumas["puma_id"]]
            
            for future in as_completed(futures):
                leng = future.result()
                df = pl.concat([df, leng], how="vertical")
        
        return df

if __name__ == "__main__":
    DataProcess()
