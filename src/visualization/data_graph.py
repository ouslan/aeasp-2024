import geopandas as gpd
import pandas as pd
import polars as pl
class DataGraph:
    def __init__(self, data_path="data/processed/lodes.parquet"):
        self.data = pl.read_parquet(data_path)
        self.shp = self.load_shape_data()
        self.df = self.create_graph_dataset()
        self.lodes = pl.read_parquet("data/processed/lodes.parquet")

    def load_shape_data(self):
        shp = gpd.read_file("data/shape_files/states.zip", engine="pyogrio")
        shp.rename({"GEOID": "state", "NAME": "state_name"}, axis=1, inplace=True)
        shp["state"] = shp["state"].astype(int)
        return shp
    
    def create_graph_dataset(self) -> gpd.GeoDataFrame:
        df = self.data.rename({"state": "STUSPS"})
        df = df.with_columns(pl.col("STUSPS").str.to_uppercase())
        df = df.to_pandas()
        df = pd.merge(df, self.shp, on="STUSPS", how="inner")
        return gpd.GeoDataFrame(df, geometry="geometry")

    def graph(self, year) -> gpd.GeoDataFrame:
        gdf = self.df.copy()
        gdf = gdf[gdf["year"] == year].reset_index(drop=True)
        return gdf
    