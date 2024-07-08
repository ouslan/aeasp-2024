import plotly.express as px
import geopandas as gpd
import pandas as pd
import polars as pl

class DataGraph:
    def __init__(self):
        self.puma = self.load_puma()
        self.data = self.load_data()

    def load_puma(self) -> gpd.GeoDataFrame:
        puma = gpd.read_file('data/interim/puma.gpkg', engin="pyogrio")
        puma["geo_id"] = puma["geo_id"].astype(str).str.zfill(6)
        return puma[["geo_id", "geometry"]]
    
    def load_data(self) -> gpd.GeoDataFrame:
        df = pd.read_parquet('data/processed/acs.parquet')
        #df['year'] = pd.to_datetime(df['year'], format='%Y-%m-%d')
        df = df[(df["year"] == 2019)].reset_index(drop=True)
        df = df.drop(columns=["year"]).reset_index(drop=True)
        df["geo_id"] = df["state"].astype(str).str.zfill(2) + df["PUMA"].astype(str).str.zfill(5)
        df = df.merge(self.puma, on="geo_id", how="inner")
        return gpd.GeoDataFrame(df, geometry=df["geometry"], crs=3857)

    def graph(self, state, sex, race) -> gpd.GeoDataFrame:
        gdf = self.data.copy()
        gdf = gdf[(gdf["state"] == state) & (gdf["sex"] == sex) & (gdf["race"] == race)]
        return gdf
    