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
        puma["GEOID10"] = puma["GEOID10"].astype(str).str.zfill(6)
        return puma[["GEOID10", "geometry"]]
    
    def load_data(self) -> gpd.GeoDataFrame:
        df = pd.read_parquet('data/processed/acs.parquet')
        df['year'] = pd.to_datetime(df['year'], format='%Y-%m-%d')
        df = df[(df["year"] == "2019-01-01")].reset_index(drop=True)
        df = df.drop(columns=["year"]).reset_index(drop=True)
        df["GEOID10"] = df["state"].astype(str).str.zfill(2) + df["PUMA"].astype(str).str.zfill(5)
        df = df.merge(self.puma, on="GEOID10", how="inner")
        return gpd.GeoDataFrame(df, geometry=df["geometry"], crs=3857)

    def graph(self, state, sex, race) -> gpd.GeoDataFrame:
        gdf = self.data.copy()
        gdf = gdf[(gdf["state"] == state) & (gdf["sex"] == sex) & (gdf["race"] == race)]
        return gdf
    