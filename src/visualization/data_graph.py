import plotly.express as px
import geopandas as gpd
import pandas as pd

class DataGraph:
    """
    A class to handle the loading and processing of geographic and statistical data, 
    and to generate graphs based on specific criteria.

    Attributes
    ----------
    puma : gpd.GeoDataFrame
        A GeoDataFrame containing PUMA boundaries and IDs.
    data : gpd.GeoDataFrame
        A GeoDataFrame containing ACS data merged with PUMA boundaries.
    """

    def __init__(self):
        """
        Initializes the DataGraph class by loading PUMA boundaries and ACS data.
        """
        self.puma = self.load_puma()
        self.data = self.load_data()

    def load_puma(self) -> gpd.GeoDataFrame:
        """
        Loads PUMA boundaries from a GeoPackage file and processes them.

        Returns
        -------
        gpd.GeoDataFrame
            A GeoDataFrame containing PUMA IDs and geometries.
        """
        puma = gpd.read_file('data/interim/pumas.gpkg', engine="pyogrio")
        puma["puma_id"] = puma["puma_id"].astype(str).str.zfill(6)
        return puma[["puma_id", "geometry"]].copy()
    
    def load_data(self) -> gpd.GeoDataFrame:
        """
        Loads ACS data from a parquet file, filters it for the year 2019, and merges it with PUMA boundaries.

        Returns
        -------
        gpd.GeoDataFrame
            A GeoDataFrame containing filtered ACS data with geometries.
        """
        df_roads = pd.read_parquet("data/processed/roads.parquet")
        df_acs = pd.read_parquet("data/processed/acs.parquet")
        df_acs["puma_id"] = df_acs["state"].astype(str).str.zfill(2) + df_acs["PUMA"].astype(str).str.zfill(5)
        # df['year'] = pd.to_datetime(df['year'], format='%Y-%m-%d')  # Uncomment if needed
        master_df = df_acs.merge(df_roads, on=["puma_id", "year"], how="left")
        master_df = master_df.sort_values(by=["year", "puma_id"], ascending=True).reset_index(drop=True)
        master_df["length"] = master_df["length"] / 1000
        master_df[['car', 'bus','streetcar', 'subway', 'railroad', 'ferry', 'taxi','motorcycle','bicycle', 'walking']] = master_df[['car', 'bus','streetcar', 'subway', 'railroad', 'ferry', 'taxi','motorcycle','bicycle', 'walking']] / 1000
        df = master_df.copy()
        df["puma_id"] = df["state"].astype(str).str.zfill(2) + df["PUMA"].astype(str).str.zfill(5)
        df = df.merge(self.puma, on="puma_id", how="inner")
        return gpd.GeoDataFrame(df, geometry=df["geometry"], crs=3857)

    def graph(self, state: str, sex: str, race: str) -> gpd.GeoDataFrame:
        """
        Filters the data based on state, sex, and race, and returns the filtered GeoDataFrame.

        Parameters
        ----------
        state : str
            The state abbreviation to filter the data.
        sex : str
            The sex to filter the data.
        race : str
            The race to filter the data.

        Returns
        -------
        gpd.GeoDataFrame
            A GeoDataFrame containing filtered data based on the specified criteria.
        """
        gdf = self.data.copy()
        gdf = gdf[(gdf["state"] == state) & (gdf["sex"] == sex) & (gdf["race"] == race)]
        return gdf
