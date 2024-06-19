
class DataGraph:
    def __init__(self, data):
        self.data = data

    def create_graph_dataset(self):

        df = self.lodes.rename({"state": "STUSPS"})
        df = df.with_columns(pl.col("STUSPS").str.to_uppercase())
        df = df.to_pandas()
        df = pd.merge(df, self.shp, on="STUSPS", how="inner")
        return gpd.GeoDataFrame(df, geometry="geometry")

    def graph(self, year):

        gdf = self.df.copy()
        gdf = gdf[gdf["year"] == year].reset_index(drop=True)
        return gdf
    