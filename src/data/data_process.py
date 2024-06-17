import os
import numpy as np
import geopandas as gpd
import polars as pl
import pandas as pd
from urllib.request import urlretrieve

class DataClean:
    """
    A class to clean and process data for various datasets including MOV, shape files, 
    state codes, census blocks, and LODES data.

    Attributes:
    ----------
    mov_file_url : str
        URL for the MOV file.
    mov_file_path : str
        Path where the MOV file is stored.
    shape_file_url : str
        URL for the shape file.
    shape_file_path : str
        Path where the shape file is stored.
    state_code_file_path : str
        Path where the state code parquet file is stored.
    blocks_file_path : str
        Path where the blocks parquet file is stored.
    lodes_file_path : str
        Path where the LODES parquet file is stored.
    mov : pl.DataFrame
        DataFrame containing MOV data.
    shp : gpd.GeoDataFrame
        GeoDataFrame containing shape data.
    codes : pl.DataFrame
        DataFrame containing state codes.
    blocks : pl.DataFrame
        DataFrame containing blocks data.
    lodes : pl.DataFrame
        DataFrame containing LODES data.
    df : gpd.GeoDataFrame
        GeoDataFrame containing merged LODES and shape data.
    """

    def __init__(self):
        """
        Initializes the DataClean object by loading or downloading necessary data files.
        
        Parameters:
        ----------
        url : str
            URL for the MOV file.
        file_name : str
            Path where the MOV file is stored.
        """
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
    
    def load_mov_data(self):
        """
        Loads the MOV data from a file or downloads it if not available.

        Returns:
        -------
        pl.DataFrame
            DataFrame containing MOV data.
        """
        if not os.path.exists(self.mov_file_path):
            self.retrieve_file(self.mov_file_url, self.mov_file_path)
        return pl.read_csv(self.mov_file_path, ignore_errors=True)
    
    def load_shape_data(self):
        """
        Loads the shape data from a file or downloads it if not available.

        Returns:
        -------
        gpd.GeoDataFrame
            GeoDataFrame containing shape data.
        """
        if not os.path.exists(self.shape_file_path):
            self.retrieve_file(self.shape_file_url, self.shape_file_path)
        shp = gpd.read_file(self.shape_file_path, engine="pyogrio")
        shp.rename({"GEOID": "state", "NAME": "state_name"}, axis=1, inplace=True)
        shp["state"] = shp["state"].astype(int)
        return shp
    
    def load_state_codes(self):
        """
        Loads the state codes from a file or generates and saves them if not available.

        Returns:
        -------
        pl.DataFrame
            DataFrame containing state codes.
        """
        if not os.path.exists(self.state_code_file_path):
            codes = self.mov.select(pl.col("state_abbr").str.to_lowercase().unique())
            codes = codes.filter(pl.col("state_abbr") != "us")
            codes = codes.join(self.mov.with_columns(pl.col("state_abbr").str.to_lowercase()), on="state_abbr", how="inner")
            codes = codes.select(pl.col("state_abbr", "fips", "state_name")).unique()
            codes.write_parquet(self.state_code_file_path)
        else:
            codes = pl.read_parquet(self.state_code_file_path)
        return codes
    
    def load_blocks_data(self):
        """
        Loads the blocks data from a file or generates and saves them if not available.

        Returns:
        -------
        pl.DataFrame
            DataFrame containing blocks data.
        """
        if not os.path.exists(self.blocks_file_path):
            empty_df = [
                pl.Series("STATEFP20", [], dtype=pl.String),
                pl.Series("GEOID20", [], dtype=pl.String),
                pl.Series("lon", [], dtype=pl.Float64),
                pl.Series("lat", [], dtype=pl.Float64),
            ]
            blocks = pl.DataFrame(empty_df).clear()
            self.retrieve_shps(blocks)
        else:
            blocks = pl.read_parquet(self.blocks_file_path).unique()
        return blocks
    
    def load_lodes_data(self):
        """
        Loads the LODES data from a file or generates and saves them if not available.

        Returns:
        -------
        pl.DataFrame
            DataFrame containing LODES data.
        """
        if not os.path.exists(self.lodes_file_path):
            empty_df = [
                pl.Series("state", [], dtype=pl.String),
                pl.Series("fips", [], dtype=pl.String),
                pl.Series("state_abbr", [], dtype=pl.String),
                pl.Series("year", [], dtype=pl.Int64),
                pl.Series("avg_distance", [], dtype=pl.Float64),
            ]
            lodes = pl.DataFrame(empty_df).clear()
            self.retrieve_lodes(lodes)
        else:
            lodes = pl.read_parquet(self.lodes_file_path)
        return lodes
    
    def retrieve_shps(self, blocks):
        """
        Retrieves shapefiles for each state, processes them, and appends the data to the blocks DataFrame.

        Parameters:
        ----------
        blocks : pl.DataFrame
            DataFrame to store blocks data.
        """
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
    
    def retrieve_lodes(self, lodes):
        """
        Retrieves LODES data for each state and year, processes them, and appends the data to the lodes DataFrame.

        Parameters:
        ----------
        lodes : pl.DataFrame
            DataFrame to store LODES data.
        """
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
    
    def retrieve_file(self, url, filename):
        """
        Downloads a file from a URL if it doesn't already exist.

        Parameters:
        ----------
        url : str
            URL of the file to download.
        filename : str
            Path where the file will be saved.
        """
        if not os.path.exists(filename):
            urlretrieve(url, filename)

    def process_lodes(self, path):
        """
        Processes a LODES file to calculate the average distance.

        Parameters:
        ----------
        path : str
            Path to the LODES file.

        Returns:
        -------
        float
            The average distance from the processed LODES data.
        """
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
    
    def create_graph_dataset(self):
        """
        Creates a merged dataset from LODES and shape data for graphing purposes.

        Returns:
        -------
        gpd.GeoDataFrame
            GeoDataFrame containing merged LODES and shape data.
        """
        df = self.lodes.rename({"state": "STUSPS"})
        df = df.with_columns(pl.col("STUSPS").str.to_uppercase())
        df = df.to_pandas()
        df = pd.merge(df, self.shp, on="STUSPS", how="inner")
        return gpd.GeoDataFrame(df, geometry="geometry")
    
    def graph(self, year):
        """
        Returns a GeoDataFrame filtered by year for graphing.

        Parameters:
        ----------
        year : int
            Year to filter the dataset by.

        Returns:
        -------
        gpd.GeoDataFrame
            Filtered GeoDataFrame for the specified year.
        """
        gdf = self.df.copy()
        gdf = gdf[gdf["year"] == year].reset_index(drop=True)
        return gdf