from requests.exceptions import RequestException
from urllib.request import urlretrieve
from urllib.error import URLError
from dotenv import load_dotenv
import geopandas as gpd
import polars as pl
import requests
import os

load_dotenv()

class DataPull:
    """
    A class to pull various datasets and save them to specified files.

    Parameters
    ----------
    debug : bool, optional
        If True, enables debug messages. The default is False.
    """

    def __init__(self, debug=False):
        """
        Initializes the DataPull class and pulls various datasets.
        """
        self.debug = debug
        self.key = os.environ.get('CENSUS_API_KEY')
        self.mov = self.pull_movs()
        self.codes = self.pull_state_codes()
        self.pull_counties()
        self.county_codes = self.pull_county_codes()
        self.pull_states()
        # self.pull_blocks()
        self.pull_pumas()
        self.pull_roads()
        self.pull_acs()

    def pull_movs(self) -> pl.DataFrame:
        """
        Pulls MOVs data from the Census Bureau and returns it as a DataFrame.

        Returns
        -------
        pl.DataFrame
            A DataFrame containing MOVs data.
        """
        self.pull_file("https://www2.census.gov/ces/movs/movs_st_main2005.csv", "data/raw/movs.csv")
        return pl.read_csv("data/raw/movs.csv", ignore_errors=True)

    def pull_state_codes(self) -> pl.DataFrame:
        """
        Pulls state codes and saves them to a parquet file.

        Returns
        -------
        pl.DataFrame
            A DataFrame containing state codes.
        """
        if not os.path.exists("data/external/state_codes.parquet"):
            codes = self.mov.select(pl.col("state_abbr").str.to_lowercase().unique())
            codes = codes.filter(pl.col("state_abbr") != "us")
            codes = codes.join(self.mov.with_columns(pl.col("state_abbr").str.to_lowercase()), on="state_abbr", how="inner")
            codes = codes.select(pl.col("state_abbr", "fips", "state_name")).unique()
            codes.write_parquet("data/external/state_codes.parquet")
            if self.debug:
                print("\033[0;36mPROCESS: \033[0m" + "Finished processing state_codes.parquet")
        return pl.read_parquet("data/external/state_codes.parquet")

    def pull_county_codes(self) -> pl.DataFrame:
        """
        Pulls county codes from shapefiles and saves them to a parquet file.

        Returns
        -------
        pl.DataFrame
            A DataFrame containing county codes.
        """
        if not os.path.exists("data/external/county_codes.parquet"):
            codes = gpd.read_file("data/shape_files/counties.zip", engine="pyogrio")
            codes["county_id"] = codes["STATEFP"] + codes["COUNTYFP"]
            code = self.codes.select(pl.col("fips")).to_series().to_list()
            codes = codes[codes["STATEFP"].astype(int).isin(code)].reset_index().copy()
            codes[["STATEFP", "COUNTYFP", "county_id", "NAME"]].to_parquet("data/external/county_codes.parquet")
            if self.debug:
                print("\033[0;36mPROCESS: \033[0m" + "Finished processing county_codes.parquet")
        return pl.read_parquet("data/external/county_codes.parquet")

    def pull_states(self) -> None:
        """
        Pulls state shapefiles from the Census Bureau and saves them locally.
        """
        self.pull_file("https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_state_500k.zip", "data/shape_files/states.zip")

    def pull_counties(self) -> None:
        """
        Pulls county shapefiles from the Census Bureau and saves them locally.
        """
        self.pull_file("https://www2.census.gov/geo/tiger/TIGER2017/COUNTY/tl_2017_us_county.zip", "data/shape_files/counties.zip")

    def pull_blocks(self) -> None:
        """
        Pulls block shapefiles for each state from the Census Bureau and saves them locally.
        """
        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/tl_2023_{str(state).zfill(2)}_tabblock20.zip"
            file_name = f"data/shape_files/block_{name}_{str(state).zfill(2)}.zip"
            self.pull_file(url, file_name)

    def pull_pumas(self) -> None:
        """
        Pulls PUMA shapefiles for each state from the Census Bureau and saves them locally.
        """
        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            url = f"https://www2.census.gov/geo/tiger/TIGER2019/PUMA/tl_2019_{str(state).zfill(2)}_puma10.zip"
            file_name = f"data/shape_files/puma_{name}_{str(state).zfill(2)}.zip"
            self.pull_file(url, file_name)

    def pull_roads(self) -> None:
        """
        Pulls road shapefiles for each county and year from the Census Bureau and saves them locally.
        """
        for year in range(2012, 2020):
            for county_id, county_name in self.county_codes.select(pl.col("county_id", "NAME")).rows():
                url = f"https://www2.census.gov/geo/tiger/TIGER{year}/ROADS/tl_{year}_{county_id}_roads.zip"
                file_name = f"data/shape_files/roads_{year}_{county_id}.zip"
                self.pull_file(url, file_name)

    def pull_acs(self) -> None:
        """
        Pulls ACS data for each state and year from the Census API and saves it to a parquet file.
        """
        empty_df = [
            pl.Series("JWMNP", [], dtype=pl.Int64),
            pl.Series("SEX", [], dtype=pl.Int64),
            pl.Series("ST", [], dtype=pl.Int64),
            pl.Series("ADJHSG", [], dtype=pl.String),
            pl.Series("ADJINC", [], dtype=pl.String),
            pl.Series("AGEP", [], dtype=pl.Int64),
            pl.Series("CIT", [], dtype=pl.Int64),
            pl.Series("JWTR", [], dtype=pl.Int64),  # Check
            pl.Series("JWRIP", [], dtype=pl.Int64),
            pl.Series("OC", [], dtype=pl.Int64),
            pl.Series("HINCP", [], dtype=pl.Int64),
            pl.Series("RACAIAN", [], dtype=pl.Int64),
            pl.Series("RACASN", [], dtype=pl.Int64),
            pl.Series("RACBLK", [], dtype=pl.Int64),
            pl.Series("RACNUM", [], dtype=pl.Int64),
            pl.Series("RACWHT", [], dtype=pl.Int64),
            pl.Series("RACSOR", [], dtype=pl.Int64),
            pl.Series("HISP", [], dtype=pl.Int64),
            pl.Series("PWGTP", [], dtype=pl.Int64),
            pl.Series("COW", [], dtype=pl.Int64),
            pl.Series("PUMA", [], dtype=pl.Int64),
            pl.Series("state", [], dtype=pl.Int64),
            pl.Series("year", [], dtype=pl.Int64),
        ]

        param = 'JWMNP,SEX,ST,ADJHSG,ADJINC,AGEP,CIT,JWTR,JWRIP,OC,HINCP,RACAIAN,RACASN,RACBLK,RACNUM,RACWHT,RACSOR,HISP,PWGTP,COW,PUMA'
        base = 'https://api.census.gov/data/'
        flow = '/acs/acs1/pums'

        for year in range(2012, 2020):
            if os.path.exists(f"data/raw/acs_{year}.parquet"):
                print("\033[0;32mINFO: \033[0m" + f"ACS data for {year} already exists")
                continue

            acs = pl.DataFrame(empty_df).clear()
            if year == 2019:
                param = param.replace("JWTR", "JWTRNS")

            for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
                url = f'{base}{year}{flow}?get={param}&for=state:{str(state).zfill(2)}&key={self.key}'

                try:
                    r = requests.get(url).json()
                    df = pl.DataFrame(r)
                    names = df.select(pl.col("column_0")).transpose()
                    df = df.drop("column_0").transpose()
                    df = df.rename(names.to_dicts().pop()).with_columns(year=pl.lit(year))
                    df = df.with_columns(pl.col("*").exclude("ADJHSG", "ADJINC").cast(pl.Int64))
                    if year == 2019:
                        df = df.rename({"JWTRNS": "JWTR"})
                    acs = pl.concat([acs, df], how="vertical")
                    print("\033[0;32mINFO: \033[0m" + f"Downloaded ACS data for {name} {year}")

                except RequestException as e:
                    print("\033[1;33mWARNING: \033[0m" + f"Could not download ACS data for {name} {year}: {e}")
                    continue

            acs.write_parquet(f"data/raw/acs_{year}.parquet")

            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"Finished downloading ACS data for {year}")

    def pull_file(self, url: str, filename: str) -> None:
        """
        Downloads a file from a given URL and saves it locally.

        Parameters
        ----------
        url : str
            The URL of the file to download.
        filename : str
            The path to save the downloaded file.
        """
        if os.path.exists(filename):
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File {filename} already exists, skipping download")
        else:
            try:
                urlretrieve(url, filename)
                if self.debug:
                    print("\033[0;32mINFO: \033[0m" + f"Downloaded {filename}")
            except URLError:
                if self.debug:
                    print("\033[1;33mWARNING: \033[0m" + f"Could not download {filename}")

if __name__ == "__main__":
    DataPull(debug=True)
