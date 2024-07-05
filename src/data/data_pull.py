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

    def __init__(self, debug=False):
        
        self.debug = debug
        self.key = os.environ.get('CENSUS_API_KEY')
        self.mov = self.pull_movs()
        self.codes = self.pull_state_codes()
        self.pull_counties()
        self.county_codes  = self.pull_county_codes()
        self.pull_states()
        self.pull_blocks()
        self.pull_pumas()
        #self.pull_lodes(2006)
        #self.pull_roads()
    
    def pull_movs(self) -> pl.DataFrame:
        
        self.pull_file("https://www2.census.gov/ces/movs/movs_st_main2005.csv","data/raw/movs.csv")
        return pl.read_csv("data/raw/movs.csv", ignore_errors=True)
    
    def pull_state_codes(self) -> pl.DataFrame:
        
        if not os.path.exists("data/external/state_codes.parquet"):
            codes = self.mov.select(pl.col("state_abbr").str.to_lowercase().unique())
            codes = codes.filter(pl.col("state_abbr") != "us")
            codes = codes.join(self.mov.with_columns(pl.col("state_abbr").str.to_lowercase()), on="state_abbr", how="inner")
            codes = codes.select(pl.col("state_abbr", "fips", "state_name")).unique()
            codes.write_parquet("data/external/state_codes.parquet")
            if self.debug:
                print("\033[0;36mPROCESS: \033[0m" + f"Finished processing state_codes.parquet")
        return pl.read_parquet("data/external/state_codes.parquet")
    
    def pull_county_codes(self) -> pl.DataFrame:
        
        if not os.path.exists("data/external/county_codes.parquet"):
            codes = gpd.read_file("data/shape_files/counties.zip", engine="pyogrio")
            codes["county_id"] = codes["STATEFP"] + codes["COUNTYFP"]
            code = self.codes.select(pl.col("fips")).to_series().to_list()
            codes = codes[codes["STATEFP"].astype(int).isin(code)].reset_index().copy()
            codes = pl.from_pandas(codes[["STATEFP", "COUNTYFP", "county_id", "NAME"]]).write_parquet("data/external/county_codes.parquet")
            if self.debug:
                print("\033[0;36mPROCESS: \033[0m" + f"Finished processing county_codes.parquet")
        return pl.read_parquet("data/external/county_codes.parquet")
    
    def pull_states(self) -> None:
        
        self.pull_file("https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip", "data/shape_files/states.zip")

    def pull_counties(self) -> None:
        
        self.pull_file("https://www2.census.gov/geo/tiger/TIGER2019/COUNTY/tl_2019_us_county.zip", "data/shape_files/counties.zip")

    def pull_blocks(self) -> None:
        
        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/tl_2023_{str(state).zfill(2)}_tabblock20.zip"
            file_name = f"data/shape_files/block_{name}_{str(state).zfill(2)}.zip"
            self.pull_file(url, file_name)
    
    def pull_pumas(self) -> None:
        
        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            url = f"https://www2.census.gov/geo/tiger/TIGER2019/PUMA/tl_2019_{str(state).zfill(2)}_puma10.zip"
            file_name = f"data/shape_files/puma_{name}_{str(state).zfill(2)}.zip"
            self.pull_file(url, file_name)

    def pull_roads(self) -> None:
        
        for year in range(2010, 2020):
            for county_id, county_name in self.county_codes.select(pl.col("county_id", "NAME")).rows():
                url = f"https://www2.census.gov/geo/tiger/TIGER{year}/ROADS/tl_{year}_{county_id}_roads.zip"
                file_name = f"data/shape_files/roads_{year}_{county_name}.zip"
                self.pull_file(url, file_name)

    def pull_acs(self) -> None:
        
        empty_df = [
                    pl.Series("JWMNP", [], dtype=pl.Int64),
                    pl.Series("SEX", [], dtype=pl.Int64),
                    pl.Series("ST", [], dtype=pl.Int64),
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
        
        param = 'JWMNP,SEX,ST,RACAIAN,RACASN,RACBLK,RACNUM,RACWHT,RACSOR,HISP,PWGTP,COW,PUMA'
        base = 'https://api.census.gov/data/'
        flow = '/acs/acs1/pums'
        key = os.environ.get('CENSUS_API_KEY')
        
        for year in range(2008,2009):
            
            if os.path.exists(f"data/raw/acs_{year}.parquet"):
                print("\033[0;32mINFO: \033[0m" + f"ACS data for {year} already exists")
                continue
            else:
                acs = pl.DataFrame(empty_df).clear()
                
                for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
                    url = f'{base}{year}{flow}?get={param}&for=state:{str(state).zfill(2)}&key={key}'
                    
                    try:
                        r = requests.get(url).json()
                        df = pl.DataFrame(r)
                        names = df.select(pl.col("column_0")).transpose()
                        df = df.drop("column_0").transpose()
                        df = df.rename(names.to_dicts().pop()).with_columns(year=pl.lit(year)).select(pl.col("*").cast(pl.Int64))
                        acs = pl.concat([acs, df], how="vertical")

                    except RequestException as e:
                        print("\033[1;33mWARNING:  \033[0m" + f"Could not download ACS data for {name} {year} {e}")
                        continue
                
                acs.write_parquet(f"data/raw/acs_{year}.parquet")
            
            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"Finished downloading ACS data for {year}")
    
    def pull_file(self, url, filename) -> None:
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
                    print("\033[1;33mWARNING:  \033[0m" + f"Could not download {filename}")

if __name__ == "__main__":
    DataPull(debug=True)
