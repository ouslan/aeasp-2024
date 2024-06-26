from urllib.request import urlretrieve
import polars as pl
import os

class DataPull:
    def __init__(self, debug=True):
        self.debug = debug
        self.mov = self.pull_movs()
        self.codes = self.pull_codes()
        self.pull_states()
        self.pull_blocks()
        self.pull_pumas()
        self.pull_lodes(2006)
    
    def pull_movs(self) -> pl.DataFrame:
        self.pull_file("https://www2.census.gov/ces/movs/movs_st_main2005.csv","data/raw/movs.csv")
        if self.debug:
            print("\033[0;32mINFO: \033[0m" + f"Finished downloading movs.csv")
        return pl.read_csv("data/raw/movs.csv", ignore_errors=True)
    
    def pull_codes(self) -> pl.DataFrame:
        if not os.path.exists("data/external/state_code.parquet"):
            codes = self.mov.select(pl.col("state_abbr").str.to_lowercase().unique())
            codes = codes.filter(pl.col("state_abbr") != "us")
            codes = codes.join(self.mov.with_columns(pl.col("state_abbr").str.to_lowercase()), on="state_abbr", how="inner")
            codes = codes.select(pl.col("state_abbr", "fips", "state_name")).unique()
            codes.write_parquet("data/external/state_code.parquet")
            if self.debug:
                print("\033[0;36mPROCESS: \033[0m" + f"Finished processing state_code.parquet")
        return pl.read_parquet("data/external/state_code.parquet")
    
    def pull_states(self) -> None:
        self.pull_file("https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip", "data/shape_files/states.zip")
        if self.debug:
            print("\033[0;32mINFO: \033[0m" + f"Finished downloading states.zip")

    def pull_blocks(self) -> None:
        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/tl_2023_{str(state).zfill(2)}_tabblock20.zip"
            file_name = f"data/shape_files/block_{name}_{str(state).zfill(2)}.zip"
            self.pull_file(url, file_name)
            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"Finished downloading block_{name}.zip")
    
    def pull_pumas(self) -> None:
        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/PUMA/tl_2023_{str(state).zfill(2)}_puma20.zip"
            file_name = f"data/shape_files/puma_{name}_{str(state).zfill(2)}.zip"
            self.pull_file(url, file_name)
            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"Finished downloading puma_{name}.zip")

    def pull_lodes(self, start_years:int) -> None:
        for state, name, fips in self.codes.select(pl.col("state_abbr", "state_name", "fips")).rows():
            for year in range(start_years, 2020):
                url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/od/{state}_od_main_JT00_{year}.csv.gz"
                file_name = f"data/raw/lodes_{state}_{year}.csv.gz"
                try:
                    self.pull_file(url, file_name)
                except:
                    print("\033[1;33mWARNING:  \033[0m" + f"Could not download lodes file for {state} {year}")
                    continue
                if self.debug:
                    print("\033[0;32mINFO: \033[0m" + f"Finished downloading {state}_{year}.csv.gz")
    
    def pull_file(self, url, filename) -> None:
        if not os.path.exists(filename):
            urlretrieve(url, filename)

if __name__ == "__main__":
    DataPull(debug=True)
