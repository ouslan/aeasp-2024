from urllib.request import urlretrieve
import os


class DataPull:
    def __init__(slef):
        self.codes = pl.read_parquet("data/external/state_code.parquet")
        self.pull_shps()
    
    def pull_shps(self, debug=False) -> None:
        for state, name in self.codes.select(pl.col("fips", "state_name")).rows():
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/tl_2023_{str(state).zfill(2)}_tabblock20.zip"
            file_name = f"data/shape_files/{name}_{str(state).zfill(2)}.zip"
            self.retrieve_file(url, file_name)
            if debug:
                print(f"Finished processing {state}")
    def pull_movs(self) -> None:
        self.pull_file("https://www2.census.gov/ces/movs/movs_st_main2005.csv","movs.csv")

    def pull_pumas(self, debug=False) -> None:
        pass

    def pull_lodes(self, lodes, years, debug=False) -> None:

        for state, name, fips in self.codes.select(pl.col("state_abbr", "state_name", "fips")).rows():
            for year in range(2005, 2020):
                url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/od/{state}_od_main_JT00_{year}.csv.gz"
                file_name = f"data/raw/lodes_{state}_{year}.csv.gz"
                try:
                    self.retrieve_file(url, file_name)
                except:
                    print(f"Failed to download {name}, {state}, {year}")
                    continue
                if debug:
                    print(f"Finished processing {name}, {state}, {year}")
    
    def pull_file(self, url, filename) -> None:
        if not os.path.exists(filename):
            urlretrieve(url, filename)
    
    def pull_pumas(self, years):
        pass