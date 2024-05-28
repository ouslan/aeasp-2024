from src.data.data_pull import download_file

def main() -> None:
    url = "https://www2.census.gov/ces/movs/movs_st_main2005.csv"
    filename = "data/raw/movs_st_main2005.csv"
    download_file(url, filename)

if __name__ == "__main__":
    main()