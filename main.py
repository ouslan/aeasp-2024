from src.data.data_process import DataProcess
from src.data.data_pull import DataPull
from src.data.data_db_dao import DAO

def main() -> None:
    #DataPull(debug=False)
    DataProcess(debug=True)
    #DAO()

if __name__ == "__main__":
    main()