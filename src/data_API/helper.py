import pandas as pd
from typing import List


class DBPaths:
    """This class provides the file paths of the respective databases and the list of symbols to pull."""

    index_db_path = r"/home/cheesecake/Downloads/Data/index/index_data.db"
    futures_db_path = r"/home/cheesecake/Downloads/Data/futures/futures_data.db"
    stocks_db_path = r"/home/cheesecake/Downloads/Data/stocks/stocks_data.db"

    @staticmethod
    def get_index_symbols() -> List[str]:
        df = pd.read_csv(r"/home/cheesecake/Downloads/fyers/utils/nse_index.csv")
        return df["Ticker"].to_list()

    @staticmethod
    def get_futures_symbols() -> List[str]:
        df = pd.read_csv(
            r"/home/cheesecake/Downloads/fyers/utils/nse_fyers_futures.csv"
        )
        return df["Ticker"].to_list()

    @staticmethod
    def get_stocks_symbols() -> List[str]:
        df = pd.read_csv(r"/home/cheesecake/Downloads/fyers/utils/nse_500_stocks.csv")
        return df["Ticker"].to_list()


if __name__ == "__main__":
    print(DBPaths.get_index_symbols())
    # print(DBPaths.get_futures_symbols())
    # print(DBPaths.get_stocks_symbols())
    print(DBPaths.futures_db_path)
    print(DBPaths.stocks_db_path)
    print(DBPaths.index_db_path)
