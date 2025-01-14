import pandas as pd
from dataclasses import dataclass
from typing import List, Union
import sqlite3
import datetime
from src.data_API.helper import DBPaths

"""This file is a work in progress. Still need to decide on the DB of choice for storing time series data. 
    Torn between TimeScaleDB and sqlite3.
"""


@dataclass()
class SecurityDataRetriever:
    """
    A class to retrieve security data like start datetime, end datetime, duration of candlesticks, et cetera from an SQLite database.

    Attributes:
        db_path (str): Path to the SQLite database file.

    Methods:
        _security_first_datetime(symbol: str) -> datetime.datetime:
            Retrieves the earliest datetime available for a given security symbol.

        _security_latest_datetime(symbol: str) -> datetime.datetime:
            Retrieves the most recent datetime available for a given security symbol.

        get_security_data(symbol: str, start_date: Union[int | None | str | datetime.datetime] = None) -> pd.DataFrame:
            Retrieves security data for a given symbol from a specified start date till the latest.

        get_available_symbols() -> List[str]:
            Retrieves and prints a list of all available security symbols in the database.

        delete_security(symbol: str) -> None:
            Deletes the whole table for the security symbol from the database.

        delete_security_from_date(symbol: str, from_date: Union[int | None | str | datetime.datetime] = 252) -> None:
            Deletes the data from table of the particular security from the passed date or for the last x days if type is int.
    """

    db_path: str

    def __post_init__(self):
        self.db_conn = sqlite3.connect(self.db_path)

    def _security_first_datetime(self, symbol: str) -> datetime.datetime:
        """Retrieves the earliest datetime available for a given security symbol."""
        assert symbol, print("You need to pass a symbol to get the date for.")
        cursor = self.db_conn.cursor()
        cursor.execute(f"SELECT * FROM '{symbol}' ORDER BY ROWID LIMIT 1")
        start_date = datetime.datetime.strptime(
            cursor.fetchone()[0], "%Y-%m-%d %H:%M:%S"
        )
        cursor.close()
        return start_date

    def _security_latest_datetime(self, symbol: str) -> datetime.datetime:
        """Retrieves the most recent datetime available for a given security symbol."""
        assert symbol, print("You need to pass a symbol to get the date for.")
        cursor = self.db_conn.cursor()
        cursor.execute(f"SELECT * FROM '{symbol}' ORDER BY ROWID DESC LIMIT 1")
        latest_date = datetime.datetime.strptime(
            cursor.fetchone()[0], "%Y-%m-%d %H:%M:%S"
        )
        cursor.close()
        return latest_date

    def get_security_data(
        self,
        symbol: str,
        start_date: Union[int | None | str | datetime.datetime] = None,
    ) -> pd.DataFrame:
        """
        Retrieves security data for a given symbol from a specified start date.

        If start_date is an integer, it's treated as the number of days before the latest date.
        If the start_date is a datetime.date obj, we get the data from that date onwards.
        If start_date is a str, then we convert it into an datetime.datetime object to complete our operations.
        If start_date is None, it retrieves data from the earliest available date.
        """
        assert symbol, print("You need to pass a symbol to get the data for.")
        if type(start_date) is int:
            start_date = self._security_latest_datetime(symbol) - datetime.timedelta(
                days=start_date
            )
        elif type(start_date) is datetime.datetime:
            start_date = start_date.date()
        elif type(start_date) is str:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            start_date = self._security_first_datetime(symbol)

        cursor = self.db_conn.cursor()
        query = f"""SELECT * FROM '{symbol}' WHERE 'date-time' >= '{start_date}' ORDER BY 'date-time' """
        results = pd.read_sql_query(query, self.db_conn, index_col=None)
        results["date-time"] = pd.to_datetime(
            results["date-time"], format="%Y-%m-%d %H:%M:%S"
        )
        cursor.close()
        return results

    def get_available_symbols(self) -> List[str]:
        """Retrieves and prints a list of all available security symbols in the database."""
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        symbols_in_db = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return symbols_in_db

    def delete_security(self, symbol: str) -> None:
        """Deleted the whole table for the security symbol from the database."""
        assert symbol, print("You need to pass a symbol to delete data for.")
        cursor = self.db_conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS '{symbol}'")
        self.db_conn.commit()
        print(f"We have deleted the data for symbol {symbol}.")
        cursor.close()
        return None

    def delete_security_from_date(
        self, symbol: str, from_date: Union[int | None | str | datetime.datetime] = 252
    ) -> None:
        """Deletes the data from table of the particular security from the passed date or for the last x days if type is int."""
        assert from_date, print(
            "You need to either pass an 'int', 'str' or a 'datetime.date' obj to delete data for symbol from it's table."
        )

        if type(from_date) is int:
            from_date = self._security_latest_datetime(symbol) - datetime.timedelta(
                days=from_date
            )
        elif type(from_date) is datetime.datetime:
            from_date = from_date.date()
        elif type(from_date) is str:
            from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()
        else:
            from_date = self._security_first_datetime(symbol)

        cursor = self.db_conn.cursor()
        try:
            from_date = datetime.datetime.strftime(
                from_date, format="%Y-%m-%d %H:%M-%S"
            )
            cursor.execute(
                f"""DELETE FROM '{symbol}' WHERE 'date-time'> '{from_date}' """
            )
            self.db_conn.commit()
            print(
                f"We have deleted data for symbol {symbol} from {from_date} to latest!"
            )
        except sqlite3.Error as e:
            print(f"We ran into an sqlite3 error while deleting the data: {e}")
        except Exception as e:
            print(f"We ran into a general error while deleting the data: {e}")
        finally:
            cursor.close()

    def check_db_integrity(
        self,
        year: Union[int | str] = datetime.datetime.now().year - 3,
        log_csv=False,
        delete_symbol=False,
    ) -> None:
        if type(year) is str:
            year = int(year)
        symbols = self.get_available_symbols()
        df = pd.DataFrame(columns=["symbol", "start", "end"])
        for i, symbol in enumerate(symbols):
            if self._security_first_datetime(symbol) > datetime.datetime(year, 1, 1):
                df.loc[i, "symbol"] = symbol
                df.loc[i, "start"] = self._security_first_datetime(symbol)
                df.loc[i, "end"] = self._security_latest_datetime(symbol)
                if delete_symbol:
                    self.delete_security(symbol=symbol)
        df.reset_index(drop=True, inplace=True)
        print(df)
        if log_csv and not df.shape[0]:
            df.to_csv(
                r"/home/cheesecake/Downloads/Data/data_API/"
                + self.db_path.split("/")[-1].split(".")[0]
                + ".csv"
            )

    def __del__(self):
        if hasattr(self, "db_conn"):
            self.db_conn.close()
            print("We have successfully closed the DB connection")


def main():
    # obj = SecurityDataRetriever(DBPaths().futures_db_path)
    # symbols = obj.get_available_symbols()
    # # print(len(symbols))
    # df = pd.DataFrame(columns=["symbol", "start", "end"])

    # for i, symbol in enumerate(symbols):
    #     if obj._security_first_datetime(symbol) > datetime.datetime(2023, 1, 1):
    #         df.loc[i, "symbol"] = symbol
    #         df.loc[i, "start"] = obj._security_first_datetime(symbol)
    #         df.loc[i, "end"] = obj._security_latest_datetime(symbol)
    #         # obj.delete_security(symbol)

    # print(df)
    # if df.shape[0] != 0:
    # df.to_csv(r"/home/cheesecake/Downloads/Data/data_API/futures.csv")
    obj = SecurityDataRetriever(DBPaths().index_db_path)
    obj.check_db_integrity(2022, delete_symbol=False)


if __name__ == "__main__":
    main()
