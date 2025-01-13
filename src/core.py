from sql import SecurityDataRetriever
from helper import DBPaths


def main():
    index_db_path = DBPaths().index_db_path
    index_symbols = SecurityDataRetriever(index_db_path).get_available_symbols()
    print(index_symbols)


if __name__ == "__main__":
    main()
