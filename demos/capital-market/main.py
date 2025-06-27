from pprint import pprint
import pathlib
import pandas as pd
import pro_capital_markets.constants as constants
import pro_capital_markets.fetch_historical as fetch_historical


def main():
    # print("Pro Capital Markets Metadata:")
    # pprint(metadata.SYMBOLS_BY_SECTOR)
    # print("Unique symbols: ", metadata.UNIQUE_SYMBOLS)
    # print("Unique length: ", len(metadata.UNIQUE_SYMBOLS))

    # convert full historical data to CSV
    # fetch_historical._convert_full_historical_to_csv()
    # fetch_historical._refactor_parquet_files()
    # fetch_historical.fetch_one_symbol()
    fetch_historical.dump_events()
    

def print_info():
    data_file = pathlib.Path("./data") / "historical_stock_daily.parquet"
    if not data_file.exists():
        print("No historical data found. Please fetch data first.")
        return
    df = pd.read_parquet(data_file)
    print(f"Total rows: {len(df)}")
    print(f"Unique symbols: {df['symbol'].unique()}")
    print(f"Unique sectors: {df['sector'].unique()}")
    print(f"Unique industries: {df['industry'].unique()}")
    print(f"Unique index funds: {df['index'].unique()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")


if __name__ == "__main__":
    main()
    # print_info()
