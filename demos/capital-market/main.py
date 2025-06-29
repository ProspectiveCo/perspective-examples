from pprint import pprint
import pathlib
import pandas as pd
import pro_capital_markets.constants as constants
import pro_capital_markets.fetch_historical as hist


def main():
    # print("Pro Capital Markets Metadata:")
    # pprint(metadata.SYMBOLS_BY_SECTOR)
    # print("Unique symbols: ", metadata.UNIQUE_SYMBOLS)
    # print("Unique length: ", len(metadata.UNIQUE_SYMBOLS))

    # hist.fetch_one_symbol("AAPL")
    # hist.fetch_all_symbols()
    historical_df = pd.read_parquet(constants.HISTORICAL_FILE)
    print(historical_df.head())

    # convert full historical data to CSV
    # hist._convert_full_historical_to_csv()
    # hist._refactor_parquet_files()
    # hist.fetch_one_symbol()
    # hist.dump_events()
    

def print_info():
    data_file = constants.HISTORICAL_FILE
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
