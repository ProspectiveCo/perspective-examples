import pandas as pd
import pro_capital_markets.constants as constants
import pro_capital_markets.market as market
import pro_capital_markets.blotter as blotter



def main():
    print("\n*** Pro Capital Markets ***\n")
    
    print("Symbols: ", constants.UNIQUE_SYMBOLS)

    print("\nFetching historical data...\n")
    # market.fetch_one_symbol("AAPL")
    # market.fetch_all_symbols()
    historical_df = pd.read_parquet(constants.MARKET_FILE)
    print("Historical DataFrame:\n", historical_df.head(n=10))

    print("\nGenerating blotter...\n")
    blotter.run_generate_blotter()
    blotter_df = pd.read_parquet(constants.BLOTTER_FILE)
    print("\nBlotter DataFrame:\n", blotter_df.head(n=10))

    print("\nConverting blotter to CSV...\n")
    blotter_df.to_csv(constants.BLOTTER_FILE.with_suffix('.csv'), index=False)
    
    # dumping events to CSV
    market.dump_events()


if __name__ == "__main__":
    main()
