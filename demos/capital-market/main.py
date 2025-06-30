import pandas as pd
import pro_capital_markets.constants as constants
import pro_capital_markets.market as market
import pro_capital_markets.blotter as blotter
import pro_capital_markets.partitions as partitions


def main():
    print("\n*** Pro Capital Markets ***\n")
    
    print("Symbols: ", constants.UNIQUE_SYMBOLS)

    print("\nFetching historical data...\n")
    # market.fetch_one_symbol("AAPL")
    market.fetch_all_symbols()
    market_df = pd.read_parquet(constants.MARKET_FILE)
    print("Historical DataFrame:\n", market_df.head(n=10))
    print("\nPartitioning market data...\n")
    partitions.partition_market_data(market_df)

    print("\nGenerating blotter...\n")
    blotter.run_generate_blotter()
    blotter_df = pd.read_parquet(constants.BLOTTER_FILE)
    print("\nBlotter DataFrame:\n", blotter_df.head(n=10))
    print("\nPartitioning blotter data...\n")
    partitions.partition_blotter_data(blotter_df)

    print("\nConverting blotter to CSV...\n")
    blotter_df.to_csv(constants.BLOTTER_FILE.with_suffix('.csv'), index=False)
    
    # dumping events to CSV
    market.dump_events()


if __name__ == "__main__":
    main()
