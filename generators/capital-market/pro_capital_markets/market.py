import os
import asyncio
import pandas as pd
import httpx
import backoff
from pathlib import Path
import random
import pro_capital_markets.constants as constants


API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", None)
BASE_URL = "https://www.alphavantage.co/query"

assert API_KEY is not None, "Please set the ALPHA_VANTAGE_API_KEY environment variable"


@backoff.on_exception(
    backoff.expo,                                  # exponential backoff
    (httpx.RequestError, httpx.HTTPStatusError),   # which errors to catch
    max_tries=3,                                   # up to 3 attempts
    jitter=backoff.full_jitter                     # add jitter
)
async def fetch_symbol(client: httpx.AsyncClient, symbol: str) -> pd.DataFrame:
    params = {
        "function": "TIME_SERIES_DAILY",      # daily
        "symbol": symbol,
        "outputsize": "full",                 # fetch everything
        "apikey": API_KEY
    }
    print(f"Fetching {symbol}...", end='', flush=True)
    resp = await client.get(BASE_URL, params=params)
    resp.raise_for_status()                         # trigger HTTPStatusError
    data = resp.json().get("Time Series (Daily)", {})
    # convert to DataFrame
    df = pd.DataFrame.from_dict(data, orient="index")
    if df.empty:
        return df
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    # rename columns
    df = df.rename(columns={
        "1. open":   "open",
        "2. high":   "high",
        "3. low":    "low",
        "4. close":  "close",
        "5. volume": "volume"
    })
    df["symbol"] = symbol
    df["sector"] = constants.STOCK_STORIES.get(symbol, {}).get("sector", "Unknown")
    df["industry"] = constants.STOCK_STORIES.get(symbol, {}).get("industry", "Unknown")
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    df["index"] = df["symbol"].apply(lambda x: random.choice(constants.STOCK_STORIES.get(x, {}).get("index", ["Unknown"])))
    df["date"] = df.index.date
    df = df.reset_index(drop=True)
    df = df.sort_values(by=["date", "symbol"])
    # print(df.head(), flush=True)  # print first few rows for debugging
    # metadata print
    print(f"{len(df)} rows, Date Range: {df["date"].min()} to {df["date"].max()}, Average: {df["open"].mean():.2f}", flush=True)
    return df


async def _fetch_all_symbols_async(output_file: Path = constants.MARKET_FILE):
    # If file exists, read it and determine already fetched symbols
    if os.path.exists(output_file):
        existing_data = pd.read_parquet(output_file)
        already_fetched = set(existing_data["symbol"].unique())
        print(f"Found existing file with symbols: {already_fetched}")
    else:
        existing_data = pd.DataFrame()
        already_fetched = set()

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Iterate over all unique symbols
        for symbol in constants.UNIQUE_SYMBOLS:
            if symbol in already_fetched:
                print(f"Skipping {symbol} (already fetched)")
                continue
            df = await fetch_symbol(client, symbol)
            # Skip if no data returned
            if df.empty:
                continue
            # Save individual symbol data
            df.to_parquet(constants.MARKET_BY_SYMBOLS_DIR / f"part-{symbol}.parquet", index=False)
            # Immediately append new data by merging with existing and writing out
            if existing_data.empty:
                existing_data = df
            else:
                existing_data = pd.concat([existing_data, df], axis=0)
            existing_data = existing_data.sort_values(by=["date", "symbol"])
            existing_data.to_parquet(output_file, index=False)
            # existing_data.to_csv(output_file.with_suffix('.csv'), index=False)
            # Update the set of fetched symbols
            already_fetched.add(symbol)
            print(f"Appended {symbol}")
    # print full set of unique symbols fetched
    print(f"Total unique symbols fetched: {len(already_fetched)}")


def fetch_one_symbol(symbol: str = "MSFT"):
    """
    Fetch historical data for a single symbol.
    """
    async def _fetch():
        async with httpx.AsyncClient(timeout=30.0) as client:
            return await fetch_symbol(client, symbol)
    df = asyncio.run(_fetch())
    output_file = constants.MARKET_BY_SYMBOLS_DIR / f"part-{symbol}.parquet"
    df.to_parquet(output_file, index=False)
    df.to_csv(output_file.with_suffix('.csv'), index=False)
    print(df.head())


def fetch_all_symbols():
    asyncio.run(_fetch_all_symbols_async())
    

def dump_events():
    """
    Dump EVENTS table to a CSV file.
    """
    df = pd.DataFrame(constants.EVENTS)
    df.sort_values(by=["date", "symbol"], inplace=True)
    df.to_csv(constants.DATA_DIR / "events.csv", index=False)


if __name__ == "__main__":
    assert API_KEY is not None, "Please set the ALPHA_VANTAGE_API_KEY environment variable"
    fetch_all_symbols()