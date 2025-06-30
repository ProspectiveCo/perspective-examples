"""
Partitions for capital markets data files by symbol, year, and sector, etc.\
Creating part files written in a way that allows for easy retrieval and analysis of financial data.\
"""
import pro_capital_markets.constants as constants
from pathlib import Path
import pandas as pd


def partition_market_data_by_symbol(market_df: pd.DataFrame):
    """
    Partition market data by symbol and save to output directory.
    """
    symbols = constants.UNIQUE_SYMBOLS
    print("\nPartitioning market by symbols...\n")
    dir = constants.MARKET_BY_SYMBOLS_DIR
    if not dir.exists():
        dir.mkdir(parents=True, exist_ok=True)
    for symbol in symbols:
        file_name = dir / f"part-{symbol}.parquet"
        print(f"Writing {file_name}... ", end="", flush=True)
        part_df = market_df[market_df['symbol'] == symbol]
        part_df.sort_values(by=['event_ts'], inplace=True, ignore_index=True)
        part_df.to_parquet(file_name, index=False)
        print(f"  DONE: {len(part_df)} rows", flush=True)


def partition_market_data_by_year(market_df: pd.DataFrame):
    """
    Partition market data by year and save to output directory.
    """
    years = sorted(market_df['date'].dt.year.unique())
    print("\nPartitioning market by year...\n")
    dir = constants.MARKET_BY_YEAR_DIR
    if not dir.exists():
        dir.mkdir(parents=True, exist_ok=True)
    print(f"Found {len(years)} unique years: {years}")
    for year in years:
        file_path = dir / f"part-{year}.parquet"
        print(f"Writing {file_path}... ", end="", flush=True)
        part_df = market_df[market_df['date'].dt.year == year]
        part_df.sort_values(by=['event_ts', 'symbol'], inplace=True, ignore_index=True)
        part_df.to_parquet(file_path, index=False)
        print(f"  DONE: {len(part_df)} rows", flush=True)


def partition_market_data_by_sector(market_df):
    """
    Partition market data by sector and save to output directory.
    """
    sectors = sorted(constants.SYMBOLS_BY_SECTOR.keys())
    print("\nPartitioning market by sector...\n")
    dir = constants.MARKET_BY_SECTOR_DIR
    if not dir.exists():
        dir.mkdir(parents=True, exist_ok=True)
    print(f"Found {len(sectors)} unique sectors: {sectors}")
    for sector in sectors:
        file_path = dir / f"part-{sector}.parquet"
        print(f"Writing {file_path}... ", end="", flush=True)
        part_df = market_df[market_df['sector'] == sector]
        part_df.sort_values(by=['event_ts', 'symbol'], inplace=True, ignore_index=True)
        part_df.to_parquet(file_path, index=False)
        print(f"  DONE: {len(part_df)} rows", flush=True)


def partition_market_data(market_df: pd.DataFrame):
    """
    Partition market data into different directories based on symbol, year, and sector.
    """
    # Partition by symbol
    partition_market_data_by_symbol(market_df)
    # Partition by year
    partition_market_data_by_year(market_df)
    # Partition by sector
    partition_market_data_by_sector(market_df)


def partition_blotter_data_by_symbol(blotter_df: pd.DataFrame):
    """
    Partition blotter data by symbol and save to output directory.
    """
    symbols = constants.UNIQUE_SYMBOLS
    print("\nPartitioning blotter by symbols...\n")
    dir = constants.BLOTTER_BY_SYMBOL_DIR
    if not dir.exists():
        dir.mkdir(parents=True, exist_ok=True)
    for symbol in symbols:
        file_name = dir / f"part-{symbol}.parquet"
        print(f"Writing {file_name}... ", end="", flush=True)
        part_df = blotter_df[blotter_df['symbol'] == symbol]
        part_df.sort_values(by=['event_ts'], inplace=True, ignore_index=True)
        part_df.to_parquet(file_name, index=False)
        print(f"  DONE: {len(part_df)} rows", flush=True)


def partition_blotter_data_by_year(blotter_df: pd.DataFrame):
    """
    Partition blotter data by year and save to output directory.
    """
    years = sorted(blotter_df['date'].dt.year.unique())
    print("\nPartitioning blotter by year...\n")
    dir = constants.BLOTTER_BY_YEAR_DIR
    if not dir.exists():
        dir.mkdir(parents=True, exist_ok=True)
    print(f"Found {len(years)} unique years: {years}")
    for year in years:
        file_path = dir / f"part-{year}.parquet"
        print(f"Writing {file_path}... ", end="", flush=True)
        part_df = blotter_df[blotter_df['date'].dt.year == year]
        part_df.sort_values(by=['event_ts', 'symbol'], inplace=True, ignore_index=True)
        part_df.to_parquet(file_path, index=False)
        print(f"  DONE: {len(part_df)} rows", flush=True)


def partition_blotter_data_by_sector(blotter_df: pd.DataFrame):
    """
    Partition blotter data by sector and save to output directory.
    """
    sectors = sorted(constants.SYMBOLS_BY_SECTOR.keys())
    print("\nPartitioning blotter by sector...\n")
    dir = constants.BLOTTER_BY_SECTOR_DIR
    if not dir.exists():
        dir.mkdir(parents=True, exist_ok=True)
    print(f"Found {len(sectors)} unique sectors: {sectors}")
    for sector in sectors:
        file_path = dir / f"part-{sector}.parquet"
        print(f"Writing {file_path}... ", end="", flush=True)
        part_df = blotter_df[blotter_df['sector'] == sector]
        part_df.sort_values(by=['event_ts', 'symbol'], inplace=True, ignore_index=True)
        part_df.to_parquet(file_path, index=False)
        print(f"  DONE: {len(part_df)} rows", flush=True)


def partition_blotter_data(blotter_df: pd.DataFrame):
    """
    Partition blotter data into different directories based on symbol, year, and sector.
    """
    # Partition by symbol
    partition_blotter_data_by_symbol(blotter_df)
    # Partition by year
    partition_blotter_data_by_year(blotter_df)
    # Partition by sector
    partition_blotter_data_by_sector(blotter_df)


def run_partitioning(market_df: pd.DataFrame, blotter_df: pd.DataFrame):
    """
    Partition both market and blotter data into different directories based on symbol, year, and sector.
    """
    print("\nPartitioning market data...\n")
    partition_market_data(market_df)
    
    print("\nPartitioning blotter data...\n")
    partition_blotter_data(blotter_df)
    
    print("\nAll data partitioned successfully.\n")

