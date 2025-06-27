"""
Reads actual historical stock performance data from the historical data file
and generates a daily blotter of stock trades. Each trade represents a stock
transaction (sale or purchase) for a specific stock symbol on a given date and 
contains a portion of the actual daily volume traded for that stock in the 
historical data file.


# Bloter Schema

| Column            | pandas dtype     | Short description                        |
| ----------------- | ---------------- | ---------------------------------------- |
| `trade_id`        | `int32`          | Unique execution ID (FIX Tag 1003).      |
| `trader`          | `category`       | Human or algo responsible for the order. |
| `desk`            | `category`       | Trading desk or book that owns the risk. |
| `event_ts`        | `datetime64[ns]` | Event timestamp in UTC nanoseconds.      |
| `symbol`          | `category`       | Exchange ticker symbol.                  |
| `security_name`   | `category`       | Full security name.                      |
| `sector_gics`     | `category`       | GICS sector / industry path.             |
| `side`            | `category`       | BUY or SELL.                             |
| `order_type`      | `category`       | LIMIT, MARKET, etc.                      |
| `order_qty`       | `int32`          | Requested share quantity.                |
| `order_status`    | `category`       | NEW, FILLED, etc.                        |
| `limit_price`     | `float32`        | Price cap / floor for the order.         |
| `qty`             | `int32`          | Executed quantity in this fill.          |
| `price`           | `float32`        | Execution price reported by the venue.   |
| `trade_value`     | `float32`        | Notional value of the fill.              |
| `commission`      | `float32`        | Broker commission paid.                  |
| `exec_venue`      | `category`       | Executing broker or trading venue.       |
| `venue_fee`       | `float32`        | Exchange or regulatory fee.              |
| `bid_price`       | `float32`        | Best bid at event time.                  |
| `ask_price`       | `float32`        | Best ask at event time.                  |
| `mid_price`       | `float32`        | Midpoint of bid and ask.                 |
| `spread_price`    | `float32`        | Bid-ask spread in price terms.           |
| `high_day`        | `float32`        | Session high when trade occurred.        |
| `low_day`         | `float32`        | Session low when trade occurred.         |
| `fund`            | `category`       | Investment fund receiving the trade.     |
| `benchmark_index` | `category`       | Performance benchmark index.             |

"""


import datetime as dt
from pathlib import Path
import pandas as pd
import pro_capital_markets.constants as constants
import random



# Compact capital-markets blotter schema
SCHEMA = {
    "trade_id":        {"dtype": "int32",            "description": "Unique execution ID (FIX Tag 1003)."},
    "trader":          {"dtype": "category",         "description": "Human or algo responsible for the order."},
    "desk":            {"dtype": "category",         "description": "Trading desk or book that owns the risk."},
    "event_ts":        {"dtype": "datetime64[ns]",   "description": "Event timestamp in UTC nanoseconds."},
    "symbol":          {"dtype": "category",         "description": "Exchange ticker symbol."},
    "security_name":   {"dtype": "category",         "description": "Full security name."},
    "sector_gics":     {"dtype": "category",         "description": "GICS sector / industry path."},
    "side":            {"dtype": "category",         "description": "BUY or SELL."},
    "order_type":      {"dtype": "category",         "description": "Order style (LIMIT, MARKET, etc.)."},
    "order_qty":       {"dtype": "int32",            "description": "Requested share quantity."},
    "order_status":    {"dtype": "category",         "description": "Lifecycle state (NEW, FILLED, ...)."},
    "limit_price":     {"dtype": "float32",          "description": "User-specified price cap/floor."},
    "qty":             {"dtype": "int32",            "description": "Executed quantity in this fill."},
    "price":           {"dtype": "float32",          "description": "Price at which the shares were filled."},
    "trade_value":     {"dtype": "float32",          "description": "Notional value of the fill."},
    "commission":      {"dtype": "float32",          "description": "Broker commission paid."},
    "exec_venue":      {"dtype": "category",         "description": "Executing broker or trading venue."},
    "venue_fee":       {"dtype": "float32",          "description": "Exchange/regulatory fee."},
    "bid_price":       {"dtype": "float32",          "description": "Best bid at event time."},
    "ask_price":       {"dtype": "float32",          "description": "Best ask at event time."},
    "mid_price":       {"dtype": "float32",          "description": "Midpoint of bid and ask."},
    "spread_price":    {"dtype": "float32",          "description": "Bid-ask spread in price terms."},
    "high_day":        {"dtype": "float32",          "description": "Session high when trade occurred."},
    "low_day":         {"dtype": "float32",          "description": "Session low when trade occurred."},
    "fund":            {"dtype": "category",         "description": "Investment fund receiving the trade."},
    "benchmark_index": {"dtype": "category",         "description": "Performance benchmark index."},
}


# Build an empty DataFrame with compact dtypes
df_blotter = pd.DataFrame({
    col: pd.Series(dtype=spec["dtype"])
    for col, spec in SCHEMA.items()
})

# df_blotter is now fully typed and ready for streaming rows


async def _generate_blotter_daily(historical_df: pd.DataFrame, for_date: dt.date, symbol_preferences: dict) -> pd.DataFrame:
    """
    Generate a daily blotter of stock trades for a specific date.
    
    :param historical_df: DataFrame containing historical stock data.
    :param for_date: The date for which to generate the blotter.
    :return: DataFrame containing the daily blotter.
    """
    df = historical_df[historical_df['date'] == for_date]


def generate_preferences() -> dict[str, list]:
    # Generate preferences for traders, desks, and benchmarks per symbol
    symbol_preferences = {}
    for sym in constants.STOCK_STORIES.keys():
        shuffled_traders = list(constants.TRADERS)
        random.shuffle(shuffled_traders)                                                                # shuffle traders for randomness each time
        weights = [0.4, 0.3, 0.2, 0.1][:len(shuffled_traders)]                                          # pick first N weights for N traders (ie: first 40%, 30%, 20%, 10% for 4 traders)
        weights += [1.0 / len(shuffled_traders)] * (len(shuffled_traders) - len(weights))               # fill remaining weights with equal distribution
        weights = [w / sum(weights) for w in weights]                                                   # normalize weights to sum to 1
        raw_p = random.choices(shuffled_traders, weights=weights, k=10 * len(shuffled_traders))         # generate 10x the number of traders to ensure enough variety
        count_d = {t: raw_p.count(t) for t in constants.TRADERS}                                        # count occurrences of each trader
        total_d = sum(count_d.values())                                                                 # total count of traders
        trader_probs = {t: round(c * 100 / total_d, 4) for t, c in count_d.items()}                     # compute and round trader probabilities
        trader_choices = [trader for trader, prob in trader_probs.items() for _ in range(int(round(prob)) )][:100]

        # random normalized weights for desks
        shuffled_desks = list(constants.DESKS)
        random.shuffle(shuffled_desks)
        weights = [0.4, 0.3, 0.2, 0.1][:len(shuffled_desks)]
        weights += [1.0 / len(shuffled_desks)] * (len(shuffled_desks) - len(weights))
        weights = [w / sum(weights) for w in weights]
        raw_p = random.choices(shuffled_desks, weights=weights, k=10 * len(shuffled_desks))
        count_d = {d: raw_p.count(d) for d in constants.DESKS}
        total_d = sum(count_d.values())
        desk_probs = {d: round(c * 100 / total_d, 4) for d, c in count_d.items()}
        desk_choices = [desk for desk, prob in desk_probs.items() for _ in range(int(round(prob)) )][:100]

        # list of funds, exec_venues, and benchmark choices for each symbol
        fund_choices       = {symbol: random.sample(constants.FUNDS, k=random.randint(1, 3))             for symbol in constants.STOCK_STORIES.keys()}
        exec_venue_choices = {symbol: random.sample(constants.EXEC_VENUES, k=random.randint(1, 2))       for symbol in constants.STOCK_STORIES.keys()}
        benchmark_choices  = {symbol: random.sample(constants.BENCHMARK_INDICES, k=random.randint(1, 2)) for symbol in constants.STOCK_STORIES.keys()}
        
        # preferences for each symbol
        symbol_preferences[sym] = {
            'trader_weights': trader_probs,
            'desk_weights': desk_probs,
            'trader_choices': trader_choices,
            'desk_choices': desk_choices,
            'fund_choices': fund_choices,
            'exec_venue_choices': exec_venue_choices,
            'benchmark_choices': benchmark_choices,
        }

    # for sym, prefs in symbol_preferences.items():
    #     print(f"{sym}: {prefs['fund_choices']}")
    #     print(f"{sym}: {prefs['exec_venue_choices']}")
    #     print(f"{sym}: {prefs['benchmark_choices']}")
    #     print()
    for sym, prefs in symbol_preferences.items():
        sorted_traders = sorted(prefs['trader_weights'].items(), key=lambda x: x[1])
        print(f"Symbol: {sym}")
        print("  Trader Weights (sorted by value):")
        for trader, prob in sorted_traders:
            print(f"    {prob:.4f}\t{trader}")
        print()

    # print(symbol_preferences['AAPL']['trader_choices'])


def generate_blotter(output_file: Path = constants.BLOTTER_FILE, historical_file: Path = constants.HISTORICAL_FILE):
    """
    Generate a daily blotter of stock trades from historical data.
    """
    assert historical_file.exists(), f"Historical data file {historical_file} does not exist."

    random.seed(42)  # for reproducibility

    # Read the historical data
    historical_df = pd.read_parquet(historical_file)
    
    # print(f"original:\n{historical_df.sort_values(by=['symbol', 'date'])[['symbol', 'date', 'close', 'volume']].head(n=20)}\n")

    # Add an `order_qty` column to the historical DataFrame.
    # `order_qty` is the difference in volume between consecutive days per symbol scaled by 0.001 (0.1%).
    # Positive or negative values indicate buy or sell.
    # sorted by symbol and date
    historical_df.sort_values(by=['symbol', 'date'], inplace=True)                  # sort by symbol and date
    qty = historical_df.groupby('symbol')['volume'].diff().fillna(0) * 0.01         # diff in volume scaled by 0.001
    historical_df['order_qty'] = qty.round().astype('int32')                        # round and convert to int32
    
    # print(f"order_qty:\n{historical_df.sort_values(by=['symbol', 'date'])[['symbol', 'date', 'close', 'volume', 'order_qty']].head(n=20)}\n")

    




if __name__ == "__main__":
    # Example usage
    # generate_blotter()
    generate_preferences()
