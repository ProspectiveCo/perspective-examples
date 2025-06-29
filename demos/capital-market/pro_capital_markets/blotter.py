"""
Reads actual historical stock performance data from the historical data file
and generates a daily blotter of stock trades. Each trade represents a stock
transaction (sale or purchase) for a specific stock symbol on a given date and 
contains a portion of the actual daily volume traded for that stock in the 
historical data file.
"""
import asyncio
import datetime as dt
import time
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
# Blotter DataFrame schema
df_blotter = pd.DataFrame({
    col: pd.Series(dtype=spec["dtype"])
    for col, spec in SCHEMA.items()
})


def seed(value: int = 42):
    random.seed(value)


async def generate_blotter_for_symbol(
    symbol: str, 
    historical_df: pd.DataFrame,
    symbol_preferences: dict[str, list],
    commissions: dict[str, float],
    venue_fees: dict[str, float],
    ) -> pd.DataFrame:
    """
    Generate a daily blotter for a specific stock symbol based on historical data.

    Generate daily blotter activity per symbol from historical data. Each symbol would generate 
    an N number of traders per day, totalling near the volume of pre-computed daily order_qty 
    column in the historical data file. Orders are randomly generated based on the symbol's
    preferences, which include trader and desk weights, fund choices, execution venues, and 
    benchmark indices. The blotter includes details such as order quantity, price, commission, execution venue fees and 
    more. 

    Args:
        symbol (str): The stock symbol to generate the blotter for.
        historical_df (pd.DataFrame): DataFrame containing historical stock data.
        symbol_preferences (dict[str, list]): Weighted preferences for traders, desks, and benchmarks per symbol.
        commissions (dict[str, float]): Commission structure for traders.
        venue_fees (dict[str, float]): Venue fee structure for execution venues.
    """
    start_timer = time.time()  # Start timer for performance measurement
    # Filter historical data for the specific symbol
    symbol_data = historical_df[historical_df['symbol'] == symbol].copy()
    if symbol_data.empty:
        return pd.DataFrame()
    
    # Get symbol preferences
    prefs = symbol_preferences.get(symbol, {})
    trader_choices = prefs.get('trader_choices', random.sample(constants.TRADERS, k=5))
    desk_choices = prefs.get('desk_choices', random.sample(constants.DESKS, k=3))
    fund_choices = prefs.get('fund_choices', {}).get(symbol, random.sample(constants.FUNDS, k=2))
    exec_venue_choices = prefs.get('exec_venue_choices', {}).get(symbol, random.sample(constants.EXEC_VENUES, k=2))
    benchmark_choices = prefs.get('benchmark_choices', {}).get(symbol, random.sample(constants.BENCHMARK_INDICES, k=2))
    
    trades = []
    trade_id_counter = random.randint(100000, 999999)
    
    # Get stock metadata
    stock_info = constants.STOCK_STORIES.get(symbol, {})
    security_name = stock_info.get('name', f'{symbol} Corp.')
    sector = stock_info.get('sector', 'Unknown')
    # industry = stock_info.get('industry', 'Unknown')
    # sector_gics = f"{sector} / {industry}"
    sector_gics = sector
    
    for _, row in symbol_data.iterrows():
        date = row['date']
        order_qty = abs(row['order_qty'])
        
        # Skip days with zero order quantity
        if order_qty == 0:
            continue
            
        # Generate 1-10 traders per day with a bell curve skewed towards higher values (mean ~7)
        weights = [1, 2, 4, 7, 10, 14, 18, 20, 14, 10]  # Skewed bell curve, peak at 8
        num_traders = random.choices(range(1, 11), weights=weights, k=1)[0]
        
        # Create random volumes that sum roughly to order_qty with jitter
        base_qty = order_qty // num_traders
        remaining_qty = order_qty
        daily_volumes = []
        
        for i in range(num_traders):
            if i == num_traders - 1:  # Last trader gets remaining quantity
                qty = remaining_qty
            else:
                # Add jitter: ±30% of base quantity
                jitter = random.randint(-int(base_qty * 0.3), int(base_qty * 0.3))
                qty = max(1, base_qty + jitter)
                qty = min(qty, remaining_qty - (num_traders - i - 1))  # Ensure we don't exceed remaining
            
            daily_volumes.append(qty)
            remaining_qty -= qty
            
            if remaining_qty <= 0:
                break
        
        # Generate trades for each volume
        for i, qty in enumerate(daily_volumes):
            if qty <= 0:
                continue
                
            trade_id_counter += 1
            
            # Select trader and desk from weighted choices
            trader = random.choice(trader_choices)
            desk = random.choice(desk_choices)
            
            # Determine side (BUY/SELL) - positive order_qty indicates net buying
            side = "BUY" if row['order_qty'] > 0 else "SELL"
            if random.random() < 0.2:  # 20% chance to flip side for realism
                side = "SELL" if side == "BUY" else "BUY"
            
            # Order type and status
            order_type = random.choice(constants.ORDER_TYPES)
            order_status = random.choices(constants.ORDER_STATUSES, weights=[0.1, 0.2, 0.65, 0.05], k=1)[0]
            
            # Price calculations
            close_price = float(row['close'])
            high_price = float(row['high'])
            low_price = float(row['low'])
            
            # Generate realistic bid/ask spread (0.01-0.10% of price)
            spread_pct = random.uniform(0.0001, 0.001)
            spread_price = close_price * spread_pct
            
            mid_price = close_price + random.uniform(-spread_price, spread_price)
            bid_price = mid_price - spread_price / 2
            ask_price = mid_price + spread_price / 2
            
            # Execution price with some slippage
            if side == "BUY":
                price = ask_price + random.uniform(0, spread_price * 0.5)  # slight slippage
                limit_price = price * random.uniform(1.001, 1.01)  # limit slightly above
            else:
                price = bid_price - random.uniform(0, spread_price * 0.5)  # slight slippage
                limit_price = price * random.uniform(0.99, 0.999)  # limit slightly below
            
            price = round(price, 2)
            limit_price = round(limit_price, 2)
            
            # Trade value and fees
            trade_value = qty * price
            commission = trade_value * commissions.get(trader, 0.001)
            
            exec_venue = random.choice(exec_venue_choices)
            venue_fee = trade_value * venue_fees.get(exec_venue, 0.0005)
            
            # Other selections
            fund = random.choice(fund_choices)
            benchmark_index = random.choice(benchmark_choices)
            
            # Create timestamp (random time during trading day)
            trading_start = dt.datetime.combine(date, dt.time(9, 30))  # 9:30 AM
            trading_end = dt.datetime.combine(date, dt.time(16, 0))    # 4:00 PM
            seconds_range = int((trading_end - trading_start).total_seconds())
            random_seconds = random.randint(0, seconds_range)
            event_ts = trading_start + dt.timedelta(seconds=random_seconds)
            
            # Create trade record
            trade = {
                "trade_id": trade_id_counter,
                "trader": trader,
                "desk": desk,
                "event_ts": event_ts,
                "symbol": symbol,
                "security_name": security_name,
                "sector_gics": sector_gics,
                "side": side,
                "order_type": order_type,
                "order_qty": qty,
                "order_status": order_status,
                "limit_price": limit_price,
                "qty": qty if order_status == "FILLED" else random.randint(0, qty),
                "price": price,
                "trade_value": trade_value,
                "commission": round(commission, 4),
                "exec_venue": exec_venue,
                "venue_fee": round(venue_fee, 4),
                "bid_price": round(bid_price, 2),
                "ask_price": round(ask_price, 2),
                "mid_price": round(mid_price, 2),
                "spread_price": round(spread_price, 4),
                "high_day": high_price,
                "low_day": low_price,
                "fund": fund,
                "benchmark_index": benchmark_index,
            }
            trades.append(trade)
    
    # Convert to DataFrame with proper schema
    if not trades:
        return pd.DataFrame()
        
    trades_df = pd.DataFrame(trades)
    
    # Apply schema dtypes
    for col, spec in SCHEMA.items():
        if col in trades_df.columns:
            if spec["dtype"] == "category":
                trades_df[col] = trades_df[col].astype("category")
            elif spec["dtype"] == "int32":
                trades_df[col] = trades_df[col].astype("int32")
            elif spec["dtype"] == "float32":
                trades_df[col] = trades_df[col].astype("float32")
            elif spec["dtype"] == "datetime64[ns]":
                trades_df[col] = pd.to_datetime(trades_df[col])
    
    end_timer = time.time()  # End timer for performance measurement
    elapsed_time = end_timer - start_timer
    print(f"Generated {len(trades_df):,} trades for symbol {symbol} in {elapsed_time:,.2f} seconds", flush=True)
    return trades_df
    


def generate_preferences() -> dict[str, dict]:
    # Generate preferences for traders, desks, and benchmarks per symbol
    symbol_preferences = {}
    
    # Generate fund, exec_venue, and benchmark choices for each symbol
    fund_choices = {symbol: random.sample(constants.FUNDS, k=random.randint(1, 3)) for symbol in constants.STOCK_STORIES.keys()}
    exec_venue_choices = {symbol: random.sample(constants.EXEC_VENUES, k=random.randint(1, 2)) for symbol in constants.STOCK_STORIES.keys()}
    benchmark_choices = {symbol: random.sample(constants.BENCHMARK_INDICES, k=random.randint(1, 2)) for symbol in constants.STOCK_STORIES.keys()}
    
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

    return symbol_preferences


def generate_commissions() -> dict[str, float]:
    """
    Generate a random commission structure for trades.
    """
    return {trader: round(random.uniform(0.0001, 0.005), 4) for trader in constants.TRADERS}


def generate_venue_fees() -> dict[str, float]:
    """
    Generate a random venue fee structure for trades.
    """
    return {venue: round(random.uniform(0.0001, 0.001), 4) for venue in constants.EXEC_VENUES}


async def generate_blotter(output_file: Path = constants.BLOTTER_FILE, historical_file: Path = constants.HISTORICAL_FILE):
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
    
    # Generate preferences, commissions, and venue fees
    symbol_preferences = generate_preferences()
    commissions = generate_commissions()
    venue_fees = generate_venue_fees()

    # Generate blotter for each symbol in the historical data
    # Run generate_blotter_for_symbol for all symbols in parallel using asyncio.gather
    symbols = historical_df['symbol'].unique()
    tasks = [
        generate_blotter_for_symbol(symbol, historical_df, symbol_preferences, commissions, venue_fees)
        for symbol in symbols
    ]
    results = await asyncio.gather(*tasks)
    all_trades = [df for df in results if not df.empty]
    
    # Concatenate all trades into a single DataFrame
    if all_trades:
        df_blotter = pd.concat(all_trades, ignore_index=True)
        df_blotter.sort_values(by=['event_ts', 'symbol'], inplace=True, ignore_index=True)
        # rest trade_ids sequentially based on df indices
        df_blotter['trade_id'] = df_blotter.index + 10_001  # trade_id offset
    else:
        # Create empty DataFrame with proper schema
        df_blotter = pd.DataFrame({col: pd.Series(dtype=spec["dtype"]) for col, spec in SCHEMA.items()})
    
    # Write the blotter to the output file
    df_blotter.to_parquet(output_file, index=False)
    print(f"Generated blotter with {len(df_blotter):,} trades for {len(df_blotter['symbol'].unique()) if not df_blotter.empty else 0:,} symbols.", flush=True)
    return df_blotter


def run_generate_blotter():
    """
    Wrapper function to run the async generate_blotter function.
    """
    return asyncio.run(generate_blotter())


if __name__ == "__main__":
    seed(42)  # Seed for reproducibility
    # Example usage
    run_generate_blotter()
    # generate_preferences()
