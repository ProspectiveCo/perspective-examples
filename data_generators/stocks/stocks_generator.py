import pandas as pd
import numpy as np
import random
from datetime import timedelta, datetime
from data_generators.generators import StreamDataGenerator
from stock_fetchers import AlphaVantageTickerInterval, AlphaVantageIntradayInterval, fetch_stock_values
from utils import logger


class StockValueStreamGenerator(StreamDataGenerator):
    def __init__(self, 
                 tickers: str | list[str],
                 function_interval: AlphaVantageTickerInterval | str = AlphaVantageTickerInterval.DAILY,
                 intraday_interval: AlphaVantageIntradayInterval | str = AlphaVantageIntradayInterval.FIFTEEN_MIN,
                 api_key: str = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.tickers = [tickers] if isinstance(tickers, str) else tickers
        self.function_interval = function_interval
        self.intraday_interval = intraday_interval
        self.api_key = api_key
        self.cache: dict[str, pd.DataFrame] = None

    def fetch_stock_values(self, **kwargs) -> dict[str, pd.DataFrame]:
        # overwrite class members from kwargs
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        # set local configuration variables
        tickers = self.tickers
        ticker_interval = self.tickers_interval
        intraday_interval = self.intraday_interval
        api_key = self.api_key
        # clear existing cache
        self.cache: dict[str, pd.DataFrame] = {}
        # fetch stock values for each ticker
        for ticker in tickers:
            try:
                df = fetch_stock_values(
                    ticker=ticker,
                    ticker_interval=ticker_interval,
                    intraday_interval=intraday_interval,
                    api_key=api_key
                )
                # Simulate streaming by setting the current timestamp to now
                df['timestamp'] = datetime.now()
                self.cache[ticker] = df
            except Exception as e:
                logger.error(f"Error fetching stock values: {e}")
                self.cache[ticker] = pd.DataFrame()
            return df

        
    def get_data(self) -> pd.DataFrame:
        df = fetch_stock_values(
            ticker=self.ticker,
            ticker_interval=self.function_interval,
            intraday_interval=self.intraday_interval,
            api_key=self.api_key
        )
        # Simulate streaming by setting the current timestamp to now
        df['timestamp'] = datetime.now()
        return df

    @property
    def schema(self) -> dict:
        return {
            'timestamp': 'datetime64[ns]',
            'ticker': 'str',
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'volume': 'int'
        }



class StockStreamDataGenerator(StreamDataGenerator):
    def __init__(self, data_filepath: str, min_trades_per_day: int = 50, max_trades_per_day: int = 200, share_prct_range: tuple = (0.00001, 0.0001)):
        super().__init__()
        self.data_filepath = data_filepath
        self.min_trades_per_day = min_trades_per_day
        self.max_trades_per_day = max_trades_per_day
        self.share_prct_range = share_prct_range
        self.brokers = [
            "Slick Sam", "Trading Tina", "Money Mike", "Clever Cathy", "Profit Pete", 
            "Risky Rachel", "Big Bucks Bob", "Smart Susan", "Lucky Luke"
        ]
        self.data = pd.read_csv(self.data_filepath, parse_dates=['date'])
        self.date_range = pd.date_range(start=self.data['date'].min(), end=self.data['date'].max())
        self.current_date_index = 0

    def get_data(self) -> pd.DataFrame:
        if self.current_date_index >= len(self.date_range):
            self.current_date_index = 0  # Reset to start if we reach the end

        current_date = self.date_range[self.current_date_index]
        self.current_date_index += 1

        day_data = self.data[self.data['date'] == current_date]
        if day_data.empty:
            return pd.DataFrame()  # Return empty DataFrame if no data for the current date

        num_tickers = len(day_data['ticker'].unique())
        log_weights = np.logspace(0, -1, num=num_tickers)
        day_data = day_data.assign(weight=log_weights)
        day_data = day_data.sort_values(by='weight', ascending=False)

        trades = []
        for _, row in day_data.iterrows():
            num_trades = random.randint(self.min_trades_per_day, self.max_trades_per_day)
            for _ in range(num_trades):
                broker = random.choice(self.brokers)
                trade_timestamp = current_date + timedelta(seconds=random.randint(0, 86399))
                bid_price = np.random.uniform(row['low'], row['high'])
                ask_price = np.random.uniform(row['low'], row['high'])
                while ask_price <= bid_price:
                    ask_price = np.random.uniform(row['low'], row['high'])
                trade_price = np.random.uniform(bid_price, ask_price)
                bid_spread = ask_price - bid_price
                shares = int(row['volume'] * row['weight'] * np.random.uniform(*self.share_prct_range))
                trade_value = round(trade_price * shares, ndigits=6)

                trade = {
                    'trade_timestamp': trade_timestamp,
                    'ticker': row['ticker'],
                    'broker': broker,
                    'bid_price': round(bid_price, 4),
                    'ask_price': round(ask_price, 4),
                    'trade_price': round(trade_price, 4),
                    'bid_spread': round(bid_spread, 4),
                    'shares': shares,
                    'trade_value': round(trade_value, 4),
                    'open': round(row['open'], 6),
                    'close': round(row['adj_close'], 6),
                    'date': row['date'],
                }
                trades.append(trade)

        return pd.DataFrame(trades)