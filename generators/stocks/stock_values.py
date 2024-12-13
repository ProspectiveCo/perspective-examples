import pandas as pd
import numpy as np
import random
from datetime import timedelta, datetime
from ..base import StreamGenerator, BatchGenerator
from .stock_fetcher import AlphaVantageIntradayInterval, AlphaVantageTickerInterval, fetch_stock_values
from utils.logger import logger


class StockValuesStreamGenerator(StreamGenerator):
    def __init__(self, 
                 tickers: str | list[str],
                 interval: float = 1.0,
                 start_time: str | datetime = datetime.now().replace(microsecond=0),
                 periods: int = None,
                 ticker_interval: AlphaVantageTickerInterval | str = AlphaVantageTickerInterval.DAILY,
                 intraday_interval: AlphaVantageIntradayInterval | str = AlphaVantageIntradayInterval.FIFTEEN_MIN,
                 api_key: str = None,
                 **kwargs):
        super().__init__(start_time=start_time, interval=interval, **kwargs)
        self.periods = periods
        self.tickers = [tickers] if isinstance(tickers, str) else tickers
        self.ticker_interval = ticker_interval
        self.intraday_interval = intraday_interval
        self.api_key = api_key
        self.cache: dict[str, pd.DataFrame] = None
        self.ticker_indices: dict[str, int] = {ticker: 0 for ticker in self.tickers}    # Keep track of the current index position within the dataframe for each ticker
        self._current_period = 0

    def fetch_stock_values(self, **kwargs) -> dict[str, pd.DataFrame]:
        # overwrite class members from kwargs
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        # set local configuration variables
        tickers = self.tickers
        ticker_interval = self.ticker_interval
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
        # zero out the ticker indices
        self.ticker_indices = {ticker: 0 for ticker in tickers}
        # return the entire cache stock values
        return self.cache

        
    def get_data(self) -> pd.DataFrame:
        # Fetch stock values if cache is empty
        if self.cache is None:
            self.fetch_stock_values()
        # construct an array of dicts of the current index of each ticker
        data = [(self.cache[ticker].iloc[index].to_dict() if index < len(self.cache[ticker].index) else {}) for ticker, index in self.ticker_indices.items()]
        # advance the ticker indices
        self.ticker_indices = {ticker: (index + 1 if index < len(self.cache[ticker].index) else 0) for ticker, index in self.ticker_indices.items()}
        df = pd.DataFrame(data)
        # set the timestamp of the data frame to the current time
        df['timestamp'] = self.current_time
        # advance the current time
        self.current_time += timedelta(seconds=self.interval)
        # return the data frame
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
    
    @staticmethod
    def required_parameters() -> list:
        return ['tickers']
    


class HistoricalStockValuesBatchGenerator(BatchGenerator):
    def __init__(
            self,
            tickers: str | list[str],
            time_span: str = '1y',
            ticker_interval: AlphaVantageTickerInterval | str = AlphaVantageTickerInterval.DAILY,
            intraday_interval: AlphaVantageIntradayInterval | str = AlphaVantageIntradayInterval.FIFTEEN_MIN,
            api_key: str = None,
            **kwargs
            ) -> None:
        super().__init__(**kwargs)
        self.tickers = [tickers] if isinstance(tickers, str) else tickers
        self.time_span = time_span
        self.ticker_interval = ticker_interval
        self.intraday_interval = intraday_interval
        self.api_key = api_key

    def get_data(self) -> pd.DataFrame:
        data = []
        for ticker in self.tickers:
            try:
                df = fetch_stock_values(
                    ticker=ticker, 
                    time_span=self.time_span, 
                    ticker_interval=self.ticker_interval, 
                    intraday_interval=self.intraday_interval, 
                    api_key=self.api_key,
                    )
                data.append(df)
            except Exception as e:
                logger.error(f"StocksBatchGenerator::FetchError: Error fetching historical stock values for ticker: {ticker}")
                logger.error(f"StocksBatchGenerator::FetchError: {e}")
        # if data is empty, return an empty data frame
        if not data:
            return pd.DataFrame()
        # concatenate the data frames and sort by timestamp and ticker
        df = pd.concat(data, ignore_index=True)
        df = df.sort_values(by=['timestamp', 'ticker'], ignore_index=True)
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
    
    @staticmethod
    def required_parameters() -> list:
        return ['tickers']
    
    @staticmethod
    def from_config(config: dict) -> 'HistoricalStockValuesBatchGenerator':
        return HistoricalStockValuesBatchGenerator(**config)
    


class StockStreamDataGenerator(StreamGenerator):
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
    
    
def test():
    stock_stream_generator = StockValuesStreamGenerator(tickers=['AAPL', 'GOOGL', 'MSFT'], interval=1.0, periods=8, start_time='2024-01-01 00:00:00')
    for _ in range(10):
        print(stock_stream_generator.get_data())
        print()
    print(f"cached sizes:")
    print({ticker: len(df) for ticker, df in stock_stream_generator.cache.items()})
    print(f"current indices:")
    print(stock_stream_generator.ticker_indices)


if __name__ == '__main__':
    test()
