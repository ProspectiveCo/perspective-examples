import io
import os
import requests
import pandas as pd
import numpy as np
import re
import random
from enum import Enum
from datetime import timedelta, datetime
import dateparser

from perspective_examples.utils import config, logger
from perspective_examples.generators.base import StreamGenerator, BatchGenerator



# Define the public interface for the module
__all__ = [
    'AlphaVantageTickerInterval', 
    'AlphaVantageIntradayInterval',
    'fetch_stocks_from_alpha_vantage',
]


class AlphaVantageTickerInterval(Enum):
    """
    Enum representing the different time intervals for stock data retrieval from the Alpha Vantage API.

    Attributes:
        INTRADAY (str): Represents intraday time series data. This provides stock prices at intervals of 1, 5, 15, 30, or 60 minutes.
        DAILY (str): Represents daily time series data. This provides stock prices at the end of each trading day.
        WEEKLY (str): Represents weekly time series data. This provides stock prices at the end of each trading week.
        MONTHLY (str): Represents monthly time series data. This provides stock prices at the end of each trading month.
    """
    INTRADAY = "TIME_SERIES_INTRADAY"
    DAILY = "TIME_SERIES_DAILY"
    WEEKLY = "TIME_SERIES_WEEKLY"
    MONTHLY = "TIME_SERIES_MONTHLY"
    


class AlphaVantageIntradayInterval(Enum):
    """
    Enum representing the different intraday intervals for stock data retrieval from the Alpha Vantage API.

    Attributes:
        ONE_MIN (str): Represents 1-minute interval data.
        FIVE_MIN (str): Represents 5-minute interval data.
        FIFTEEN_MIN (str): Represents 15-minute interval data.
        THIRTY_MIN (str): Represents 30-minute interval data.
        SIXTY_MIN (str): Represents 60-minute interval data.
    """
    ONE_MIN = "1min"
    FIVE_MIN = "5min"
    FIFTEEN_MIN = "15min"
    THIRTY_MIN = "30min"
    SIXTY_MIN = "60min"



def fetch_stocks_from_alpha_vantage(
    ticker: str, 
    time_span: str = '-1y',
    ticker_interval: 'AlphaVantageTickerInterval | str' = AlphaVantageTickerInterval.DAILY,
    intraday_interval: 'AlphaVantageIntradayInterval | str' = AlphaVantageIntradayInterval.FIFTEEN_MIN,
    api_key: str = None,
) -> pd.DataFrame:

    # --- Validating Params ---
    # correct the start_date based on the provided options
    start_date = None
    # parse time_span using dateparser if it is provided
    if time_span:
        # if time_span is not a negative value, insert a negative sign
        if not time_span.startswith('-'):
            logger.warning(f"time_span is expected to be a negative value (e.g., '-1y'). Provided time_span: {time_span}. Adding a negative sign...")
            time_span = f"-{time_span}"
            logger.info(f"Converted time_span to: {time_span}")
        start_date = dateparser.parse(time_span, settings={'RELATIVE_BASE': datetime.now()})
    # if time_span was not parsed, default start_date to 1 year ago
    if not start_date:
        start_date = datetime.now() - timedelta(days=365)
    # strip off the time from the start_date
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    # --- Validate and set the ticker interval and intraday interval ---
    # Valid string values are defined by the AlphaVantageTickerInterval enum.
    if isinstance(ticker_interval, str):
        try:
            ticker_interval = AlphaVantageTickerInterval[ticker_interval]
        except KeyError:
            logger.warning(f"Invalid ticker interval: {ticker_interval}. Must be one of {list(AlphaVantageTickerInterval)}")
            logger.info("Continuing with default ticker interval: DAILY.")
            ticker_interval = AlphaVantageTickerInterval.DAILY
    elif not isinstance(ticker_interval, AlphaVantageTickerInterval):
        logger.warning(f"ticker_interval must be of type str or AlphaVantageTickerInterval, not {type(ticker_interval)}")
        logger.info("Continuing with default ticker interval: DAILY.")
        ticker_interval = AlphaVantageTickerInterval.DAILY
    # Validate and set the intraday interval
    if isinstance(intraday_interval, str):
        try:
            intraday_interval = AlphaVantageIntradayInterval(intraday_interval)
        except ValueError:
            logger.warning(f"Invalid intraday interval: {intraday_interval}. Must be one of {list(AlphaVantageIntradayInterval)}")
            logger.info("Continuing with default intraday interval: 15 minutes.")
            intraday_interval = AlphaVantageIntradayInterval.FIFTEEN_MIN
    elif not isinstance(intraday_interval, AlphaVantageIntradayInterval):
        logger.warning(f"intraday_interval must be of type str or AlphaVantageIntradayInterval, not {type(intraday_interval)}")
        logger.info("Continuing with default intraday interval: 15 minutes.")
        intraday_interval = AlphaVantageIntradayInterval.FIFTEEN_MIN

    # --- Fetching Stock Values from the API ---
    # Fetch the stock values from the Alpha Vantage API
    if api_key is None:
        api_key = config.get('alpha_vantage', {}).get('api_key', None)
    if api_key is None or api_key == '':
        api_key = os.getenv('ALPHA_VANTAGE_API_KEY', 'DEFAULT')
    if api_key == 'DEFAULT':
        logger.warning("No API key provided. Please set the $ALPHA_VANTAGE_API_KEY environment variable OR configure the config.yaml file.")
        logger.warning("Continuing without an API key. This may result in rate limiting from the Alpha Vantage API.")
    url = config.get('alpha_vantage', {}).get('base_url', 'https://www.alphavantage.co/query')
    params = {
        'function': ticker_interval.value,
        'symbol': ticker,
        'apikey': api_key,
        'datatype': 'csv',  # Request CSV format
        'outputsize': 'full',  # Request full data
    }
    # add the intra-day interval if the ticker_interval is INTRADAY
    if ticker_interval == AlphaVantageTickerInterval.INTRADAY:
        params['interval'] = intraday_interval.value
    logger.info(f"Fetching Alpha Vantage Stock Values: ticker={ticker} start_date={start_date} interval={ticker_interval}")
    logger.info("This may take a few seconds...")
    response = requests.get(url, params=params)
        
    # Check if the response is successful
    if response.status_code != 200:
        raise ValueError(f"Error fetching data from Alpha Vantage API: {response.status_code}")
    # check for common error responses from Alpha Vanatage API
    patterns: list[str] = [
        (r'"Information":\s*".*rate limit.*premium.*"', "API rate limit exceeded. Please upgrade to a premium plan."),
        (r'"Error*":\s*".*"', "Generic Error"),
    ]
    for pattern, error_message in patterns:
        match = re.search(pattern, response.text)
        if match:
            raise ValueError(f"Error fetching data from Alpha Vantage API. {error_message}\n{response.text}")
    print(response.text)
    # --- Parsing CSV and Post Processing ---
    # Read the CSV data into a pandas DataFrame
    df = pd.read_csv(io.StringIO(response.text))
    # Ensure the DataFrame has the expected columns
    expected_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    if not all(column in df.columns for column in expected_columns):
        raise ValueError("Invalid response from Alpha Vantage API. Missing expected columns.")
    # Convert the timestamp column to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    # Filter the DataFrame to include only data from the start_date onwards
    df = df[df['timestamp'] >= start_date]
    # add a ticker column after the timestamp
    df.insert(1, 'ticker', ticker)

    return df



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
                df = fetch_stocks_from_alpha_vantage(
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
    
    @staticmethod
    def from_config(config: dict) -> 'StockValuesStreamGenerator':
        return StockValuesStreamGenerator(**config)
    


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
                df = fetch_stocks_from_alpha_vantage(
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
