import time
import requests
import os
from enum import Enum


# Define the public interface for the module
__all__ = ['AlphaVantageTickerInterval', 'FetchCurrentStockValue']

# default module values
__DEFAULT_ALPHA_VANTAGE_API_URL__ = 'https://www.alphavantage.co/query'


class AlphaVantageTickerInterval(Enum):
    """
    Enum representing the different time intervals for stock data retrieval from the Alpha Vantage API.

    Attributes:
        TIME_SERIES_INTRADAY (str): Represents intraday time series data. This provides stock prices at intervals of 1, 5, 15, 30, or 60 minutes.
        TIME_SERIES_DAILY (str): Represents daily time series data. This provides stock prices at the end of each trading day.
        TIME_SERIES_WEEKLY (str): Represents weekly time series data. This provides stock prices at the end of each trading week.
        TIME_SERIES_MONTHLY (str): Represents monthly time series data. This provides stock prices at the end of each trading month.
    """
    TIME_SERIES_INTRADAY = "TIME_SERIES_INTRADAY"
    TIME_SERIES_DAILY = "TIME_SERIES_DAILY"
    TIME_SERIES_WEEKLY = "TIME_SERIES_WEEKLY"
    TIME_SERIES_MONTHLY = "TIME_SERIES_MONTHLY"


class FetchCurrentStockValue:
    """
    A class to fetch and cache the current stock value using the Alpha Vantage API.
    Attributes:
        cache_duration (int): Duration in seconds to cache the stock values.
        cache (dict): A dictionary to store cached stock values.
        api_key (str): API key for Alpha Vantage.
        ticker_interval (AlphaVantageTickerInterval): The interval for fetching stock data.
    Methods:
        get_stock_value(ticker):
            Fetches the current stock value for the given ticker symbol, using the cache if available.
        fetch_stock_value(ticker):
            Fetches the current stock value for the given ticker symbol directly from the Alpha Vantage API.
    """

    def __init__(
        self, 
        cache_duration=3, 
        ticker_interval: 'AlphaVantageTickerInterval | str' = AlphaVantageTickerInterval.TIME_SERIES_DAILY
    ):
        self.cache_duration = cache_duration
        self.cache = {}
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        assert self.api_key, "API key for Alpha Vantage is not set in environment variables.\nPlease visit https://www.alphavantage.co/ to obtain a free API key and set ALPHA_VANTAGE_API_KEY in your environment variables."
        # Validate and set the ticker interval. 
        # Valid string values are defined by the AlphaVantageTickerInterval enum.
        if isinstance(ticker_interval, str):
            try:
                ticker_interval = AlphaVantageTickerInterval[ticker_interval]
            except KeyError:
                raise ValueError(f"Invalid ticker interval: {ticker_interval}. Must be one of {list(AlphaVantageTickerInterval)}")
        elif not isinstance(ticker_interval, AlphaVantageTickerInterval):
            raise TypeError(f"ticker_interval must be of type str or AlphaVantageTickerInterval, not {type(ticker_interval)}")

        self.ticker_interval = ticker_interval

    def get_stock_value(self, ticker):
        current_time = time.time()
        if ticker in self.cache:
            cached_time, cached_value = self.cache[ticker]
            if current_time - cached_time < self.cache_duration:
                return cached_value

        stock_value = self.fetch_stock_value(ticker)
        self.cache[ticker] = (time.time(), stock_value)
        return stock_value

    def fetch_stock_value(self, ticker):
        if not self.api_key:
            raise ValueError("API key for Alpha Vantage is not set in environment variables.")
        
        url = f'https://www.alphavantage.co/query?function={self.ticker_interval.value}&symbol={ticker}&interval=1min&apikey={self.api_key}'
        response = requests.get(url)
        data = response.json()
        
        # Extract the latest stock price from the response
        time_series = data.get('Time Series (1min)', {})
        if not time_series:
            raise ValueError("Invalid response from Alpha Vantage API.")
        
        latest_timestamp = sorted(time_series.keys())[0]
        latest_data = time_series[latest_timestamp]
        return float(latest_data['1. open'])


if __name__ == '__main__':
    fetcher = FetchCurrentStockValue()
    stock_value = fetcher.get_stock_value('AAPL')
    print(f"Current stock value of AAPL: {stock_value}")
    