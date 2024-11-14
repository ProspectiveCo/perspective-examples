import time
import requests
import io
from enum import Enum
import dateparser
import pandas as pd
from datetime import datetime, timedelta
from data_generators.stocks import stocks_generator_config
from data_generators.utils import logger


# Define the public interface for the module
__all__ = [
    'AlphaVantageTickerInterval', 
    'AlphaVantageIntradayInterval',
    'fetch_stock_values',
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



def fetch_stock_values(
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
    # if time_span was misparsed, default start_date to 1 year ago
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
    if not api_key:
        api_key = stocks_generator_config['alpha_vantage']['api_key']
    url = stocks_generator_config['alpha_vantage']['base_url']
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


def _get_past_date_from_options(
    time_span: str = None,
    start_date: str | datetime = None,
    nrows: int = None,
    ticker_interval: 'AlphaVantageTickerInterval' = None,
) -> datetime:
    """
    Calculate a past date based on the provided options: time_span, start_date, and nrows.
    Args:
        time_span (str, optional): A string representing a relative time span (e.g., '-2d' or '-5y').
        start_date (str | datetime, optional): A specific start date in 'YYYY-MM-DD' format or a datetime object.
        nrows (int, optional): The number of rows to determine the past date based on the ticker interval.
    Returns:
        datetime: The calculated past date based on the most recent date from the provided options.
    Notes:
        - If `time_span` is provided, it will be parsed to a datetime object.
        - If `start_date` is provided as a string, it will be parsed to a datetime object.
        - If `nrows` is provided, the past date will be calculated based on the ticker interval.
        - If all three options are None, the past date will default to 1 year ago.
        - If `start_date` is in the future, it will be ignored.
        - If `nrows` is provided for `INTRADAY`, it will be ignored.
    """
    # Parse the time_span string using dateparser
    relative_time_span_date = None
    if time_span:
        relative_time_span_date = dateparser.parse(time_span, settings={'RELATIVE_BASE': datetime.now()})
    # check if start_date is a string and parse it
    if start_date:
        # ignore start_date if it is not a string or datetime
        if not (isinstance(start_date, datetime) or isinstance(start_date, str)):
            logger.warning(f"Invalid start_date: {start_date}. Expected type: str or datetime.")
            logger.info("Continuing as if start_date was not defined.")
        # parse start_date if it is a string
        if isinstance(start_date, str):
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError as e:
                logger.warning(f"Invalid start_date format: {start_date}. Expected format: YYYY-MM-DD. Error: {e}")
                logger.info("Continuing as if start_date was not defined.")
    # check if start_date is in the past
    if start_date and start_date >= datetime.now():
        logger.warning(f"start_date {start_date} is not in the past. Continuing as if start_date was not defined.")
        start_date = None
    # correctly calculating a date in the past based on nrows value
    relative_nrows_date = None
    if nrows:
        if not ticker_interval:
            ticker_interval = AlphaVantageTickerInterval.DAILY
        if ticker_interval == AlphaVantageTickerInterval.INTRADAY:
            logger.warning("nrows is not supported for INTRADAY. Ignoring nrows.")
        elif ticker_interval == AlphaVantageTickerInterval.DAILY:
            relative_nrows_date = datetime.now() - timedelta(days=nrows)
        elif ticker_interval == AlphaVantageTickerInterval.WEEKLY:
            relative_nrows_date = datetime.now() - timedelta(weeks=nrows)
        elif ticker_interval == AlphaVantageTickerInterval.MONTHLY:
            relative_nrows_date = datetime.now() - pd.DateOffset(months=nrows)
    # If all three date sources are None, set start_date to 1 year ago
    if not any([relative_time_span_date, start_date, relative_nrows_date]):
        start_date = datetime.now() - timedelta(days=365)
    # take the most recent date from the three date sources
    past_date = max(filter(None, [relative_time_span_date, start_date, relative_nrows_date]))
    return past_date


if __name__ == '__main__':
    df = fetch_stock_values('AAPL', time_span='-1y', ticker_interval='DAILY', intraday_interval='15min')
    print(df)
    