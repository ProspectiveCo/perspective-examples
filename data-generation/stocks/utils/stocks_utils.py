import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from perspective.widget import PerspectiveWidget
import threading
import time
from typing import Union, List


__all__ = (
    "STOCKS_SCHEMA",
    "TRADES_SCHEMA",
    "DemoDataStreamPlayer",
)

STOCKS_SCHEMA = {
    'date': 'date', 
    'year': 'integer', 
    'ticker': 'string', 
    'adj_close': 'float', 
    'close': 'float', 
    'high': 'float', 
    'low': 'float', 
    'open': 'float', 
    'volume': 'integer', 
    'current_shares': 'float', 
    'daily_trades': 'float', 
    'trade_price': 'float', 
    'trade_value': 'float', 
    'cash_reserves': 'float', 
    'portfolio_value': 'float'
}

TRADES_SCHEMA = {
    'trade_id': 'integer', 
    'trade_timestamp': 'datetime', 
    'ticker': 'string', 
    'broker': 'string', 
    'bid_price': 'float', 
    'ask_price': 'float', 
    'trade_price': 'float', 
    'bid_spread': 'float', 
    'shares': 'integer', 
    'trade_value': 'float', 
    'open': 'float', 
    'close': 'float', 
    'date': 'date'
}


class DemoDataStreamPlayer:
    """
    A class to stock's date file and 
    """

    def __init__(self, data_filepath: str, widgets: Union[PerspectiveWidget, List[PerspectiveWidget]], interval: int = 0.1, **kwargs):
        """
        Initializes the RepeatedTimer instance.

        Parameters:
        - interval: The interval in seconds between each function execution.
        - function: The function to run at the specified interval.
        - args: Arguments to pass to the function.
        - kwargs: Keyword arguments to pass to the function.
        """
        self.data_filepath = data_filepath
        if isinstance(widgets, list):
            self.widgets: List[PerspectiveWidget] = widgets
        else:
            self.widgets: List[PerspectiveWidget] = [widgets]
        self.interval = interval
        self.kwargs = kwargs
        self.thread = None
        self.stop_event = threading.Event()
        self.current_frame_date = None
        # load the datafile into a dataframe
        self.min_date, self.max_date = None, None
        self.df = self._load_df(data_filepath)

    def _load_df(self, filepath) -> pd.DataFrame:
        """read the stocks data file and prepare columns

        Returns:
            pd.DataFrame: stocks dataframe
        """
        df = pd.read_csv(filepath, header=0)
        # make sure we have all the important columns
        for col in {'date', 'ticker'}:
            assert col in df.columns, f"'{col}' column not present!"
        # drop columns
        df.drop(columns=['year'], inplace=True, errors='ignore')
        # converting down column data types to float and unsigned ints
        for col in df.columns:
            cast_down_type = None
            if df[col].dtype == np.float64:
                cast_down_type = 'float'
            elif df[col].dtype == np.int64:
                cast_down_type = 'integer'
            if cast_down_type:
                df[col] = pd.to_numeric(df[col], downcast=cast_down_type)
        # load min/max data dates
        self.min_date, self.max_date = df['date'].min(), df['date'].max()
        print(f"data min: {self.min_date}  max: {self.max_date}")
        return df

    def _run(self):
        """
        The function that runs in a separate thread, calling the target function
        at the specified interval.
        """
        while not self.stop_event.is_set():
            try:
                # set current date if not set already!
                if (self.current_frame_date is None) or (self.current_frame_date > self.max_date):
                    self.current_frame_date = self.min_date
                # get current frame data for today
                df = self.df
                frame = df[df['date'] == self.current_frame_date]
                data = frame.to_dict(orient='records')
                # advance to date & send data to perspective widget
                self.current_frame_date = datetime.strftime(
                    datetime.strptime(self.current_frame_date, r'%Y-%m-%d') + timedelta(days=1), 
                    r'%Y-%m-%d')
                for widget in self.widgets:
                    widget.update(data.copy())
            except Exception as e:
                print(f"Error occurred: {e}")
            # Sleep until the next execution time
            time.sleep(self.interval)

    def start(self):
        """
        Start the background thread that runs the function at the specified interval.
        """
        if self.thread is None or not self.thread.is_alive():
            self.stop_event.clear()  # Reset the stop event
            self.thread = threading.Thread(target=self._run)
            self.thread.daemon = True  # Make thread a daemon so it will exit when the main program exits
            self.thread.start()

    def stop(self):
        """
        Stop the background thread and wait for it to finish cleanly.
        """
        self.stop_event.set()  # Signal the thread to stop
        if self.thread is not None:
            self.thread.join()  # Wait for the thread to finish
