import time
import threading
import pandas as pd
from utils import logger

class StreamDataGenerator:
    def __init__(self, 
                 interval: float = 1.0,
                 push_data_callback: callable = None,
                 **kwargs) -> None:
        self.interval: float = interval
        self.running: bool = False
        self.thread: threading.Thread = None
        self._push_data_callback: callable = push_data_callback
        self._kwargs = kwargs

    def start(self) -> None:
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._start_stream)
            self.thread.start()

    def stop(self) -> None:
        if self.running:
            self.running = False
            if self.thread is not None:
                self.thread.join()

    def _start_stream(self) -> None:
        while self.running:
            # Generate data for a single interval
            df: pd.DataFrame = self.generate_data()
            # Push data to callback
            if self._push_data_callback is not None:
                self._push_data_callback(df)
            # Sleep for interval
            time.sleep(self.interval)

    def generate_data(self) -> pd.DataFrame:
        # Placeholder for data generation logic
        raise NotImplementedError

    def set_interval(self, interval: float) -> None:
        self.interval = interval

    @property
    def schema(self) -> dict:
        raise NotImplementedError
    
