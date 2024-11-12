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
            # If the thread is already running, stop it
            if self.thread is not None and self.thread.is_alive():
                logger.warning("Stream generator thread was already running. Stopping existing stream data generator")
                self.stop()  # Stop the existing thread
            self.running = True
            logger.debug("Starting stream data generator thread")
            self.thread = threading.Thread(target=self._start_stream)
            self.thread.daemon = True   # Make thread a daemon so it will exit when the main program exits
            self.thread.start()
        else:
            logger.warning("Stream data generator is already running. Not doing anything.")

    def stop(self) -> None:
        if self.running or (self.thread is not None and self.thread.is_alive()):
            logger.debug("Stopping stream data generator thread")
            self.running = False
            if self.thread is not None:
                self.thread.join()

    def _start_stream(self) -> None:
        while self.running:
            # Generate data for a single interval
            start_time = time.time()
            df: pd.DataFrame = self.generate_data()
            end_time = time.time()
            logger.debug(f"Stream data generation: {len(df.index)} rows in {round(end_time - start_time, 4)} seconds")
            # Push data to callback
            if self._push_data_callback is not None:
                start_time = time.time()
                self._push_data_callback(df)
                end_time = time.time()
                logger.debug(f"Callback execution time: {round(end_time - start_time, 4)} seconds")
            # Sleep for interval
            time.sleep(self.interval)

    def generate_data(self) -> pd.DataFrame:
        # Placeholder for data generation logic
        raise NotImplementedError

    def set_interval(self, interval: float) -> None:
        logger.debug(f"Stream data generator interval set to: {interval}s")
        self.interval = interval

    @property
    def schema(self) -> dict:
        raise NotImplementedError
