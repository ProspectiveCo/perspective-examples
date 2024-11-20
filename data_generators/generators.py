import time
from datetime import datetime
import threading
import pandas as pd
from utils import logger
from abc import ABC, abstractmethod
import codetiming


class StreamDataGenerator(ABC):
    def __init__(self, 
                 interval: float = 1.0,                                     # interval: The number of seconds to wait between each batch of data generation.
                 min_rows: int = 10,                                        # min_rows: The minimum number of rows to generate in a single batch.
                 max_rows: int = 100,                                       # max_rows: The maximum number of rows to generate in a single batch.
                 start_time: str | datetime = datetime.now(),               # start_time: The start time for the data generator. Timestamp of the first row. Set to None to start from current time. Sample date format: "2021-01-01 00:00:00"
                 end_time: str | datetime = None,                           # end_time: The end time for the data generator. Timestamp of the last row. Set to None if generator should run indefinitely. Sample date format: "2021-01-01 00:00:00"
                 loopback: bool = False,                                    # loopback: If True, the generator will loop back to the start time after reaching the end time.
                 data_generator_callback_function: callable = None,         # data_generator_callback_function: Callback function to push generated data to when new batch of data is generated.
                 **kwargs) -> None:
        # Initialize the stream data generator

        # setting data generator throughput parameters
        self.interval: float = interval
        self.min_rows: int = min_rows
        self.max_rows: int = max_rows
        # setting data generator time parameters
        # parsing start_time and end_time if they are strings
        try:
            if isinstance(start_time, str):
                self.start_time = pd.to_datetime(start_time)
            elif isinstance(start_time, datetime):
                self.start_time = start_time
            else:
                raise ValueError("Invalid type for start_time")
        except Exception as e:
            logger.warning(f"Stream-Generator::InvalidStartTime: {e}. Using current system timestamp as default.")
            self.start_time = datetime.now()
        try:
            if isinstance(end_time, str):
                self.end_time = pd.to_datetime(end_time)
            elif isinstance(end_time, datetime):
                self.end_time = end_time
            else:
                raise ValueError("Invalid type for end_time")
        except Exception as e:
            logger.warning(f"Stream-Generator::InvalidEndTime: {e}. Using None as default.")
            self.end_time = None
        # setting current time. current_time is used to keep track of the current timestamp of the data batch while generating data
        self.current_time: datetime = self.start_time
        # setting loopback parameter. loopback is used to reset the current_time to start_time after reaching end_time. If end_time is None, loopback is ignored.
        self.loopback: bool = loopback
        # setting data generator callback function. The callback function is called with the generated data batch.
        self.running: bool = False
        self.data_generator_thread: threading.Thread = None
        self.data_generator_callback_function: callable = data_generator_callback_function
        # setting additional keyword arguments
        self._kwargs = kwargs

    def start(self) -> None:
        if not self.running:
            # If the thread is already running, stop it
            if self.data_generator_thread is not None and self.data_generator_thread.is_alive():
                logger.warning("Stream-Generator::ThreadIsRunning: Stream generator thread was already running. Stopping existing stream data generator")
                self.stop()  # Stop the existing thread
            logger.debug("Stream-Generator::StartGenThread")
            self.running = True
            self.data_generator_thread = threading.Thread(target=self._data_generator_runner)
            self.data_generator_thread.daemon = True   # Make thread a daemon so it will exit when the main program exits
            self.data_generator_thread.start()
        else:
            logger.warning("Stream-Generator::ThreadIsRunning: Stream data generator is already running. Not doing anything.")

    def stop(self) -> None:
        if self.running or (self.data_generator_thread is not None and self.data_generator_thread.is_alive()):
            logger.debug("Stream-Generator::StopGenThread")
            self.running = False
            if self.data_generator_thread is not None:
                self.data_generator_thread.join()

    def _data_generator_runner(self) -> None:
        while self.running:
            # Generate data for a single interval
            with codetiming.Timer(text="Stream-Generator::DataGenTime: {milliseconds:.4f} ms", logger=logger.debug):
                df: pd.DataFrame = self.get_data()
            # Push data to callback
            if self.data_generator_callback_function is not None:
                # Call the callback function with the generated data
                with codetiming.Timer(text="Stream-Generator::CallbackFunctionTime: {milliseconds:.4f} ms", logger=logger.debug):
                    self.data_generator_callback_function(df)
            # Sleep for interval
            time.sleep(self.interval)

    @abstractmethod
    def get_data(self) -> pd.DataFrame:
        # Placeholder for data generation logic
        pass

    @property
    @abstractmethod
    def schema(self) -> dict:
        pass
