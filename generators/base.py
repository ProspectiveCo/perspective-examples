import time
from datetime import datetime, timedelta
import threading
import pandas as pd
from utils.logger import logger
from abc import ABC, abstractmethod
from typing import Union, List, Tuple, Callable
import codetiming
import atexit           # to ensure the generator thread exists with the main thread exists


__all__ = [
    "Generator",
    "StreamGenerator",
    "BatchGenerator",
    ]


class Generator(ABC):
    namespace: str = "default"

    def __init__(self, 
                 **kwargs
                 ) -> None:
        self.row_count: int = 0
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    def get_data(self) -> pd.DataFrame:
        pass

    @property
    @abstractmethod
    def schema(self) -> dict:
        pass

    @staticmethod
    @abstractmethod
    def required_parameters() -> dict[str, str]:
        pass

    @staticmethod
    @abstractmethod
    def from_config(config: dict) -> 'Generator':
        pass


class StreamGenerator(Generator):
    namespace: str = "stream"

    def __init__(self, 
                 interval: float = 1.0,                                     # interval: The number of seconds to wait between each batch of data generation.
                 nrows: int | tuple[int, int] = 100,                        # nrows: The number of rows to generate in each batch. Can be an int or a tuple of two ints.
                 start_time: str | datetime = datetime.now(),               # start_time: The start time for the data generator. Timestamp of the first row. Set to None to start from current time. Sample date format: "2021-01-01 00:00:00"
                 end_time: str | datetime = None,                           # end_time: The end time for the data generator. Timestamp of the last row. Set to None if generator should run indefinitely. Sample date format: "2021-01-01 00:00:00"
                 loopback: bool = False,                                    # loopback: If True, the generator will loop back to the start time after reaching the end time.
                 callback_subscribers: Union[Callable, List[Callable]] = None,    # callback_subscribers: A list of callback functions to call with the generated data batch.
                 **kwargs) -> None:
        # Initialize the stream data generator
        super().__init__(**kwargs)

        # setting data generator throughput parameters
        self.interval: float = interval
        self.nrows: int | tuple[int, int] = nrows
        if isinstance(nrows, tuple):
            self.min_rows: int = nrows[0]
            self.max_rows: int = nrows[1]
        else:
            self.min_rows: int = nrows
            self.max_rows: int = nrows
        # setting data generator time parameters
        # parsing start_time and end_time if they are strings
        try:
            if isinstance(start_time, str):
                self.start_time = pd.to_datetime(start_time).replace(microsecond=0)
            elif isinstance(start_time, datetime):
                self.start_time = start_time
            else:
                raise ValueError("Invalid type for start_time")
        except Exception as e:
            logger.warning(f"Stream-Generator::InvalidStartTime: {e}. Using current system timestamp as default.")
            self.start_time = datetime.now()
        try:
            if isinstance(end_time, str):
                self.end_time = pd.to_datetime(end_time).replace(microsecond=0)
            elif isinstance(end_time, datetime):
                self.end_time = end_time
            elif end_time is None:
                self.end_time = None
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
        if callback_subscribers is not None:
            if isinstance(callback_subscribers, list):
                self._callback_subscribers: list[callable] = callback_subscribers
            elif callable(callback_subscribers):
                self._callback_subscribers: list[callable] = [callback_subscribers]
            else:
                raise ValueError("Invalid type for callback_subscribers")
        else:
            self._callback_subscribers: list[callable] = []
        atexit.register(self.stop)  # Register the stop method to be called on exit

    def start(self) -> None:
        if not self.running:
            # If the thread is already running, stop it
            if self.data_generator_thread is not None and self.data_generator_thread.is_alive():
                logger.warning("Stream-Generator::ThreadIsRunning: Stream generator thread was already running. Stopping existing stream data generator")
                self.stop()  # Stop the existing thread
            logger.debug("Stream-Generator::StartGenThread")
            logger.info(f"Stream-Generator: start_time={self.start_time}, end_time={self.end_time}, interval={self.interval}, nrows={self.nrows}")
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
                try:
                    self.data_generator_thread.join()
                except Exception as e:
                    logger.info(f"Stream-Generator::ThreadJoinWarning: {e}")
        self.running = False

    def _data_generator_runner(self) -> None:
        while self.running:
            # take note of the current time before the call to get new data and other inherited methods
            tmp_time = self.current_time
            # Generate data for a single interval
            with codetiming.Timer(text="Stream-Generator::DataGenTime: {milliseconds:.3f} ms", logger=logger.debug):
                df: pd.DataFrame = self.get_data()
            # Push data to callback
            if self._callback_subscribers:
                # Call the callback function with the generated data
                with codetiming.Timer(text="Stream-Generator::CallbackFunctionTime: {milliseconds:.3f} ms", logger=logger.debug):
                    for callback in self._callback_subscribers:
                        callback(df)
            # adnvance the current time by the interval if implemented methods haven't yet
            if tmp_time == self.current_time:
                self.current_time += timedelta(seconds=self.interval)
            # check the current time and end time to decide whether to stop the generator
            logger.debug(f"Stream-Generator: current_time={self.current_time}, end_time={self.end_time}")
            if self.end_time is not None and self.current_time >= self.end_time:
                logger.info("Stream-Generator::EndTimeReached: Stopping stream data generator.")
                self.stop()
            # Sleep for interval
            time.sleep(self.interval)

    def add_subscriber(self, callback_function: callable) -> None:
        self._callback_subscribers.append(callback_function)

    def is_running(self) -> bool:
        return self.running



class BatchGenerator(Generator):
    namespace: str = "batch"

    def __init__(self, 
                    nrows: int = 100,                                     # nrows: The number of rows to generate in each batch.
                    **kwargs) -> None:
        super().__init__(**kwargs)
        self.nrows: int = nrows

    @abstractmethod
    def get_data(self) -> pd.DataFrame:
        pass