import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timedelta
from enum import Enum


class SupportedFileTypes(Enum):
    CSV = ".csv"
    PARQUET = ".parquet"
    ARROW = ".arrow"
    DATAFRAME = "dataframe"

_SUPPORTED_FILE_TYPES = [file_type.value for file_type in SupportedFileTypes]


class StreamDataSource(BaseModel):
    source: str | pd.DataFrame = Field(..., description="The source data to play.")
    description: str = Field(None, description="The description of the source data.")
    cols_description: dict[str, str] = Field(None, description="The description of the columns of the source data.")

    # dataframe options
    # index_cols: str | list[str] = Field(None, description="The index column of the source data.")
    # ts_col: str = Field(None, description="The timestamp column of the source data.")
    chunksize: int = Field(100_000, description="The batch size to use when playing the stream.")
    loopback: bool = Field(True, description="Whether to loop back to the beginning of the stream when the end is reached.")
    df_read_options: dict = Field({}, description="Additional options to pass to the pandas read function.")

    # private fields
    _chunk_iterator: pd.io.parsers.readers.TextFileReader | pq.ParquetFile = None
    _cur_df: pd.DataFrame = None
    # _batched_read: bool = False

    @field_validator("source")
    @classmethod
    def validate_source(cls, v):
        if not isinstance(v, (str, pd.DataFrame)):
            raise ValueError("Source must be a pandas DataFrame or a path to a data file.")
        if isinstance(v, str):
            # check to see if the file exists
            if not os.path.exists(v):
                raise ValueError(f"File {v} does not exist.")
            else:
                # check for valid file extension
                _, ext = os.path.splitext(v)
                if ext not in _SUPPORTED_FILE_TYPES:
                    raise ValueError(f"Invalid file type: {ext}. At this point only following file formats are supported: {', '.join(_SUPPORTED_FILE_TYPES)}")
        elif isinstance(v, pd.DataFrame):
            raise NotImplementedError("Pandas DataFrame source is not yet supported.")
        return v
    
    @field_validator("chunksize")
    @classmethod
    def validate_chunksize(cls, v):
        if v is None:
            raise NotImplementedError("Chunk size must be greater than 0 and cannot be None.")
        elif v <= 0:
            raise ValueError("Chunk size must be greater than 0.")
        return v
    
    def _get_file_extension(self):
        if isinstance(self.source, str):
            _, ext = os.path.splitext(self.source)
            return ext
        else:
            return None
    
    def model_post_init(self, __context):
        self._chunk_iterator = self.open()
        return super().model_post_init(__context)
    
    @property
    def filetype(self) -> SupportedFileTypes:
        if self.source is None:
            raise ValueError("Source must be provided before accessing the file type.")
        if isinstance(self.source, pd.DataFrame):
            return SupportedFileTypes.DATAFRAME
        elif isinstance(self.source, str):
            ext = self._get_file_extension()
            if ext == ".csv":
                return SupportedFileTypes.CSV
            elif ext == ".parquet":
                return SupportedFileTypes.PARQUET
            elif ext == ".arrow":
                return SupportedFileTypes.ARROW
            else:
                raise ValueError(f"Invalid file type: {ext}")
        else:
            raise ValueError("Unsupported source type.")

    def open(self) -> None:
        if isinstance(self.source, str):
            filetype = self.filetype
            if filetype == SupportedFileTypes.CSV:
                self._chunk_iterator = pd.read_csv(self.source, chunksize=self.chunksize, **self.df_read_options)
            elif filetype == SupportedFileTypes.PARQUET or filetype == SupportedFileTypes.ARROW:
                _f = pq.ParquetFile(self.source, **self.df_read_options)
                self._chunk_iterator = _f.iter_batches(batch_size=self.chunksize)
            else:
                raise ValueError(f"Invalid file type: {filetype}")
        elif isinstance(self.source, pd.DataFrame):
            raise NotImplementedError("Pandas DataFrame source is not yet supported.")
        else:
            raise ValueError("Source must be a pandas DataFrame or a path to a data file.")
        
    def read(self) -> pd.DataFrame:
        if isinstance(self._chunk_iterator, (pd.io.parsers.readers.TextFileReader, pq.ParquetFile)):
            try:
                self._cur_df = next(self._chunk_iterator)
                return self._cur_df
            except StopIteration:
                if self.loopback:
                    self._chunk_iterator = self.open()
                    return self.read()
                else:
                    return None
        else:
            raise ValueError("Invalid chunk iterator.")
        
    def get_df(self):
        return self._chunk_iterator
    
    def set_df(self, df: pd.DataFrame):
        if isinstance(df, pd.DataFrame):
            self._chunk_iterator = df
        else:
            raise ValueError("Data must be a pandas DataFrame.")
        


class StreamPlayer(StreamDataSource):
    speed: float = Field(1.0, description="The speed at which to play the stream.")
    frame_ts_interval: str | float | timedelta | pd.timedelta = Field("1s", description="The time interval to advance the stream by in each frame. ts_column must be provided. Supports timedelta intervals.")
    frame_batch_size: int = Field(1, description="The number of rows to play in each frame.")
    loopback: bool = Field(True, description="Whether to loop back to the beginning of the stream when the end is reached.")

    # dataframe options
    index_cols: str | list[str] = Field(None, description="The index column of the source data.")
    ts_col: str = Field(None, description="The timestamp column of the source data.")
    indexing_method: str = Field("ts_index_cols", description="The indexing method to use. Options are 'ts_index_cols', 'index_cols', or 'ts'.")

    @field_validator("speed")
    @classmethod
    def validate_speed(cls, v):
        if v <= 0:
            raise ValueError("Speed must be greater than 0.")
        return v

    @field_validator("frame_batch_size")
    @classmethod
    def validate_frame_batch_size(cls, v):
        if v <= 0:
            raise ValueError("frame_batch_size must be greater than 0.")
        return v
    
    @field_validator("frame_ts_interval")
    @classmethod
    def validate_frame_ts_interval(cls, v):
        try:
            pd.Timedelta(v)
        except Exception as e:
            raise ValueError(f"Invalid time interval: {v}.") from e
        return v

    def model_post_init(self, __context):
        # make sure either frame_batch_size or frame_ts_interval is provided
        if not self.frame_batch_size and not self.frame_ts_interval:
            raise ValueError("Either frame_batch_size or frame_ts_interval must be provided.")
        return super().model_post_init(__context)
    



    @field_validator("stream")
    @classmethod
    def validate_stream(cls, v):
        from perspective_data import get_streams
        streams = get_streams()
        if v not in streams:
            raise ValueError(f"Stream {v} not found. Available streams: {list(streams.keys())}")
        return v