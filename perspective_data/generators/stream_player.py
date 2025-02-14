import os
import pandas as pd
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timedelta

_SUPPORTED_FILE_TYPES = [".csv", ".parquet"]


class StreamDataSource(BaseModel):
    source: str | pd.DataFrame = Field(..., description="The source data to play.")
    description: str = Field(None, description="The description of the source data.")
    cols_description: dict[str, str] = Field(None, description="The description of the columns of the source data.")

    # dataframe options
    index_cols: str | list[str] = Field(None, description="The index column of the source data.")
    ts_col: str = Field(None, description="The timestamp column of the source data.")
    read_batch_size: int = Field(100_000, description="The batch size to use when playing the stream.")

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
        return v
    
    def _get_extension(self):
        if isinstance(self.source, str):
            _, ext = os.path.splitext(self.source)
            return ext
        else:
            return None

    def get_df(self):
        if isinstance(self.source, str):
            ext = self._get_extension()
            if ext == ".csv":
                return pd.read_csv(self.source, index_col=self.index_col)
            elif ext == ".parquet":
                return pd.read_parquet(self.source, engine="pyarrow", index_col=self.index_col)
            else:
                raise ValueError(f"Invalid file type: {ext}")
        elif isinstance(self.source, pd.DataFrame):
            return self.source
        else:
            raise ValueError("Source must be a pandas DataFrame or a path to a CSV file.")
        
    def get_schema(self):
        df = self.get_df()
        return {col: str(df[col].dtype) for col in df.columns}


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